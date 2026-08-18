//! # The High-Performance Asynchronous MQTT Client
//!
//! This module contains `MqttClient`, `MqttSender`, `MqttReceiver`, and multi-packet batching APIs.

use crate::error::{MqttError, ProtocolError};
use crate::packet::{
    self, Connect, Disconnect, EncodePacket, MqttPacket, PingReq, PubAck, Publish, QoS, Subscribe,
    Unsubscribe, Will,
};
use crate::transport::{self, MqttQuicTransport, MqttTransport};
use crate::util::RawPacketFrameIter;
use embassy_time::{Duration, Instant};
use heapless::Vec;

/// Represents the MQTT protocol version used by the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MqttVersion {
    V3,
    V5,
}

/// Streaming and latency mode for MQTT client operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum StreamMode {
    /// Standard request-response / transactional MQTT mode.
    #[default]
    Standard,
    /// Real-time streaming mode: tuned for continuous high-frequency telemetry,
    /// fast-path zero-copy frame dispatch, and minimal latency.
    RealTimeStreaming,
}

/// A zero-allocation streaming writer for transmitting large or continuous payloads
/// chunk-by-chunk directly across the transport without requiring a large RAM buffer.
pub struct MqttStreamWriter<'c, T: MqttTransport> {
    transport: &'c mut T,
    remaining_bytes: usize,
    total_bytes: usize,
}

impl<'c, T: MqttTransport> MqttStreamWriter<'c, T> {
    /// Writes a slice of chunk data directly to the transport.
    pub async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        if chunk.len() > self.remaining_bytes {
            return Err(MqttError::BufferTooSmall);
        }
        self.transport.send(chunk).await?;
        self.remaining_bytes -= chunk.len();
        Ok(())
    }

    /// Returns the number of remaining bytes expected in the stream.
    pub fn remaining_bytes(&self) -> usize {
        self.remaining_bytes
    }

    /// Returns the total payload size of the stream.
    pub fn total_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Finalizes the stream and verifies that all declared bytes were sent.
    pub fn finish(self) -> Result<(), MqttError<T::Error>> {
        if self.remaining_bytes != 0 {
            return Err(MqttError::Protocol(ProtocolError::IncompletePacket));
        }
        Ok(())
    }
}

/// Configuration options for the `MqttClient`.
#[derive(Debug, Clone)]
pub struct MqttOptions<'a> {
    pub client_id: &'a str,
    pub broker_addr: &'a str,
    pub broker_port: u16,
    pub version: MqttVersion,
    pub keep_alive: Duration,
    pub clean_session: bool,
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
    pub will: Option<Will<'a>>,
    pub stream_mode: StreamMode,
}

#[cfg(feature = "defmt")]
impl<'a> defmt::Format for MqttOptions<'a> {
    fn format(&self, fmt: defmt::Formatter) {
        defmt::write!(
            fmt,
            "MqttOptions {{ client_id: {}, broker_addr: {}, broker_port: {}, version: {}, keep_alive_secs: {}, clean_session: {}, stream_mode: {} }}",
            self.client_id,
            self.broker_addr,
            self.broker_port,
            self.version,
            self.keep_alive.as_secs(),
            self.clean_session,
            self.stream_mode,
        );
    }
}

impl<'a> MqttOptions<'a> {
    pub fn new(client_id: &'a str, broker_addr: &'a str, broker_port: u16) -> Self {
        Self {
            client_id,
            broker_addr,
            broker_port,
            version: MqttVersion::V3,
            keep_alive: Duration::from_secs(60),
            clean_session: true,
            username: None,
            password: None,
            will: None,
            stream_mode: StreamMode::Standard,
        }
    }

    pub fn with_version(mut self, version: MqttVersion) -> Self {
        self.version = version;
        self
    }

    pub fn with_keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    pub fn with_credentials(mut self, username: &'a str, password: &'a str) -> Self {
        self.username = Some(username);
        self.password = Some(password);
        self
    }

    pub fn with_clean_session(mut self, clean: bool) -> Self {
        self.clean_session = clean;
        self
    }

    pub fn with_will(mut self, topic: &'a str, payload: &'a [u8], qos: QoS, retain: bool) -> Self {
        self.will = Some(Will::new(topic, payload, qos, retain));
        self
    }

    pub fn with_stream_mode(mut self, mode: StreamMode) -> Self {
        self.stream_mode = mode;
        self
    }
}

/// Represents the current connection state of the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
}

/// A publish request descriptor used for high-performance multi-packet burst sending.
#[derive(Debug, Clone, Copy)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct PublishMessage<'a> {
    pub topic: &'a str,
    pub payload: &'a [u8],
    pub qos: QoS,
    pub retain: bool,
}

impl<'a> PublishMessage<'a> {
    pub fn new(topic: &'a str, payload: &'a [u8], qos: QoS) -> Self {
        Self {
            topic,
            payload,
            qos,
            retain: false,
        }
    }
}

/// The asynchronous, high-throughput MQTT client with multi-packet and zero-copy support.
pub struct MqttClient<'a, T, const MAX_TOPICS: usize, const BUF_SIZE: usize>
where
    T: MqttTransport,
{
    transport: T,
    options: MqttOptions<'a>,
    tx_buffer: [u8; BUF_SIZE],
    rx_buffer: [u8; BUF_SIZE],
    rx_len: usize,
    state: ConnectionState,
    last_tx_time: Instant,
    next_packet_id: u16,
}

impl<'a, T, const MAX_TOPICS: usize, const BUF_SIZE: usize> MqttClient<'a, T, MAX_TOPICS, BUF_SIZE>
where
    T: MqttTransport,
{
    pub fn new(transport: T, options: MqttOptions<'a>) -> Self {
        Self {
            transport,
            options,
            tx_buffer: [0; BUF_SIZE],
            rx_buffer: [0; BUF_SIZE],
            rx_len: 0,
            state: ConnectionState::Disconnected,
            last_tx_time: Instant::now(),
            next_packet_id: 1,
        }
    }

    pub fn state(&self) -> ConnectionState {
        self.state
    }

    pub fn options(&self) -> &MqttOptions<'a> {
        &self.options
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    pub fn into_transport(self) -> T {
        self.transport
    }

    /// Attempts to connect to the MQTT broker.
    pub async fn connect(&mut self) -> Result<(), MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        self.state = ConnectionState::Connecting;
        let mut connect_packet = Connect::new(
            self.options.client_id,
            self.options.keep_alive.as_secs() as u16,
            self.options.clean_session,
        );
        connect_packet.username = self.options.username;
        connect_packet.password = self.options.password;
        connect_packet.will = self.options.will.clone();

        let len = connect_packet
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::cast_transport_error)?;

        self.transport.send(&self.tx_buffer[..len]).await?;

        let n = self.transport.recv(&mut self.rx_buffer).await?;
        if n == 0 {
            self.state = ConnectionState::Disconnected;
            return Err(MqttError::Protocol(ProtocolError::InvalidResponse));
        }

        let packet = packet::decode::<T::Error>(&self.rx_buffer[..n], self.options.version)?
            .ok_or(MqttError::Protocol(ProtocolError::InvalidResponse))?;

        if let MqttPacket::ConnAck(connack) = packet {
            if connack.reason_code == 0 {
                self.state = ConnectionState::Connected;
                self.last_tx_time = Instant::now();
                self.rx_len = 0;
                Ok(())
            } else {
                self.state = ConnectionState::Disconnected;
                Err(MqttError::ConnectionRefused(connack.reason_code.into()))
            }
        } else {
            self.state = ConnectionState::Disconnected;
            Err(MqttError::Protocol(ProtocolError::InvalidResponse))
        }
    }

    /// Publishes a single message to a topic.
    pub async fn publish<'p>(
        &mut self,
        topic: &'p str,
        payload: &'p [u8],
        qos: QoS,
    ) -> Result<(), MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        if qos == QoS::ExactlyOnce {
            return Err(MqttError::Protocol(ProtocolError::UnsupportedQoS));
        }

        let packet_id = if qos != QoS::AtMostOnce {
            Some(self.get_next_packet_id())
        } else {
            None
        };

        let mut publish_packet = Publish::new(topic, payload, qos);
        publish_packet.packet_id = packet_id;

        let len = publish_packet
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::cast_transport_error)?;

        self.transport.send(&self.tx_buffer[..len]).await?;
        self.last_tx_time = Instant::now();
        Ok(())
    }

    /// Fast-path multi-packet burst publish: encodes multiple messages into the TX buffer and transmits in a single batch.
    pub async fn publish_batch(
        &mut self,
        messages: &[PublishMessage<'_>],
    ) -> Result<usize, MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        let mut cursor = 0;
        let mut sent_count = 0;

        for msg in messages {
            if msg.qos == QoS::ExactlyOnce {
                return Err(MqttError::Protocol(ProtocolError::UnsupportedQoS));
            }

            let packet_id = if msg.qos != QoS::AtMostOnce {
                Some(self.get_next_packet_id())
            } else {
                None
            };

            let mut publish_packet = Publish::new(msg.topic, msg.payload, msg.qos);
            publish_packet.packet_id = packet_id;
            publish_packet.retain = msg.retain;

            match publish_packet.encode(&mut self.tx_buffer[cursor..], self.options.version) {
                Ok(len) => {
                    cursor += len;
                    sent_count += 1;
                }
                Err(MqttError::BufferTooSmall) => {
                    if cursor > 0 {
                        self.transport.send(&self.tx_buffer[..cursor]).await?;
                        cursor = 0;
                        let len = publish_packet
                            .encode(&mut self.tx_buffer, self.options.version)
                            .map_err(MqttError::cast_transport_error)?;
                        cursor += len;
                        sent_count += 1;
                    } else {
                        return Err(MqttError::BufferTooSmall);
                    }
                }
                Err(e) => return Err(MqttError::cast_transport_error(e)),
            }
        }

        if cursor > 0 {
            self.transport.send(&self.tx_buffer[..cursor]).await?;
            self.last_tx_time = Instant::now();
        }

        Ok(sent_count)
    }

    /// Begins streaming a payload of `total_payload_len` bytes on `topic`.
    ///
    /// Sends the MQTT `PUBLISH` frame header immediately and returns an `MqttStreamWriter`
    /// that allows writing payload chunks directly to the transport without buffering the entire payload in RAM.
    pub async fn begin_stream_publish<'c>(
        &'c mut self,
        topic: &str,
        total_payload_len: usize,
        qos: QoS,
    ) -> Result<MqttStreamWriter<'c, T>, MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        if qos == QoS::ExactlyOnce {
            return Err(MqttError::Protocol(ProtocolError::UnsupportedQoS));
        }

        let packet_id = if qos != QoS::AtMostOnce {
            Some(self.get_next_packet_id())
        } else {
            None
        };

        let mut variable_header_len = 2 + topic.len();
        if packet_id.is_some() {
            variable_header_len += 2;
        }
        if self.options.version == MqttVersion::V5 {
            variable_header_len += 1;
        }

        let remaining_len = variable_header_len + total_payload_len;

        let mut header_byte_0 = 0x30;
        if qos == QoS::AtLeastOnce {
            header_byte_0 |= 0x02;
        }
        *self.tx_buffer.get_mut(0).ok_or(MqttError::BufferTooSmall)? = header_byte_0;

        let len_bytes = crate::util::write_variable_byte_integer_len(
            self.tx_buffer
                .get_mut(1..)
                .ok_or(MqttError::BufferTooSmall)?,
            remaining_len,
        )
        .map_err(MqttError::cast_transport_error)?;

        let mut cursor = 1 + len_bytes;

        cursor += crate::util::write_utf8_string(
            self.tx_buffer
                .get_mut(cursor..)
                .ok_or(MqttError::BufferTooSmall)?,
            topic,
        )
        .map_err(MqttError::cast_transport_error)?;

        if let Some(pid) = packet_id {
            self.tx_buffer
                .get_mut(cursor..cursor + 2)
                .ok_or(MqttError::BufferTooSmall)?
                .copy_from_slice(&pid.to_be_bytes());
            cursor += 2;
        }

        if self.options.version == MqttVersion::V5 {
            *self
                .tx_buffer
                .get_mut(cursor)
                .ok_or(MqttError::BufferTooSmall)? = 0x00;
            cursor += 1;
        }

        self.transport.send(&self.tx_buffer[..cursor]).await?;
        self.last_tx_time = Instant::now();

        Ok(MqttStreamWriter {
            transport: &mut self.transport,
            remaining_bytes: total_payload_len,
            total_bytes: total_payload_len,
        })
    }

    /// Subscribes to one or more topic filters.
    pub async fn subscribe(&mut self, topics: &[(&str, QoS)]) -> Result<u16, MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        let pid = self.get_next_packet_id();
        let mut sub_packet = Subscribe::new(pid);
        for (topic, qos) in topics {
            sub_packet
                .add_topic(topic, *qos)
                .map_err(MqttError::cast_transport_error)?;
        }

        let len = sub_packet
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::cast_transport_error)?;

        self.transport.send(&self.tx_buffer[..len]).await?;
        self.last_tx_time = Instant::now();
        Ok(pid)
    }

    /// Unsubscribes from one or more topic filters.
    pub async fn unsubscribe(&mut self, topics: &[&str]) -> Result<u16, MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        let pid = self.get_next_packet_id();
        let mut unsub_packet = Unsubscribe::new(pid);
        for topic in topics {
            unsub_packet
                .add_topic(topic)
                .map_err(MqttError::cast_transport_error)?;
        }

        let len = unsub_packet
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::cast_transport_error)?;

        self.transport.send(&self.tx_buffer[..len]).await?;
        self.last_tx_time = Instant::now();
        Ok(pid)
    }

    /// Sends a graceful DISCONNECT packet and marks client as disconnected.
    pub async fn disconnect(&mut self) -> Result<(), MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        if self.state == ConnectionState::Connected {
            let disc = Disconnect::new();
            if let Ok(len) = disc.encode(&mut self.tx_buffer, self.options.version) {
                let _ = self.transport.send(&self.tx_buffer[..len]).await;
            }
        }
        self.state = ConnectionState::Disconnected;
        Ok(())
    }

    /// Polls the connection for incoming packets and handles keep-alives.
    pub async fn poll<'p>(&'p mut self) -> Result<Option<MqttEvent<'p>>, MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        if self.last_tx_time.elapsed() >= self.options.keep_alive {
            let len = PingReq
                .encode(&mut self.tx_buffer, self.options.version)
                .map_err(MqttError::cast_transport_error)?;
            self.transport.send(&self.tx_buffer[..len]).await?;
            self.last_tx_time = Instant::now();
        }

        let n = self.transport.recv(&mut self.rx_buffer).await?;
        if n > 0
            && let Some(packet) =
                packet::decode::<T::Error>(&self.rx_buffer[..n], self.options.version)?
        {
            match packet {
                MqttPacket::Publish(p) => {
                    if p.qos == QoS::AtLeastOnce
                        && let Some(pid) = p.packet_id
                    {
                        let ack = PubAck::new(pid);
                        if let Ok(len) = ack.encode(&mut self.tx_buffer, self.options.version) {
                            let _ = self.transport.send(&self.tx_buffer[..len]).await;
                        }
                    }
                    return Ok(Some(MqttEvent::Publish(p)));
                }
                MqttPacket::PubAck(ack) => return Ok(Some(MqttEvent::PubAck(ack))),
                MqttPacket::SubAck(suback) => return Ok(Some(MqttEvent::SubAck(suback))),
                MqttPacket::UnsubAck(unsuback) => return Ok(Some(MqttEvent::UnsubAck(unsuback))),
                MqttPacket::PingResp => return Ok(Some(MqttEvent::PingResp)),
                MqttPacket::Disconnect(disc) => {
                    self.state = ConnectionState::Disconnected;
                    return Ok(Some(MqttEvent::Disconnect(disc)));
                }
                _ => {}
            }
        }
        Ok(None)
    }

    /// Multi-packet burst polling: parses all available packets in the receive buffer.
    pub async fn poll_batch<'p, const MAX_EVENTS: usize>(
        &'p mut self,
    ) -> Result<Vec<MqttEvent<'p>, MAX_EVENTS>, MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        let mut events = Vec::new();
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        if self.last_tx_time.elapsed() >= self.options.keep_alive {
            let len = PingReq
                .encode(&mut self.tx_buffer, self.options.version)
                .map_err(MqttError::cast_transport_error)?;
            self.transport.send(&self.tx_buffer[..len]).await?;
            self.last_tx_time = Instant::now();
        }

        let n = self.transport.recv(&mut self.rx_buffer).await?;
        if n > 0 {
            let iter = RawPacketFrameIter::new(&self.rx_buffer[..n]);
            for frame in iter.flatten() {
                if let Some(packet) = packet::decode::<T::Error>(frame, self.options.version)? {
                    match packet {
                        MqttPacket::Publish(p) => {
                            if p.qos == QoS::AtLeastOnce
                                && let Some(pid) = p.packet_id
                            {
                                let ack = PubAck::new(pid);
                                if let Ok(ack_len) =
                                    ack.encode(&mut self.tx_buffer, self.options.version)
                                {
                                    let _ = self.transport.send(&self.tx_buffer[..ack_len]).await;
                                }
                            }
                            let _ = events.push(MqttEvent::Publish(p));
                        }
                        MqttPacket::PubAck(ack) => {
                            let _ = events.push(MqttEvent::PubAck(ack));
                        }
                        MqttPacket::SubAck(suback) => {
                            let _ = events.push(MqttEvent::SubAck(suback));
                        }
                        MqttPacket::UnsubAck(unsuback) => {
                            let _ = events.push(MqttEvent::UnsubAck(unsuback));
                        }
                        MqttPacket::PingResp => {
                            let _ = events.push(MqttEvent::PingResp);
                        }
                        MqttPacket::Disconnect(disc) => {
                            self.state = ConnectionState::Disconnected;
                            let _ = events.push(MqttEvent::Disconnect(disc));
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(events)
    }

    pub fn get_next_packet_id(&mut self) -> u16 {
        let id = self.next_packet_id;
        self.next_packet_id = self.next_packet_id.wrapping_add(1);
        if self.next_packet_id == 0 {
            self.next_packet_id = 1;
        }
        id
    }
}

/// Represents an event received from the MQTT broker.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MqttEvent<'p> {
    Publish(Publish<'p>),
    PubAck(PubAck<'p>),
    SubAck(packet::SubAck<'p>),
    UnsubAck(packet::UnsubAck<'p>),
    PingResp,
    Disconnect(Disconnect<'p>),
}

/// A dedicated real-time MQTT over QUIC / H3 client.
pub struct QuicMqttClient<'a, Q, const BUF_SIZE: usize>
where
    Q: MqttQuicTransport,
{
    transport: Q,
    options: MqttOptions<'a>,
    tx_buffer: [u8; BUF_SIZE],
    rx_buffer: [u8; BUF_SIZE],
}

impl<'a, Q, const BUF_SIZE: usize> QuicMqttClient<'a, Q, BUF_SIZE>
where
    Q: MqttQuicTransport,
{
    pub fn new(transport: Q, options: MqttOptions<'a>) -> Self {
        Self {
            transport,
            options,
            tx_buffer: [0; BUF_SIZE],
            rx_buffer: [0; BUF_SIZE],
        }
    }

    pub fn transport(&self) -> &Q {
        &self.transport
    }

    pub fn transport_mut(&mut self) -> &mut Q {
        &mut self.transport
    }

    pub fn into_transport(self) -> Q {
        self.transport
    }

    /// Sends ultra-fast real-time telemetry via unreliable QUIC datagrams (zero handshake / zero HoL blocking).
    pub async fn publish_datagram(
        &mut self,
        topic: &str,
        payload: &[u8],
    ) -> Result<(), MqttError<Q::Error>>
    where
        Q::Error: transport::TransportError,
    {
        let publish_packet = Publish::new(topic, payload, QoS::AtMostOnce);
        let len = publish_packet
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::cast_transport_error)?;

        self.transport
            .send_datagram(&self.tx_buffer[..len])
            .await
            .map_err(MqttError::Transport)
    }

    /// Receives a telemetry datagram.
    pub async fn recv_datagram<'p>(
        &'p mut self,
    ) -> Result<Option<MqttEvent<'p>>, MqttError<Q::Error>>
    where
        Q::Error: transport::TransportError,
    {
        let n = self
            .transport
            .recv_datagram(&mut self.rx_buffer)
            .await
            .map_err(MqttError::Transport)?;

        if n > 0
            && let Some(MqttPacket::Publish(p)) =
                packet::decode::<Q::Error>(&self.rx_buffer[..n], self.options.version)?
        {
            return Ok(Some(MqttEvent::Publish(p)));
        }
        Ok(None)
    }

    /// Opens a dedicated unidirectional QUIC stream for continuous real-time telemetry streaming
    /// with zero Head-of-Line blocking.
    pub async fn open_telemetry_stream(
        &mut self,
        topic: &str,
    ) -> Result<Q::SendStream, MqttError<Q::Error>>
    where
        Q::Error: transport::TransportError,
    {
        use crate::transport::MqttQuicSendStream;
        let mut send_stream = self
            .transport
            .open_uni_stream()
            .await
            .map_err(MqttError::Transport)?;

        let topic_len = topic.len();
        if topic_len + 2 > self.tx_buffer.len() {
            return Err(MqttError::BufferTooSmall);
        }

        self.tx_buffer[0..2].copy_from_slice(&(topic_len as u16).to_be_bytes());
        self.tx_buffer[2..2 + topic_len].copy_from_slice(topic.as_bytes());

        send_stream
            .write(&self.tx_buffer[..2 + topic_len])
            .await
            .map_err(MqttError::Transport)?;

        Ok(send_stream)
    }
}
