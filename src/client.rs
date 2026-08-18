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
}

#[cfg(feature = "defmt")]
impl<'a> defmt::Format for MqttOptions<'a> {
    fn format(&self, fmt: defmt::Formatter) {
        defmt::write!(
            fmt,
            "MqttOptions {{ client_id: {}, broker_addr: {}, broker_port: {}, version: {}, keep_alive_secs: {}, clean_session: {} }}",
            self.client_id,
            self.broker_addr,
            self.broker_port,
            self.version,
            self.keep_alive.as_secs(),
            self.clean_session,
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
}
