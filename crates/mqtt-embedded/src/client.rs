//! # Embedded Asynchronous MQTT Client State Machine
//!
//! Provides zero-allocation publishing, subscribing, multi-packet batching, and keep-alive heartbeats.

use embassy_time::{Duration, Instant};
use heapless::Vec;
use mqtt_packet::{
    Connect, Disconnect, EncodePacket, MqttPacket, MqttVersion, PingReq,
    Property, PubAck, PubComp, PubRec, PubRel, Publish, QoS, RawPacketFrameIter,
    SubAck, Subscribe, UnsubAck, Unsubscribe, Will, decode,
};

use crate::error::{ConnectReasonCode, MqttError, ProtocolError};
use crate::inflight::InflightQueue;
use crate::stream_writer::MqttStreamWriter;
use crate::transport::{MqttQuicTransport, MqttTransport, TransportError};

/// Streaming mode for real-time sensor loops.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum StreamMode {
    #[default]
    Standard,
    RealTimeStreaming,
}

/// Incoming MQTT event yielded by `poll()` or `poll_batch()`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MqttEvent<'a> {
    Publish(Publish<'a>),
    PubAck(PubAck<'a>),
    PubRec(PubRec<'a>),
    PubRel(PubRel<'a>),
    PubComp(PubComp<'a>),
    SubAck(SubAck<'a>),
    UnsubAck(UnsubAck<'a>),
    PingResp,
    Disconnect,
}

/// Outgoing publish message for batch publishing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// Connection options and credentials for the embedded client.
#[derive(Debug, Clone)]
pub struct MqttOptions<'a> {
    pub client_id: &'a str,
    pub host: &'a str,
    pub port: u16,
    pub keep_alive: Duration,
    pub clean_session: bool,
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
    pub will: Option<Will<'a>>,
    pub properties: Vec<Property<'a>, 8>,
    pub version: MqttVersion,
    pub stream_mode: StreamMode,
}

impl<'a> MqttOptions<'a> {
    pub fn new(client_id: &'a str, host: &'a str, port: u16) -> Self {
        Self {
            client_id,
            host,
            port,
            keep_alive: Duration::from_secs(30),
            clean_session: true,
            username: None,
            password: None,
            will: None,
            properties: Vec::new(),
            version: MqttVersion::V3_1_1,
            stream_mode: StreamMode::Standard,
        }
    }

    pub fn with_keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    pub fn with_clean_session(mut self, clean_session: bool) -> Self {
        self.clean_session = clean_session;
        self
    }

    pub fn with_credentials(mut self, username: &'a str, password: &'a str) -> Self {
        self.username = Some(username);
        self.password = Some(password);
        self
    }

    pub fn with_will(mut self, will: Will<'a>) -> Self {
        self.will = Some(will);
        self
    }

    pub fn with_version(mut self, version: MqttVersion) -> Self {
        self.version = version;
        self
    }

    pub fn with_stream_mode(mut self, stream_mode: StreamMode) -> Self {
        self.stream_mode = stream_mode;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
}

/// Asynchronous zero-allocation embedded MQTT client.
pub struct MqttClient<'a, T, const MAX_TOPICS: usize = 8, const BUF_SIZE: usize = 1024, const MAX_INFLIGHT: usize = 8>
where
    T: MqttTransport,
{
    transport: T,
    options: MqttOptions<'a>,
    state: ConnectionState,
    next_packet_id: u16,
    last_tx_time: Instant,
    last_rx_time: Instant,
    tx_buffer: [u8; BUF_SIZE],
    rx_buffer: [u8; BUF_SIZE],
    rx_len: usize,
    inflight: InflightQueue<MAX_INFLIGHT>,
}

impl<'a, T, const MAX_TOPICS: usize, const BUF_SIZE: usize, const MAX_INFLIGHT: usize>
    MqttClient<'a, T, MAX_TOPICS, BUF_SIZE, MAX_INFLIGHT>
where
    T: MqttTransport,
{
    pub fn new(transport: T, options: MqttOptions<'a>) -> Self {
        Self {
            transport,
            options,
            state: ConnectionState::Disconnected,
            next_packet_id: 1,
            last_tx_time: Instant::now(),
            last_rx_time: Instant::now(),
            tx_buffer: [0u8; BUF_SIZE],
            rx_buffer: [0u8; BUF_SIZE],
            rx_len: 0,
            inflight: InflightQueue::new(),
        }
    }

    pub fn state(&self) -> ConnectionState {
        self.state
    }

    pub fn is_connected(&self) -> bool {
        self.state == ConnectionState::Connected
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }

    fn get_next_packet_id(&mut self) -> u16 {
        let id = self.next_packet_id;
        self.next_packet_id = self.next_packet_id.wrapping_add(1);
        if self.next_packet_id == 0 {
            self.next_packet_id = 1;
        }
        id
    }

    /// Establishes the MQTT session handshake with the broker.
    pub async fn connect(&mut self) -> Result<(), MqttError<T::Error>>
    where
        T::Error: TransportError,
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
        connect_packet.properties = self.options.properties.clone();

        let len = connect_packet
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::from)?;

        self.transport.send(&self.tx_buffer[..len]).await?;
        self.last_tx_time = Instant::now();

        let read_len = self.transport.recv(&mut self.rx_buffer).await?;
        if read_len == 0 {
            self.state = ConnectionState::Disconnected;
            return Err(MqttError::NotConnected);
        }

        self.last_rx_time = Instant::now();

        if let Some(MqttPacket::ConnAck(connack)) =
            decode(&self.rx_buffer[..read_len], self.options.version)?
        {
            if connack.reason_code == 0 {
                self.state = ConnectionState::Connected;
                self.inflight.clear();
                Ok(())
            } else {
                self.state = ConnectionState::Disconnected;
                Err(MqttError::ConnectionRefused(ConnectReasonCode::from(
                    connack.reason_code,
                )))
            }
        } else {
            self.state = ConnectionState::Disconnected;
            Err(MqttError::Protocol(ProtocolError::InvalidResponse))
        }
    }

    /// Publishes a message over the connected transport.
    pub async fn publish(
        &mut self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
    ) -> Result<(), MqttError<T::Error>>
    where
        T::Error: TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        let packet_id = if qos != QoS::AtMostOnce {
            let pid = self.get_next_packet_id();
            self.inflight.track_outbound(pid, qos)?;
            Some(pid)
        } else {
            None
        };

        let mut publish_packet = Publish::new(topic, payload, qos);
        publish_packet.packet_id = packet_id;

        let len = publish_packet
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::from)?;

        self.transport.send(&self.tx_buffer[..len]).await?;
        self.last_tx_time = Instant::now();
        Ok(())
    }

    /// Multi-packet burst publish in a single network pass.
    pub async fn publish_batch(
        &mut self,
        messages: &[PublishMessage<'_>],
    ) -> Result<usize, MqttError<T::Error>>
    where
        T::Error: TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        let mut cursor = 0;
        let mut sent_count = 0;

        for msg in messages {
            let packet_id = if msg.qos != QoS::AtMostOnce {
                let pid = self.get_next_packet_id();
                self.inflight.track_outbound(pid, msg.qos)?;
                Some(pid)
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
                Err(mqtt_packet::PacketError::BufferTooSmall) => {
                    if cursor > 0 {
                        self.transport.send(&self.tx_buffer[..cursor]).await?;
                        cursor = 0;
                        let len = publish_packet
                            .encode(&mut self.tx_buffer, self.options.version)
                            .map_err(MqttError::from)?;
                        cursor += len;
                        sent_count += 1;
                    } else {
                        return Err(MqttError::BufferTooSmall);
                    }
                }
                Err(e) => return Err(MqttError::from(e)),
            }
        }

        if cursor > 0 {
            self.transport.send(&self.tx_buffer[..cursor]).await?;
            self.last_tx_time = Instant::now();
        }

        Ok(sent_count)
    }

    /// Begins streaming an arbitrary-length payload directly over the transport.
    pub async fn begin_stream_publish<'c>(
        &'c mut self,
        topic: &str,
        total_payload_len: usize,
        qos: QoS,
    ) -> Result<MqttStreamWriter<'c, T>, MqttError<T::Error>>
    where
        T::Error: TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        let packet_id = if qos != QoS::AtMostOnce {
            let pid = self.get_next_packet_id();
            self.inflight.track_outbound(pid, qos)?;
            Some(pid)
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

        let mut header_buf = [0u8; 16];
        let mut flags = 0x30;
        flags |= (qos as u8) << 1;
        header_buf[0] = flags;

        let len_bytes = mqtt_packet::write_variable_byte_integer_len(
            &mut header_buf[1..],
            remaining_len,
        )
        .map_err(MqttError::from)?;

        let total_fixed_header_len = 1 + len_bytes;
        self.transport.send(&header_buf[..total_fixed_header_len]).await?;

        let mut var_buf = [0u8; 256];
        let mut var_cursor = 0;
        var_cursor += mqtt_packet::write_utf8_string(&mut var_buf[var_cursor..], topic)
            .map_err(MqttError::from)?;

        if let Some(pid) = packet_id {
            var_buf[var_cursor..var_cursor + 2].copy_from_slice(&pid.to_be_bytes());
            var_cursor += 2;
        }

        if self.options.version == MqttVersion::V5 {
            var_buf[var_cursor] = 0;
            var_cursor += 1;
        }

        self.transport.send(&var_buf[..var_cursor]).await?;
        self.last_tx_time = Instant::now();

        Ok(MqttStreamWriter::new(&mut self.transport, total_payload_len))
    }

    /// Subscribes to one or more topic filters.
    pub async fn subscribe(
        &mut self,
        topics: &[(&str, QoS)],
    ) -> Result<u16, MqttError<T::Error>>
    where
        T::Error: TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        let packet_id = self.get_next_packet_id();
        let mut sub = Subscribe::new(packet_id);
        for &(t, q) in topics {
            sub.add_topic(t, q).map_err(MqttError::from)?;
        }

        let len = sub
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::from)?;

        self.transport.send(&self.tx_buffer[..len]).await?;
        self.last_tx_time = Instant::now();
        Ok(packet_id)
    }

    /// Unsubscribes from one or more topic filters.
    pub async fn unsubscribe(&mut self, topics: &[&str]) -> Result<u16, MqttError<T::Error>>
    where
        T::Error: TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        let packet_id = self.get_next_packet_id();
        let mut unsub = Unsubscribe::new(packet_id);
        for &t in topics {
            unsub.add_topic(t).map_err(MqttError::from)?;
        }

        let len = unsub
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::from)?;

        self.transport.send(&self.tx_buffer[..len]).await?;
        self.last_tx_time = Instant::now();
        Ok(packet_id)
    }

    /// Performs keep-alive ping and parses next incoming event from the socket.
    pub async fn poll<'p>(&'p mut self) -> Result<Option<MqttEvent<'p>>, MqttError<T::Error>>
    where
        T::Error: TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        // Send PINGREQ if keepalive interval elapsed
        let now = Instant::now();
        if now.duration_since(self.last_tx_time) >= self.options.keep_alive / 2 {
            let ping = PingReq;
            let len = ping
                .encode(&mut self.tx_buffer, self.options.version)
                .map_err(MqttError::from)?;
            self.transport.send(&self.tx_buffer[..len]).await?;
            self.last_tx_time = now;
        }

        // Receive incoming bytes
        let n = self.transport.recv(&mut self.rx_buffer).await?;
        if n == 0 {
            return Ok(None);
        }

        self.rx_len = n;
        self.last_rx_time = Instant::now();

        let parsed = decode(&self.rx_buffer[..self.rx_len], self.options.version)?;
        match parsed {
            Some(MqttPacket::Publish(p)) => {
                // Auto acknowledge QoS 1 and QoS 2
                if p.qos == QoS::AtLeastOnce {
                    if let Some(pid) = p.packet_id {
                        let puback = PubAck::new(pid);
                        let len = puback
                            .encode(&mut self.tx_buffer, self.options.version)
                            .map_err(MqttError::from)?;
                        self.transport.send(&self.tx_buffer[..len]).await?;
                    }
                } else if p.qos == QoS::ExactlyOnce {
                    if let Some(pid) = p.packet_id {
                        let pubrec = PubRec::new(pid);
                        let len = pubrec
                            .encode(&mut self.tx_buffer, self.options.version)
                            .map_err(MqttError::from)?;
                        self.transport.send(&self.tx_buffer[..len]).await?;
                        let _ = self.inflight.track_inbound_qos2::<T::Error>(pid);
                    }
                }

                Ok(Some(MqttEvent::Publish(p)))
            }
            Some(MqttPacket::PubAck(ack)) => {
                self.inflight.handle_puback(ack.packet_id);
                Ok(Some(MqttEvent::PubAck(ack)))
            }
            Some(MqttPacket::PubRec(rec)) => {
                self.inflight.handle_pubrec(rec.packet_id);
                let pubrel = PubRel::new(rec.packet_id);
                let len = pubrel
                    .encode(&mut self.tx_buffer, self.options.version)
                    .map_err(MqttError::from)?;
                self.transport.send(&self.tx_buffer[..len]).await?;
                Ok(Some(MqttEvent::PubRec(rec)))
            }
            Some(MqttPacket::PubRel(rel)) => {
                self.inflight.handle_pubrel(rel.packet_id);
                let pubcomp = PubComp::new(rel.packet_id);
                let len = pubcomp
                    .encode(&mut self.tx_buffer, self.options.version)
                    .map_err(MqttError::from)?;
                self.transport.send(&self.tx_buffer[..len]).await?;
                Ok(Some(MqttEvent::PubRel(rel)))
            }
            Some(MqttPacket::PubComp(comp)) => {
                self.inflight.handle_pubcomp(comp.packet_id);
                Ok(Some(MqttEvent::PubComp(comp)))
            }
            Some(MqttPacket::SubAck(suback)) => Ok(Some(MqttEvent::SubAck(suback))),
            Some(MqttPacket::UnsubAck(unsuback)) => Ok(Some(MqttEvent::UnsubAck(unsuback))),
            Some(MqttPacket::PingResp) => Ok(Some(MqttEvent::PingResp)),
            Some(MqttPacket::Disconnect(_)) => {
                self.state = ConnectionState::Disconnected;
                Ok(Some(MqttEvent::Disconnect))
            }
            _ => Ok(None),
        }
    }

    /// Fast-path multi-packet event polling.
    pub async fn poll_batch<'p, const MAX_EVENTS: usize>(
        &'p mut self,
    ) -> Result<Vec<MqttEvent<'p>, MAX_EVENTS>, MqttError<T::Error>>
    where
        T::Error: TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        let n = self.transport.recv(&mut self.rx_buffer).await?;
        if n == 0 {
            return Ok(Vec::new());
        }

        self.rx_len = n;
        self.last_rx_time = Instant::now();

        let mut events = Vec::new();
        let frame_iter = RawPacketFrameIter::new(&self.rx_buffer[..self.rx_len]);

        for frame_res in frame_iter {
            let frame = frame_res.map_err(MqttError::from)?;
            if let Some(packet) = decode(frame, self.options.version)? {
                match packet {
                    MqttPacket::Publish(p) => {
                        if p.qos == QoS::AtLeastOnce {
                            if let Some(pid) = p.packet_id {
                                let puback = PubAck::new(pid);
                                let len = puback
                                    .encode(&mut self.tx_buffer, self.options.version)
                                    .map_err(MqttError::from)?;
                                self.transport.send(&self.tx_buffer[..len]).await?;
                            }
                        } else if p.qos == QoS::ExactlyOnce {
                            if let Some(pid) = p.packet_id {
                                let pubrec = PubRec::new(pid);
                                let len = pubrec
                                    .encode(&mut self.tx_buffer, self.options.version)
                                    .map_err(MqttError::from)?;
                                self.transport.send(&self.tx_buffer[..len]).await?;
                                let _ = self.inflight.track_inbound_qos2::<T::Error>(pid);
                            }
                        }
                        let _ = events.push(MqttEvent::Publish(p));
                    }
                    MqttPacket::PubAck(ack) => {
                        self.inflight.handle_puback(ack.packet_id);
                        let _ = events.push(MqttEvent::PubAck(ack));
                    }
                    MqttPacket::PubRec(rec) => {
                        self.inflight.handle_pubrec(rec.packet_id);
                        let pubrel = PubRel::new(rec.packet_id);
                        let len = pubrel
                            .encode(&mut self.tx_buffer, self.options.version)
                            .map_err(MqttError::from)?;
                        self.transport.send(&self.tx_buffer[..len]).await?;
                        let _ = events.push(MqttEvent::PubRec(rec));
                    }
                    MqttPacket::PubRel(rel) => {
                        self.inflight.handle_pubrel(rel.packet_id);
                        let pubcomp = PubComp::new(rel.packet_id);
                        let len = pubcomp
                            .encode(&mut self.tx_buffer, self.options.version)
                            .map_err(MqttError::from)?;
                        self.transport.send(&self.tx_buffer[..len]).await?;
                        let _ = events.push(MqttEvent::PubRel(rel));
                    }
                    MqttPacket::PubComp(comp) => {
                        self.inflight.handle_pubcomp(comp.packet_id);
                        let _ = events.push(MqttEvent::PubComp(comp));
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
                    MqttPacket::Disconnect(_) => {
                        self.state = ConnectionState::Disconnected;
                        let _ = events.push(MqttEvent::Disconnect);
                    }
                    _ => {}
                }
            }
        }

        Ok(events)
    }

    /// Gracefully disconnects the client session.
    pub async fn disconnect(&mut self) -> Result<(), MqttError<T::Error>>
    where
        T::Error: TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Ok(());
        }

        let disconnect = Disconnect::new();
        let len = disconnect
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::from)?;

        self.transport.send(&self.tx_buffer[..len]).await?;
        self.state = ConnectionState::Disconnected;
        Ok(())
    }
}

/// QUIC MQTT Client wrapper.
pub struct QuicMqttClient<'a, Q: MqttQuicTransport, const BUF_SIZE: usize = 1024> {
    pub transport: Q,
    pub options: MqttOptions<'a>,
    rx_buf: [u8; BUF_SIZE],
}

impl<'a, Q: MqttQuicTransport, const BUF_SIZE: usize> QuicMqttClient<'a, Q, BUF_SIZE> {
    pub fn new(transport: Q, options: MqttOptions<'a>) -> Self {
        Self {
            transport,
            options,
            rx_buf: [0u8; BUF_SIZE],
        }
    }

    pub async fn publish_datagram(&mut self, topic: &str, payload: &[u8]) -> Result<(), MqttError<Q::Error>>
    where
        Q::Error: TransportError,
    {
        let mut buf = [0u8; 1024];
        let mut cursor = 0;
        cursor += mqtt_packet::write_utf8_string(&mut buf[cursor..], topic).map_err(MqttError::from)?;
        if cursor + payload.len() > buf.len() {
            return Err(MqttError::QuicError(crate::error::QuicErrorKind::DatagramTooLarge));
        }
        buf[cursor..cursor + payload.len()].copy_from_slice(payload);
        self.transport.send_datagram(&buf[..cursor + payload.len()]).await?;
        Ok(())
    }

    pub async fn recv_datagram<'p>(&'p mut self) -> Result<Option<MqttEvent<'p>>, MqttError<Q::Error>>
    where
        Q::Error: TransportError,
    {
        let n = self.transport.recv_datagram(&mut self.rx_buf).await?;
        if n == 0 {
            return Ok(None);
        }
        if let Some(MqttPacket::Publish(p)) = decode(&self.rx_buf[..n], self.options.version)? {
            Ok(Some(MqttEvent::Publish(p)))
        } else {
            Ok(None)
        }
    }
}
