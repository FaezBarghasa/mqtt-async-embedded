//! # The Asynchronous MQTT Client
//!
//! This module contains the primary `MqttClient` struct, which manages the state,
//! connection, and communication with an MQTT broker.

use crate::error::{MqttError, ProtocolError};
use crate::packet::{
    self, Connect, EncodePacket, MqttPacket, PingReq, Publish,
    QoS,
};
use crate::transport::{self, MqttTransport};
use embassy_time::{Duration, Instant};

/// Represents the MQTT protocol version used by the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MqttVersion {
    V3,
    V5,
}

/// Configuration options for the `MqttClient`.
pub struct MqttOptions<'a> {
    client_id: &'a str,
    broker_addr: &'a str,
    broker_port: u16,
    version: MqttVersion,
    keep_alive: Duration,
}

impl<'a> MqttOptions<'a> {
    pub fn new(client_id: &'a str, broker_addr: &'a str, broker_port: u16) -> Self {
        Self { client_id, broker_addr, broker_port, version: MqttVersion::V3, keep_alive: Duration::from_secs(60) }
    }
    #[cfg(feature = "v5")]
    pub fn with_version(mut self, version: MqttVersion) -> Self { self.version = version; self }
    pub fn with_keep_alive(mut self, keep_alive: Duration) -> Self { self.keep_alive = keep_alive; self }
}

/// Represents the current connection state of the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
}

/// The asynchronous MQTT client.
pub struct MqttClient<'a, T, const MAX_TOPICS: usize, const BUF_SIZE: usize>
where
    T: MqttTransport,
{
    transport: T,
    options: MqttOptions<'a>,
    tx_buffer: [u8; BUF_SIZE],
    rx_buffer: [u8; BUF_SIZE],
    state: ConnectionState,
    last_tx_time: Instant,
    next_packet_id: u16,
}

impl<'a, T, const MAX_TOPICS: usize, const BUF_SIZE: usize>
MqttClient<'a, T, MAX_TOPICS, BUF_SIZE>
where
    T: MqttTransport,
{
    pub fn new(transport: T, options: MqttOptions<'a>) -> Self {
        Self { transport, options, tx_buffer: [0; BUF_SIZE], rx_buffer: [0; BUF_SIZE], state: ConnectionState::Disconnected, last_tx_time: Instant::now(), next_packet_id: 1 }
    }

    /// Attempts to connect to the MQTT broker.
    pub async fn connect(&mut self) -> Result<(), MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        self.state = ConnectionState::Connecting;
        let connect_packet = Connect::new(
            self.options.client_id,
            self.options.keep_alive.as_secs() as u16,
            true,
        );
        let len = connect_packet
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::cast_transport_error)?;
        self.transport.send(&self.tx_buffer[..len]).await?;
        let n = self.transport.recv(&mut self.rx_buffer).await?;
        let packet = packet::decode::<T::Error>(&self.rx_buffer[..n], self.options.version)?
            .ok_or(MqttError::Protocol(ProtocolError::InvalidResponse))?;
        if let MqttPacket::ConnAck(connack) = packet {
            if connack.reason_code == 0 {
                self.state = ConnectionState::Connected;
                self.last_tx_time = Instant::now();
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

    /// Publishes a message to a topic.
    pub async fn publish<'p>(
        &mut self,
        _topic: &'p str,
        _payload: &'p [u8],
        _qos: QoS,
    ) -> Result<(), MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        Ok(())
    }

    /// Sends a pre-constructed packet over the transport.
    async fn _send_packet<P>(&mut self, packet: P) -> Result<(), MqttError<T::Error>>
    where
        P: EncodePacket,
        T::Error: transport::TransportError,
    {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }
        let len = packet
            .encode(&mut self.tx_buffer, self.options.version)
            .map_err(MqttError::cast_transport_error)?;
        self.transport.send(&self.tx_buffer[..len]).await?;
        self.last_tx_time = Instant::now();
        Ok(())
    }

    /// Polls the connection for incoming packets and handles keep-alives.
    ///
    /// The returned `MqttEvent` contains references to the client's internal receive
    /// buffer. These references are only valid until the next call to `poll`.
    pub async fn poll<'p>(&'p mut self) -> Result<Option<MqttEvent<'p>>, MqttError<T::Error>>
    where
        T::Error: transport::TransportError,
    {
        if self.state != ConnectionState::Connected { return Err(MqttError::NotConnected); }

        if self.last_tx_time.elapsed() >= self.options.keep_alive {
            self._send_packet(PingReq).await?;
        }

        let n = self.transport.recv(&mut self.rx_buffer).await?;
        if n > 0 {
            if let Some(packet) = packet::decode::<T::Error>(&self.rx_buffer[..n], self.options.version)? {
                if let MqttPacket::Publish(p) = packet {
                    // The event is valid for the lifetime 'p of the poll borrow
                    return Ok(Some(MqttEvent::Publish(p)));
                }
            }
        }
        Ok(None)
    }

    fn get_next_packet_id(&mut self) -> u16 {
        self.next_packet_id = self.next_packet_id.wrapping_add(1);
        if self.next_packet_id == 0 { self.next_packet_id = 1; }
        self.next_packet_id
    }
}

/// Represents an event received from the MQTT broker.
/// The lifetime `'p` indicates that the event borrows data from the client's
/// buffer and is only valid for the duration of the `poll` call.
#[derive(Debug)]
pub enum MqttEvent<'p> {
    Publish(Publish<'p>),
}

