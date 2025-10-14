//! The asynchronous MQTT client.

use crate::error::{ConnectReasonCode, MqttError, ProtocolError};
use crate::packet::{
    self, ConnAck, Connect, Disconnect, EncodePacket, MqttPacket, PingReq, Publish, QoS,
    Subscribe,
};
#[cfg(feature = "v5")]
use crate::packet::{Properties, Property};
use crate::transport::MqttTransport;
use embassy_time::{Duration, Instant};
use heapless::Vec;

/// The MQTT protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MqttVersion {
    /// MQTT v3.1.1
    V3_1_1,
    /// MQTT v5
    #[cfg(feature = "v5")]
    V5,
}

/// The connection state of the MQTT client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ConnectionState {
    /// The client is disconnected.
    Disconnected,
    /// The client is currently connecting.
    Connecting,
    /// The client is connected.
    Connected,
}

/// Options for configuring the MQTT client.
#[derive(Debug)]
pub struct MqttOptions<'a> {
    /// The client ID to use when connecting to the broker.
    pub client_id: &'a str,
    /// The keep-alive interval in seconds.
    pub keep_alive: Duration,
    /// Whether to start a clean session.
    pub clean_session: bool,
    /// The MQTT protocol version to use.
    pub version: MqttVersion,
    /// MQTT v5 connect properties.
    #[cfg(feature = "v5")]
    pub connect_properties: Properties<'a>,
}

impl<'a> MqttOptions<'a> {
    /// Creates a new `MqttOptions` with default values.
    pub fn new(client_id: &'a str) -> Self {
        Self {
            client_id,
            keep_alive: Duration::from_secs(30),
            clean_session: true,
            version: MqttVersion::V3_1_1,
            #[cfg(feature = "v5")]
            connect_properties: Vec::new(),
        }
    }

    /// Sets the MQTT protocol version.
    pub fn set_version(mut self, version: MqttVersion) -> Self {
        self.version = version;
        self
    }

    /// Sets the MQTT v5 connect properties.
    #[cfg(feature = "v5")]
    pub fn set_connect_properties(mut self, properties: Properties<'a>) -> Self {
        self.connect_properties = properties;
        self
    }

    /// Sets the keep-alive interval.
    pub fn set_keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }
}

/// An asynchronous MQTT client.
pub struct MqttClient<'a, T, const R: usize, const W: usize>
where
    T: MqttTransport,
{
    transport: T,
    options: MqttOptions<'a>,
    state: ConnectionState,
    rx_buffer: [u8; R],
    tx_buffer: [u8; W],
    last_ping: Instant,
    next_packet_id: u16,
}

impl<'a, T, const R: usize, const W: usize> MqttClient<'a, T, R, W>
where
    T: MqttTransport,
{
    /// Creates a new `MqttClient`.
    pub fn new(transport: T, options: MqttOptions<'a>) -> Self {
        Self {
            transport,
            options,
            state: ConnectionState::Disconnected,
            rx_buffer: [0; R],
            tx_buffer: [0; W],
            last_ping: Instant::now(),
            next_packet_id: 1,
        }
    }

    /// Returns the next available packet identifier.
    fn get_packet_id(&mut self) -> u16 {
        let id = self.next_packet_id;
        self.next_packet_id = self.next_packet_id.wrapping_add(1);
        if self.next_packet_id == 0 {
            self.next_packet_id = 1;
        }
        id
    }

    /// Connects to the MQTT broker.
    pub async fn connect(&mut self) -> Result<(), MqttError<T::Error>> {
        self.state = ConnectionState::Connecting;

        let connect_packet = Connect {
            clean_session: self.options.clean_session,
            keep_alive: self.options.keep_alive.as_secs() as u16,
            client_id: self.options.client_id,
            #[cfg(feature = "v5")]
            properties: self.options.connect_properties.clone(),
        };

        let len = connect_packet.encode(&mut self.tx_buffer, self.options.version)?;
        self.transport.send(&self.tx_buffer[..len]).await?;

        // Wait for ConnAck
        let len = self.transport.recv(&mut self.rx_buffer).await?;
        let packet = packet::decode(&self.rx_buffer[..len], self.options.version)?;

        if let Some(MqttPacket::ConnAck(ConnAck {
                                            reason_code, ..
                                        })) = packet
        {
            let reason_code = ConnectReasonCode::from(reason_code);
            if reason_code == ConnectReasonCode::Success {
                self.state = ConnectionState::Connected;
                self.last_ping = Instant::now();
                Ok(())
            } else {
                Err(MqttError::ConnectionRefused(reason_code))
            }
        } else {
            Err(MqttError::Protocol(ProtocolError::InvalidResponse))
        }
    }

    /// Publishes a message to a topic.
    #[cfg(not(feature = "v5"))]
    pub async fn publish<'p>(
        &mut self,
        topic: &'p str,
        payload: &'p [u8],
        qos: QoS,
    ) -> Result<(), MqttError<T::Error>> {
        let packet_id = if qos == QoS::AtMostOnce {
            None
        } else {
            Some(self.get_packet_id())
        };

        let publish_packet = Publish {
            topic,
            qos,
            payload,
            packet_id,
        };
        self.send_publish_packet(publish_packet).await
    }

    /// Publishes a message to a topic (MQTT v5).
    #[cfg(feature = "v5")]
    pub async fn publish<'p>(
        &mut self,
        topic: &'p str,
        payload: &'p [u8],
        qos: QoS,
        properties: &[Property<'p>],
    ) -> Result<(), MqttError<T::Error>> {
        let packet_id = if qos == QoS::AtMostOnce {
            None
        } else {
            Some(self.get_packet_id())
        };

        let publish_packet = Publish {
            topic,
            qos,
            payload,
            packet_id,
            properties: {
                let mut props = Vec::new();
                props.extend_from_slice(properties).unwrap();
                props
            },
        };
        self.send_publish_packet(publish_packet).await
    }

    /// Sends a `Publish` packet.
    async fn send_publish_packet<'p>(
        &mut self,
        publish_packet: Publish<'p>,
    ) -> Result<(), MqttError<T::Error>> {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }

        let len = publish_packet.encode(&mut self.tx_buffer, self.options.version)?;
        self.transport.send(&self.tx_buffer[..len]).await?;
        Ok(())
    }

    /// Subscribes to a topic.
    pub async fn subscribe<'p>(
        &mut self,
        topic: &'p str,
        qos: QoS,
    ) -> Result<(), MqttError<T::Error>> {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }
        let packet_id = self.get_packet_id();
        let mut topics = Vec::<(&str, QoS), 8>::new();
        topics.push((topic, qos)).ok(); // Ignore error on full vec for now

        let subscribe_packet = Subscribe {
            packet_id,
            topics,
            #[cfg(feature = "v5")]
            properties: Vec::new(),
        };

        let len = subscribe_packet.encode(&mut self.tx_buffer, self.options.version)?;
        self.transport.send(&self.tx_buffer[..len]).await?;

        // Awaiting SubAck
        let len = self.transport.recv(&mut self.rx_buffer).await?;
        if let Some(MqttPacket::SubAck(suback)) =
            packet::decode(&self.rx_buffer[..len], self.options.version)?
        {
            if suback.packet_id == packet_id {
                // TODO: Check reason codes in V5
                Ok(())
            } else {
                Err(MqttError::Protocol(ProtocolError::UnmatchedPacketId))
            }
        } else {
            Err(MqttError::Protocol(ProtocolError::InvalidResponse))
        }
    }

    /// Sends a `PINGREQ` packet to the broker.
    pub async fn ping(&mut self) -> Result<(), MqttError<T::Error>> {
        if self.state != ConnectionState::Connected {
            return Err(MqttError::NotConnected);
        }
        let len = PingReq.encode(&mut self.tx_buffer, self.options.version)?;
        self.transport.send(&self.tx_buffer[..len]).await?;
        self.last_ping = Instant::now();
        Ok(())
    }

    /// Disconnects from the MQTT broker.
    pub async fn disconnect(&mut self) -> Result<(), MqttError<T::Error>> {
        let len = Disconnect.encode(&mut self.tx_buffer, self.options.version)?;
        self.transport.send(&self.tx_buffer[..len]).await?;
        self.state = ConnectionState::Disconnected;
        Ok(())
    }

    /// Polls the transport for incoming packets and handles keep-alive.
    /// This should be called regularly in a background task.
    pub async fn poll(&mut self) -> Result<Option<MqttPacket>, MqttError<T::Error>> {
        if self.state != ConnectionState::Connected {
            // Do not poll if not connected, but don't return an error either,
            // as the poll loop might run before connect() is called.
            return Ok(None);
        }

        // Check if we need to send a ping
        if self.last_ping.elapsed() >= self.options.keep_alive {
            self.ping().await?;
        }

        // Try to receive a packet. A transport supporting timeouts is recommended.
        match self.transport.recv(&mut self.rx_buffer).await {
            Ok(len) if len > 0 => packet::decode(&self.rx_buffer[..len], self.options.version),
            Ok(_) => Ok(None), // No data received or connection closed gracefully
            Err(e) => Err(e.into()),
        }
    }
}