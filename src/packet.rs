//! Routines for encoding and decoding MQTT control packets.

use crate::client::MqttVersion;
use crate::error::{ConnectReasonCode, MqttError, ProtocolError};
use crate::transport;
use crate::util::{
    decode_variable_byte_integer, encode_variable_byte_integer, read_utf8_string,
    write_utf8_string,
};
use heapless::Vec;

/// Represents the Quality of Service levels in MQTT.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u8)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

#[cfg(feature = "v5")]
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum Property<'a> {
    PayloadFormatIndicator(u8),
    MessageExpiryInterval(u32),
    ContentType(&'a str),
    ResponseTopic(&'a str),
    CorrelationData(&'a [u8]),
    SubscriptionIdentifier(u32),
    SessionExpiryInterval(u32),
    AssignedClientIdentifier(&'a str),
    ServerKeepAlive(u16),
    AuthenticationMethod(&'a str),
    AuthenticationData(&'a [u8]),
    RequestProblemInformation(u8),
    WillDelayInterval(u32),
    RequestResponseInformation(u8),
    ResponseInformation(&'a str),
    ServerReference(&'a str),
    ReasonString(&'a str),
    ReceiveMaximum(u16),
    TopicAliasMaximum(u16),
    TopicAlias(u16),
    MaximumQoS(u8),
    RetainAvailable(u8),
    UserProperty((&'a str, &'a str)),
    MaximumPacketSize(u32),
    WildcardSubscriptionAvailable(u8),
    SubscriptionIdentifierAvailable(u8),
    SharedSubscriptionAvailable(u8),
}

#[cfg(feature = "v5")]
pub type Properties<'a> = Vec<Property<'a>, 16>;

/// Trait for all MQTT control packets that can be decoded from the broker.
pub trait DecodePacket<'a>: Sized {
    fn decode<T: transport::TransportError>(
        buf: &'a [u8],
        version: MqttVersion,
    ) -> Result<Self, MqttError<T>>;
}

/// Trait for all MQTT control packets that can be encoded to send to the broker.
pub trait EncodePacket {
    fn encode<T: transport::TransportError>(
        &self,
        buf: &mut [u8],
        version: MqttVersion,
    ) -> Result<usize, MqttError<T>>;
}

/// Represents incoming packets from the broker.
#[derive(Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MqttPacket<'a> {
    ConnAck(ConnAck),
    Publish(Publish<'a>),
    PubAck(PubAck),
    SubAck(SubAck),
    PingResp,
}

/// Decodes a raw buffer into an `MqttPacket` variant.
pub fn decode<'a, T: transport::TransportError>(
    buf: &'a [u8],
    version: MqttVersion,
) -> Result<Option<MqttPacket<'a>>, MqttError<T>> {
    if buf.is_empty() {
        return Ok(None);
    }

    let packet_type = buf[0] >> 4;
    match packet_type {
        2 => Ok(Some(MqttPacket::ConnAck(ConnAck::decode(buf, version)?))),
        3 => Ok(Some(MqttPacket::Publish(Publish::decode(buf, version)?))),
        4 => Ok(Some(MqttPacket::PubAck(PubAck::decode(buf, version)?))),
        9 => Ok(Some(MqttPacket::SubAck(SubAck::decode(buf, version)?))),
        13 => Ok(Some(MqttPacket::PingResp)),
        _ => Err(MqttError::Protocol(ProtocolError::InvalidPacketType(
            packet_type,
        ))),
    }
}

// --- CONNECT Packet ---

#[derive(Debug, Default)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Connect<'a> {
    pub clean_session: bool,
    pub keep_alive: u16,
    pub client_id: &'a str,
    #[cfg(feature = "v5")]
    pub properties: Properties<'a>,
}

impl<'a> EncodePacket for Connect<'a> {
    fn encode<T: transport::TransportError>(
        &self,
        buf: &mut [u8],
        version: MqttVersion,
    ) -> Result<usize, MqttError<T>> {
        let mut cursor = 0;
        // Fixed header
        buf[cursor] = 0x10;
        cursor += 2; // Reserve space for remaining length

        // Variable Header
        // Protocol Name
        cursor += write_utf8_string(&mut buf[cursor..], "MQTT")?;
        // Protocol Version
        #[cfg(feature = "v5")]
        if version == MqttVersion::V5 {
            buf[cursor] = 5;
        } else {
            buf[cursor] = 4;
        }
        #[cfg(not(feature = "v5"))]
        {
            buf[cursor] = 4;
        }
        cursor += 1;

        // Connect Flags
        let mut flags = 0;
        if self.clean_session {
            flags |= 0x02;
        }
        buf[cursor] = flags;
        cursor += 1;

        // Keep Alive
        buf[cursor..cursor + 2].copy_from_slice(&self.keep_alive.to_be_bytes());
        cursor += 2;

        // V5 Properties
        #[cfg(feature = "v5")]
        if version == MqttVersion::V5 {
            let properties_len = encode_properties(&mut buf[cursor..], &self.properties)?;
            cursor += properties_len;
        }

        // Payload: Client ID
        cursor += write_utf8_string(&mut buf[cursor..], self.client_id)?;

        // Fill in remaining length
        let remaining_len = cursor - 2;
        let len_bytes = encode_variable_byte_integer(&mut buf[1..], remaining_len as u32)?;
        // Shift variable header and payload to make space for multi-byte remaining length
        if len_bytes > 1 {
            buf.copy_within(2..cursor, 1 + len_bytes);
            cursor += len_bytes - 1;
        }

        Ok(cursor)
    }
}

// --- CONNACK Packet ---
#[derive(Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ConnAck {
    pub session_present: bool,
    pub reason_code: u8,
    #[cfg(feature = "v5")]
    pub properties: Properties<'static>,
}

impl<'a> DecodePacket<'a> for ConnAck {
    fn decode<T: transport::TransportError>(
        buf: &'a [u8],
        version: MqttVersion,
    ) -> Result<Self, MqttError<T>> {
        #[cfg(feature = "v5")]
        if version == MqttVersion::V5 {
            let (remaining_len, len_bytes) = decode_variable_byte_integer(&buf[1..])?;
            if buf.len() < 2 + len_bytes + remaining_len as usize {
                return Err(MqttError::BufferTooSmall);
            }
            let session_present = (buf[2 + len_bytes] & 0x01) != 0;
            let reason_code = buf[3 + len_bytes];
            let (properties, _) =
                decode_properties(&buf[4 + len_bytes..4 + len_bytes + remaining_len as usize])?;
            return Ok(ConnAck {
                session_present,
                reason_code,
                properties,
            });
        }

        // V3.1.1 decoding
        if buf.len() < 4 || buf[0] != 0x20 || buf[1] != 2 {
            return Err(MqttError::Protocol(ProtocolError::InvalidPacketType(
                buf[0],
            )));
        }
        Ok(ConnAck {
            session_present: (buf[2] & 0x01) != 0,
            reason_code: buf[3],
            #[cfg(feature = "v5")]
            properties: Vec::new(),
        })
    }
}

// --- PUBLISH Packet ---

#[derive(Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Publish<'a> {
    pub topic: &'a str,
    pub qos: QoS,
    pub payload: &'a [u8],
    pub packet_id: Option<u16>,
    #[cfg(feature = "v5")]
    pub properties: Properties<'a>,
}

impl<'a> EncodePacket for Publish<'a> {
    fn encode<T: transport::TransportError>(
        &self,
        buf: &mut [u8],
        _version: MqttVersion,
    ) -> Result<usize, MqttError<T>> {
        // Simplified encoder, does not support V5 properties yet for brevity
        let mut cursor = 2; // Start after fixed header
        let topic_len = write_utf8_string(&mut buf[cursor..], self.topic)?;
        cursor += topic_len;

        if let Some(packet_id) = self.packet_id {
            buf[cursor..cursor + 2].copy_from_slice(&packet_id.to_be_bytes());
            cursor += 2;
        }

        let payload_len = self.payload.len();
        buf[cursor..cursor + payload_len].copy_from_slice(self.payload);
        cursor += payload_len;

        // Fixed header
        buf[0] = 0x30 | ((self.qos as u8) << 1);
        buf[1] = (cursor - 2) as u8; // Simplified length

        Ok(cursor)
    }
}

impl<'a> DecodePacket<'a> for Publish<'a> {
    fn decode<T: transport::TransportError>(
        buf: &'a [u8],
        _version: MqttVersion,
    ) -> Result<Self, MqttError<T>> {
        // Simplified decoder, does not support V5 properties yet for brevity
        let (topic, topic_len_consumed) = read_utf8_string(&buf[2..])?;
        let payload = &buf[2 + topic_len_consumed..];

        Ok(Publish {
            topic,
            qos: QoS::AtMostOnce, // Simplified
            payload,
            packet_id: None, // Simplified
            #[cfg(feature = "v5")]
            properties: Vec::new(),
        })
    }
}

// Other packet types (PubAck, Subscribe, etc.) would be updated similarly...
// For brevity, only showing stubs.

#[derive(Debug)]
pub struct PubAck {
    pub packet_id: u16,
}
impl DecodePacket<'_> for PubAck {
    fn decode<T: transport::TransportError>(
        _buf: &'_ [u8],
        _version: MqttVersion,
    ) -> Result<Self, MqttError<T>> {
        Ok(PubAck { packet_id: 0 })
    }
}

#[derive(Debug)]
pub struct Subscribe<'a> {
    pub packet_id: u16,
    pub topics: Vec<(&'a str, QoS), 8>,
    #[cfg(feature = "v5")]
    pub properties: Properties<'a>,
}
impl<'a> EncodePacket for Subscribe<'a> {
    fn encode<T: transport::TransportError>(
        &self,
        _buf: &mut [u8],
        _version: MqttVersion,
    ) -> Result<usize, MqttError<T>> {
        Ok(0)
    }
}

#[derive(Debug)]
pub struct SubAck {
    pub packet_id: u16,
    pub return_codes: Vec<u8, 8>,
}
impl<'a> DecodePacket<'a> for SubAck {
    fn decode<T: transport::TransportError>(
        _buf: &'a [u8],
        _version: MqttVersion,
    ) -> Result<Self, MqttError<T>> {
        Ok(SubAck {
            packet_id: 0,
            return_codes: Vec::new(),
        })
    }
}

pub struct PingReq;
impl EncodePacket for PingReq {
    fn encode<T: transport::TransportError>(
        &self,
        buf: &mut [u8],
        _version: MqttVersion,
    ) -> Result<usize, MqttError<T>> {
        if buf.len() < 2 {
            return Err(MqttError::BufferTooSmall);
        }
        buf[0] = 0xC0;
        buf[1] = 0x00;
        Ok(2)
    }
}

pub struct Disconnect;
impl EncodePacket for Disconnect {
    fn encode<T: transport::TransportError>(
        &self,
        buf: &mut [u8],
        _version: MqttVersion,
    ) -> Result<usize, MqttError<T>> {
        if buf.len() < 2 {
            return Err(MqttError::BufferTooSmall);
        }
        buf[0] = 0xE0;
        buf[1] = 0x00;
        Ok(2)
    }
}

#[cfg(feature = "v5")]
fn encode_properties<'a, T: transport::TransportError>(
    buf: &mut [u8],
    properties: &[Property<'a>],
) -> Result<usize, MqttError<T>> {
    let mut cursor = 0;
    let mut properties_len = 0;

    // First pass to calculate length
    for prop in properties {
        properties_len += 1; // Property identifier
        match prop {
            Property::PayloadFormatIndicator(_) => properties_len += 1,
            Property::MessageExpiryInterval(_) => properties_len += 4,
            Property::ContentType(s) => properties_len += 2 + s.len(),
            // ... other properties
            _ => {}
        }
    }

    let len_bytes = encode_variable_byte_integer(&mut buf[cursor..], properties_len as u32)?;
    cursor += len_bytes;

    // Second pass to write properties
    for prop in properties {
        match prop {
            Property::PayloadFormatIndicator(v) => {
                buf[cursor] = 0x01;
                buf[cursor + 1] = *v;
                cursor += 2;
            }
            Property::MessageExpiryInterval(v) => {
                buf[cursor] = 0x02;
                buf[cursor + 1..cursor + 5].copy_from_slice(&v.to_be_bytes());
                cursor += 5;
            }
            Property::ContentType(s) => {
                buf[cursor] = 0x03;
                cursor += 1;
                cursor += write_utf8_string(&mut buf[cursor..], s)?;
            }
            _ => {}
        }
    }

    Ok(cursor)
}

#[cfg(feature = "v5")]
fn decode_properties<'a, T: transport::TransportError>(
    buf: &'a [u8],
) -> Result<(Properties<'a>, usize), MqttError<T>> {
    let mut properties = Properties::new();
    if buf.is_empty() {
        return Ok((properties, 0));
    }

    let (mut properties_len, mut cursor) = decode_variable_byte_integer(buf)?;

    let initial_cursor = cursor;

    while properties_len > 0 {
        let identifier = buf[cursor];
        cursor += 1;
        properties_len -= 1;
        match identifier {
            0x01 => {
                properties
                    .push(Property::PayloadFormatIndicator(buf[cursor]))
                    .map_err(|_| MqttError::BufferTooSmall)?;
                cursor += 1;
                properties_len -= 1;
            }
            // ... other properties
            _ => {
                // Unknown property, skip it
                // This requires a proper property length decoder which is complex.
                // For now, we assume we know all properties.
                return Err(MqttError::Protocol(
                    ProtocolError::InvalidPropertyIdentifier(identifier),
                ));
            }
        }
    }
    Ok((properties, cursor - initial_cursor))
}

