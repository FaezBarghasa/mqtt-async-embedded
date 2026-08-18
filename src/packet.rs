//! # MQTT Packet Structures and Serialization
//!
//! This module defines the core MQTT packet types and the traits for encoding and
//! decoding them to and from a byte buffer. It supports both MQTT v3.1.1 and v5.

use crate::client::MqttVersion;
use crate::error::{MqttError, ProtocolError};
use crate::transport;
use crate::util::{self, read_utf8_string, write_utf8_string};
use heapless::Vec;

/// Represents the Quality of Service (QoS) levels for MQTT messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u8)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl From<u8> for QoS {
    fn from(val: u8) -> Self {
        match val {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            _ => QoS::ExactlyOnce,
        }
    }
}

/// A trait for packets that can be encoded into a byte buffer.
pub trait EncodePacket {
    fn encode(
        &self,
        buf: &mut [u8],
        version: MqttVersion,
    ) -> Result<usize, MqttError<transport::ErrorPlaceHolder>>;
}

/// A trait for packets that can be decoded from a byte buffer.
pub trait DecodePacket<'a>: Sized {
    fn decode(
        buf: &'a [u8],
        version: MqttVersion,
    ) -> Result<Self, MqttError<transport::ErrorPlaceHolder>>;
}

/// An enumeration of all possible MQTT control packets.
#[derive(Debug, Clone)]
pub enum MqttPacket<'a> {
    Connect(Connect<'a>),
    ConnAck(ConnAck<'a>),
    Publish(Publish<'a>),
    PubAck(PubAck<'a>),
    Subscribe(Subscribe<'a>),
    SubAck(SubAck<'a>),
    PingReq,
    PingResp,
    Disconnect(Disconnect<'a>),
}

/// Decodes a raw byte buffer into a specific `MqttPacket`.
pub fn decode<'a, T>(
    buf: &'a [u8],
    version: MqttVersion,
) -> Result<Option<MqttPacket<'a>>, MqttError<T>>
where
    T: transport::TransportError,
{
    if buf.is_empty() {
        return Ok(None);
    }

    let packet_type = buf[0] >> 4;
    let packet = match packet_type {
        1 => MqttPacket::Connect(Connect::decode(buf, version).map_err(MqttError::cast_transport_error)?),
        2 => MqttPacket::ConnAck(ConnAck::decode(buf, version).map_err(MqttError::cast_transport_error)?),
        3 => MqttPacket::Publish(Publish::decode(buf, version).map_err(MqttError::cast_transport_error)?),
        4 => MqttPacket::PubAck(PubAck::decode(buf, version).map_err(MqttError::cast_transport_error)?),
        8 => MqttPacket::Subscribe(Subscribe::decode(buf, version).map_err(MqttError::cast_transport_error)?),
        9 => MqttPacket::SubAck(SubAck::decode(buf, version).map_err(MqttError::cast_transport_error)?),
        12 => MqttPacket::PingReq,
        13 => MqttPacket::PingResp,
        14 => MqttPacket::Disconnect(Disconnect::decode(buf, version).map_err(MqttError::cast_transport_error)?),
        _ => return Err(MqttError::Protocol(ProtocolError::InvalidPacketType(packet_type))),
    };

    Ok(Some(packet))
}

#[derive(Debug, Clone)]
pub struct Property<'a> {
    pub id: u8,
    pub data: &'a [u8],
}

// --- CONNECT Packet ---
#[derive(Debug, Clone)]
pub struct Connect<'a> {
    pub clean_session: bool,
    pub keep_alive: u16,
    pub client_id: &'a str,
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> Connect<'a> {
    pub fn new(client_id: &'a str, keep_alive: u16, clean_session: bool) -> Self {
        Self {
            client_id,
            keep_alive,
            clean_session,
            username: None,
            password: None,
            properties: Vec::new(),
        }
    }
}

impl<'a> EncodePacket for Connect<'a> {
    fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, MqttError<transport::ErrorPlaceHolder>> {
        let mut cursor = 0;
        buf[cursor] = 0x10;
        cursor += 1;
        let remaining_len_pos = cursor;
        cursor += 4;
        let content_start = cursor;
        let protocol_name = if version == MqttVersion::V5 { "MQTT" } else { "MQIsdp" };
        cursor += write_utf8_string(&mut buf[cursor..], protocol_name)?;
        buf[cursor] = if version == MqttVersion::V5 { 5 } else { 3 };
        cursor += 1;

        let mut flags = 0;
        if self.clean_session {
            flags |= 0x02;
        }
        if self.username.is_some() {
            flags |= 0x80;
        }
        if self.password.is_some() {
            flags |= 0x40;
        }
        buf[cursor] = flags;
        cursor += 1;

        buf[cursor..cursor + 2].copy_from_slice(&self.keep_alive.to_be_bytes());
        cursor += 2;

        if version == MqttVersion::V5 {
            util::write_properties(&mut cursor, buf, &self.properties)?;
        }

        cursor += write_utf8_string(&mut buf[cursor..], self.client_id)?;
        if let Some(user) = self.username {
            cursor += write_utf8_string(&mut buf[cursor..], user)?;
        }
        if let Some(pass) = self.password {
            cursor += write_utf8_string(&mut buf[cursor..], pass)?;
        }

        let remaining_len = cursor - content_start;
        let len_bytes = util::write_variable_byte_integer_len(&mut buf[remaining_len_pos..], remaining_len)?;
        let header_len = 1 + len_bytes;
        buf.copy_within(content_start..cursor, header_len);
        Ok(header_len + remaining_len)
    }
}

impl<'a> DecodePacket<'a> for Connect<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, MqttError<transport::ErrorPlaceHolder>> {
        let mut cursor = 1;
        let _remaining_len = util::read_variable_byte_integer(&mut cursor, buf)?;
        let _proto_name = read_utf8_string(&mut cursor, buf)?;
        let _proto_level = buf[cursor];
        cursor += 1;
        let connect_flags = buf[cursor];
        cursor += 1;
        let clean_session = (connect_flags & 0x02) != 0;
        let has_username = (connect_flags & 0x80) != 0;
        let has_password = (connect_flags & 0x40) != 0;

        let keep_alive = u16::from_be_bytes([buf[cursor], buf[cursor + 1]]);
        cursor += 2;

        let properties = if version == MqttVersion::V5 {
            util::read_properties(&mut cursor, buf)?
        } else {
            Vec::new()
        };

        let client_id = read_utf8_string(&mut cursor, buf)?;
        let username = if has_username {
            Some(read_utf8_string(&mut cursor, buf)?)
        } else {
            None
        };
        let password = if has_password {
            Some(read_utf8_string(&mut cursor, buf)?)
        } else {
            None
        };

        Ok(Self {
            clean_session,
            keep_alive,
            client_id,
            username,
            password,
            properties,
        })
    }
}

// --- CONNACK Packet ---
#[derive(Debug, Clone)]
pub struct ConnAck<'a> {
    pub session_present: bool,
    pub reason_code: u8,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> DecodePacket<'a> for ConnAck<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, MqttError<transport::ErrorPlaceHolder>> {
        let mut cursor = 1;
        let _remaining_len = util::read_variable_byte_integer(&mut cursor, buf)?;
        if cursor >= buf.len() {
            return Err(MqttError::Protocol(ProtocolError::IncompletePacket));
        }
        let session_present = (buf[cursor] & 0x01) != 0;
        cursor += 1;
        let reason_code = buf[cursor];
        cursor += 1;
        let properties = if version == MqttVersion::V5 && cursor < buf.len() {
            util::read_properties(&mut cursor, buf)?
        } else {
            Vec::new()
        };
        Ok(Self {
            session_present,
            reason_code,
            properties,
        })
    }
}

// --- PUBLISH Packet ---
#[derive(Debug, Clone)]
pub struct Publish<'a> {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: &'a str,
    pub packet_id: Option<u16>,
    pub payload: &'a [u8],
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> Publish<'a> {
    pub fn new(topic: &'a str, payload: &'a [u8], qos: QoS) -> Self {
        Self {
            dup: false,
            qos,
            retain: false,
            topic,
            packet_id: if qos == QoS::AtMostOnce { None } else { Some(1) },
            payload,
            properties: Vec::new(),
        }
    }
}

impl<'a> EncodePacket for Publish<'a> {
    fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, MqttError<transport::ErrorPlaceHolder>> {
        let mut flags = 0x30; // 0x30 = PUBLISH
        if self.dup {
            flags |= 0x08;
        }
        flags |= (self.qos as u8) << 1;
        if self.retain {
            flags |= 0x01;
        }

        let mut cursor = 0;
        buf[cursor] = flags;
        cursor += 1;

        let remaining_len_pos = cursor;
        cursor += 4;
        let content_start = cursor;

        cursor += write_utf8_string(&mut buf[cursor..], self.topic)?;

        if self.qos != QoS::AtMostOnce {
            let pid = self.packet_id.unwrap_or(1);
            buf.get_mut(cursor..cursor + 2)
                .ok_or(MqttError::BufferTooSmall)?
                .copy_from_slice(&pid.to_be_bytes());
            cursor += 2;
        }

        if version == MqttVersion::V5 {
            util::write_properties(&mut cursor, buf, &self.properties)?;
        }

        let end = cursor + self.payload.len();
        buf.get_mut(cursor..end)
            .ok_or(MqttError::BufferTooSmall)?
            .copy_from_slice(self.payload);
        cursor = end;

        let remaining_len = cursor - content_start;
        let len_bytes = util::write_variable_byte_integer_len(&mut buf[remaining_len_pos..], remaining_len)?;
        let header_len = 1 + len_bytes;
        buf.copy_within(content_start..cursor, header_len);
        Ok(header_len + remaining_len)
    }
}

impl<'a> DecodePacket<'a> for Publish<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, MqttError<transport::ErrorPlaceHolder>> {
        if buf.is_empty() {
            return Err(MqttError::Protocol(ProtocolError::IncompletePacket));
        }
        let header = buf[0];
        let dup = (header & 0x08) != 0;
        let qos = QoS::from((header >> 1) & 0x03);
        let retain = (header & 0x01) != 0;

        let mut cursor = 1;
        let remaining_len = util::read_variable_byte_integer(&mut cursor, buf)?;
        let content_start = cursor;
        let content_end = content_start + remaining_len;

        if buf.len() < content_end {
            return Err(MqttError::Protocol(ProtocolError::IncompletePacket));
        }

        let topic = read_utf8_string(&mut cursor, buf)?;

        let packet_id = if qos != QoS::AtMostOnce {
            if cursor + 2 > buf.len() {
                return Err(MqttError::Protocol(ProtocolError::IncompletePacket));
            }
            let pid = u16::from_be_bytes([buf[cursor], buf[cursor + 1]]);
            cursor += 2;
            Some(pid)
        } else {
            None
        };

        let properties = if version == MqttVersion::V5 && cursor < content_end {
            util::read_properties(&mut cursor, buf)?
        } else {
            Vec::new()
        };

        let payload = &buf[cursor..content_end];

        Ok(Publish {
            dup,
            qos,
            retain,
            topic,
            packet_id,
            payload,
            properties,
        })
    }
}

// --- PUBACK Packet ---
#[derive(Debug, Clone)]
pub struct PubAck<'a> {
    pub packet_id: u16,
    pub reason_code: u8,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> PubAck<'a> {
    pub fn new(packet_id: u16) -> Self {
        Self {
            packet_id,
            reason_code: 0,
            properties: Vec::new(),
        }
    }
}

impl<'a> EncodePacket for PubAck<'a> {
    fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, MqttError<transport::ErrorPlaceHolder>> {
        buf[0] = 0x40;
        let mut cursor = 1;
        let remaining_len_pos = cursor;
        cursor += 4;
        let content_start = cursor;

        buf.get_mut(cursor..cursor + 2)
            .ok_or(MqttError::BufferTooSmall)?
            .copy_from_slice(&self.packet_id.to_be_bytes());
        cursor += 2;

        if version == MqttVersion::V5 && (!self.properties.is_empty() || self.reason_code != 0) {
            *buf.get_mut(cursor).ok_or(MqttError::BufferTooSmall)? = self.reason_code;
            cursor += 1;
            util::write_properties(&mut cursor, buf, &self.properties)?;
        }

        let remaining_len = cursor - content_start;
        let len_bytes = util::write_variable_byte_integer_len(&mut buf[remaining_len_pos..], remaining_len)?;
        let header_len = 1 + len_bytes;
        buf.copy_within(content_start..cursor, header_len);
        Ok(header_len + remaining_len)
    }
}

impl<'a> DecodePacket<'a> for PubAck<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, MqttError<transport::ErrorPlaceHolder>> {
        let mut cursor = 1;
        let remaining_len = util::read_variable_byte_integer(&mut cursor, buf)?;
        let content_end = cursor + remaining_len;
        if cursor + 2 > buf.len() {
            return Err(MqttError::Protocol(ProtocolError::IncompletePacket));
        }
        let packet_id = u16::from_be_bytes([buf[cursor], buf[cursor + 1]]);
        cursor += 2;

        let mut reason_code = 0;
        let mut properties = Vec::new();
        if version == MqttVersion::V5 && cursor < content_end {
            reason_code = buf[cursor];
            cursor += 1;
            if cursor < content_end {
                properties = util::read_properties(&mut cursor, buf)?;
            }
        }

        Ok(PubAck {
            packet_id,
            reason_code,
            properties,
        })
    }
}

// --- SUBSCRIBE Packet ---
#[derive(Debug, Clone)]
pub struct Subscribe<'a> {
    pub packet_id: u16,
    pub topics: Vec<(&'a str, QoS), 8>,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> Subscribe<'a> {
    pub fn new(packet_id: u16) -> Self {
        Self {
            packet_id,
            topics: Vec::new(),
            properties: Vec::new(),
        }
    }

    pub fn add_topic(&mut self, topic: &'a str, qos: QoS) -> Result<(), MqttError<transport::ErrorPlaceHolder>> {
        self.topics
            .push((topic, qos))
            .map_err(|_| MqttError::BatchCapacityExceeded)
    }
}

impl<'a> EncodePacket for Subscribe<'a> {
    fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, MqttError<transport::ErrorPlaceHolder>> {
        buf[0] = 0x82; // 0x82 = SUBSCRIBE with QoS 1 in fixed header
        let mut cursor = 1;
        let remaining_len_pos = cursor;
        cursor += 4;
        let content_start = cursor;

        buf.get_mut(cursor..cursor + 2)
            .ok_or(MqttError::BufferTooSmall)?
            .copy_from_slice(&self.packet_id.to_be_bytes());
        cursor += 2;

        if version == MqttVersion::V5 {
            util::write_properties(&mut cursor, buf, &self.properties)?;
        }

        for (topic, qos) in &self.topics {
            cursor += write_utf8_string(&mut buf[cursor..], topic)?;
            *buf.get_mut(cursor).ok_or(MqttError::BufferTooSmall)? = *qos as u8;
            cursor += 1;
        }

        let remaining_len = cursor - content_start;
        let len_bytes = util::write_variable_byte_integer_len(&mut buf[remaining_len_pos..], remaining_len)?;
        let header_len = 1 + len_bytes;
        buf.copy_within(content_start..cursor, header_len);
        Ok(header_len + remaining_len)
    }
}

impl<'a> DecodePacket<'a> for Subscribe<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, MqttError<transport::ErrorPlaceHolder>> {
        let mut cursor = 1;
        let remaining_len = util::read_variable_byte_integer(&mut cursor, buf)?;
        let content_end = cursor + remaining_len;
        if cursor + 2 > buf.len() {
            return Err(MqttError::Protocol(ProtocolError::IncompletePacket));
        }
        let packet_id = u16::from_be_bytes([buf[cursor], buf[cursor + 1]]);
        cursor += 2;

        let properties = if version == MqttVersion::V5 && cursor < content_end {
            util::read_properties(&mut cursor, buf)?
        } else {
            Vec::new()
        };

        let mut topics = Vec::new();
        while cursor < content_end {
            let topic = read_utf8_string(&mut cursor, buf)?;
            let qos = QoS::from(buf[cursor]);
            cursor += 1;
            topics.push((topic, qos)).map_err(|_| MqttError::BatchCapacityExceeded)?;
        }

        Ok(Subscribe {
            packet_id,
            topics,
            properties,
        })
    }
}

// --- SUBACK Packet ---
#[derive(Debug, Clone)]
pub struct SubAck<'a> {
    pub packet_id: u16,
    pub reason_codes: Vec<u8, 8>,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> DecodePacket<'a> for SubAck<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, MqttError<transport::ErrorPlaceHolder>> {
        let mut cursor = 1;
        let remaining_len = util::read_variable_byte_integer(&mut cursor, buf)?;
        let content_end = cursor + remaining_len;
        if cursor + 2 > buf.len() {
            return Err(MqttError::Protocol(ProtocolError::IncompletePacket));
        }
        let packet_id = u16::from_be_bytes([buf[cursor], buf[cursor + 1]]);
        cursor += 2;

        let properties = if version == MqttVersion::V5 && cursor < content_end {
            util::read_properties(&mut cursor, buf)?
        } else {
            Vec::new()
        };

        let mut reason_codes = Vec::new();
        while cursor < content_end {
            reason_codes.push(buf[cursor]).map_err(|_| MqttError::BatchCapacityExceeded)?;
            cursor += 1;
        }

        Ok(SubAck {
            packet_id,
            reason_codes,
            properties,
        })
    }
}

// --- PINGREQ Packet ---
#[derive(Debug, Clone, Copy)]
pub struct PingReq;

impl EncodePacket for PingReq {
    fn encode(&self, buf: &mut [u8], _version: MqttVersion) -> Result<usize, MqttError<transport::ErrorPlaceHolder>> {
        if buf.len() < 2 {
            return Err(MqttError::BufferTooSmall);
        }
        buf[0] = 0xC0;
        buf[1] = 0x00;
        Ok(2)
    }
}

// --- PINGRESP Packet ---
#[derive(Debug, Clone, Copy)]
pub struct PingResp;

// --- DISCONNECT Packet ---
#[derive(Debug, Clone)]
pub struct Disconnect<'a> {
    pub reason_code: u8,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> Disconnect<'a> {
    pub fn new() -> Self {
        Self {
            reason_code: 0,
            properties: Vec::new(),
        }
    }
}

impl<'a> Default for Disconnect<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> DecodePacket<'a> for Disconnect<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, MqttError<transport::ErrorPlaceHolder>> {
        let mut cursor = 1;
        let mut reason_code = 0;
        let mut properties = Vec::new();

        if buf.len() > 1 {
            let remaining_len = util::read_variable_byte_integer(&mut cursor, buf)?;
            let content_end = cursor + remaining_len;
            if version == MqttVersion::V5 && cursor < content_end {
                reason_code = buf[cursor];
                cursor += 1;
                if cursor < content_end {
                    properties = util::read_properties(&mut cursor, buf)?;
                }
            }
        }

        Ok(Disconnect {
            reason_code,
            properties,
        })
    }
}

impl<'a> EncodePacket for Disconnect<'a> {
    fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, MqttError<transport::ErrorPlaceHolder>> {
        buf[0] = 0xE0;
        let mut cursor = 1;
        let remaining_len_pos = cursor;
        cursor += 4;
        let content_start = cursor;

        if version == MqttVersion::V5 && (!self.properties.is_empty() || self.reason_code != 0) {
            *buf.get_mut(cursor).ok_or(MqttError::BufferTooSmall)? = self.reason_code;
            cursor += 1;
            util::write_properties(&mut cursor, buf, &self.properties)?;
        }

        let remaining_len = cursor - content_start;
        let len_bytes = util::write_variable_byte_integer_len(&mut buf[remaining_len_pos..], remaining_len)?;
        let header_len = 1 + len_bytes;
        buf.copy_within(content_start..cursor, header_len);
        Ok(header_len + remaining_len)
    }
}
