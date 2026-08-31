//! # MQTT Packet Encoders and Decoders

use crate::error::PacketError;
use crate::packet_types::{
    ConnAck, Connect, Disconnect, MqttPacket, MqttVersion, PingReq, PingResp, PubAck, PubComp,
    PubRec, PubRel, Publish, QoS, SubAck, Subscribe, UnsubAck, Unsubscribe, Will,
};
use crate::properties::{read_properties, write_properties};
use crate::varint::{
    peek_variable_byte_integer, read_binary_data, read_utf8_string, read_variable_byte_integer,
    write_binary_data, write_utf8_string, write_variable_byte_integer_len,
};
use heapless::Vec;

/// Trait for packets that can be serialized into a byte buffer.
pub trait EncodePacket {
    /// Encodes the packet into the provided buffer. Returns the number of bytes written.
    fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, PacketError>;
}

/// Trait for packets that can be deserialized from a byte buffer.
pub trait DecodePacket<'a>: Sized {
    /// Decodes a packet from the provided buffer.
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, PacketError>;
}

// --- CONNECT ---
impl<'a> EncodePacket for Connect<'a> {
    fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, PacketError> {
        *buf.get_mut(0).ok_or(PacketError::BufferTooSmall)? = 0x10; // 0x10 = CONNECT
        let mut cursor = 1;
        let remaining_len_pos = cursor;
        cursor += 4; // Reserve up to 4 bytes for Variable Byte Integer
        let content_start = cursor;

        // Protocol Name & Level
        match version {
            MqttVersion::V3_1_1 | MqttVersion::V3 => {
                cursor += write_utf8_string(
                    buf.get_mut(cursor..).ok_or(PacketError::BufferTooSmall)?,
                    "MQTT",
                )?;
                *buf.get_mut(cursor).ok_or(PacketError::BufferTooSmall)? = 0x04;
                cursor += 1;
            }
            MqttVersion::V5 => {
                cursor += write_utf8_string(
                    buf.get_mut(cursor..).ok_or(PacketError::BufferTooSmall)?,
                    "MQTT",
                )?;
                *buf.get_mut(cursor).ok_or(PacketError::BufferTooSmall)? = 0x05;
                cursor += 1;
            }
        }

        // Connect Flags
        let mut flags = 0u8;
        if self.clean_session {
            flags |= 0x02;
        }
        if let Some(ref will) = self.will {
            flags |= 0x04;
            flags |= (will.qos as u8) << 3;
            if will.retain {
                flags |= 0x20;
            }
        }
        if self.password.is_some() {
            flags |= 0x40;
        }
        if self.username.is_some() {
            flags |= 0x80;
        }

        *buf.get_mut(cursor).ok_or(PacketError::BufferTooSmall)? = flags;
        cursor += 1;

        // Keep Alive
        buf.get_mut(cursor..cursor + 2)
            .ok_or(PacketError::BufferTooSmall)?
            .copy_from_slice(&self.keep_alive.to_be_bytes());
        cursor += 2;

        // MQTT v5 Properties
        if version == MqttVersion::V5 {
            write_properties(&mut cursor, buf, &self.properties)?;
        }

        // Payload: Client Identifier
        cursor += write_utf8_string(
            buf.get_mut(cursor..).ok_or(PacketError::BufferTooSmall)?,
            self.client_id,
        )?;

        // Payload: Will
        if let Some(ref will) = self.will {
            if version == MqttVersion::V5 {
                write_properties(&mut cursor, buf, &will.properties)?;
            }
            cursor += write_utf8_string(
                buf.get_mut(cursor..).ok_or(PacketError::BufferTooSmall)?,
                will.topic,
            )?;
            cursor += write_binary_data(
                buf.get_mut(cursor..).ok_or(PacketError::BufferTooSmall)?,
                will.payload,
            )?;
        }

        // Payload: Username
        if let Some(user) = self.username {
            cursor += write_utf8_string(
                buf.get_mut(cursor..).ok_or(PacketError::BufferTooSmall)?,
                user,
            )?;
        }

        // Payload: Password
        if let Some(pass) = self.password {
            cursor += write_utf8_string(
                buf.get_mut(cursor..).ok_or(PacketError::BufferTooSmall)?,
                pass,
            )?;
        }

        let remaining_len = cursor - content_start;
        let len_bytes = write_variable_byte_integer_len(
            buf.get_mut(remaining_len_pos..)
                .ok_or(PacketError::BufferTooSmall)?,
            remaining_len,
        )?;

        let header_len = 1 + len_bytes;
        buf.copy_within(content_start..cursor, header_len);
        Ok(header_len + remaining_len)
    }
}

impl<'a> DecodePacket<'a> for Connect<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, PacketError> {
        let mut cursor = 1;
        let remaining_len = read_variable_byte_integer(&mut cursor, buf)?;
        let content_end = cursor + remaining_len;
        if content_end > buf.len() {
            return Err(PacketError::IncompletePacket);
        }

        let protocol_name = read_utf8_string(&mut cursor, buf)?;
        let protocol_level = *buf.get(cursor).ok_or(PacketError::IncompletePacket)?;
        cursor += 1;

        if protocol_name != "MQTT" && protocol_name != "MQIsdp" {
            return Err(PacketError::MalformedPacket);
        }

        let flags = *buf.get(cursor).ok_or(PacketError::IncompletePacket)?;
        cursor += 1;
        let clean_session = (flags & 0x02) != 0;
        let will_flag = (flags & 0x04) != 0;
        let will_qos = QoS::from((flags >> 3) & 0x03);
        let will_retain = (flags & 0x20) != 0;
        let password_flag = (flags & 0x40) != 0;
        let username_flag = (flags & 0x80) != 0;

        if cursor + 2 > content_end {
            return Err(PacketError::IncompletePacket);
        }
        let keep_alive = u16::from_be_bytes([buf[cursor], buf[cursor + 1]]);
        cursor += 2;

        let properties =
            if (version == MqttVersion::V5 || protocol_level == 5) && cursor < content_end {
                read_properties(&mut cursor, buf)?
            } else {
                Vec::new()
            };

        let client_id = read_utf8_string(&mut cursor, buf)?;

        let will = if will_flag {
            let will_props =
                if (version == MqttVersion::V5 || protocol_level == 5) && cursor < content_end {
                    read_properties(&mut cursor, buf)?
                } else {
                    Vec::new()
                };
            let topic = read_utf8_string(&mut cursor, buf)?;
            let payload = read_binary_data(&mut cursor, buf)?;
            Some(Will {
                topic,
                payload,
                qos: will_qos,
                retain: will_retain,
                properties: will_props,
            })
        } else {
            None
        };

        let username = if username_flag {
            Some(read_utf8_string(&mut cursor, buf)?)
        } else {
            None
        };

        let password = if password_flag {
            Some(read_utf8_string(&mut cursor, buf)?)
        } else {
            None
        };

        Ok(Connect {
            client_id,
            keep_alive,
            clean_session,
            username,
            password,
            will,
            properties,
        })
    }
}

// --- CONNACK ---
impl<'a> DecodePacket<'a> for ConnAck<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, PacketError> {
        let mut cursor = 1;
        let remaining_len = read_variable_byte_integer(&mut cursor, buf)?;
        let content_end = cursor + remaining_len;
        if content_end > buf.len() || cursor + 2 > content_end {
            return Err(PacketError::IncompletePacket);
        }
        let session_present = (buf[cursor] & 0x01) != 0;
        cursor += 1;
        let reason_code = buf[cursor];
        cursor += 1;
        let properties = if version == MqttVersion::V5 && cursor < content_end {
            read_properties(&mut cursor, buf)?
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

impl<'a> EncodePacket for ConnAck<'a> {
    fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, PacketError> {
        *buf.get_mut(0).ok_or(PacketError::BufferTooSmall)? = 0x20;
        let mut cursor = 1;
        let remaining_len_pos = cursor;
        cursor += 4;
        let content_start = cursor;

        let flags = if self.session_present { 0x01 } else { 0x00 };
        *buf.get_mut(cursor).ok_or(PacketError::BufferTooSmall)? = flags;
        cursor += 1;
        *buf.get_mut(cursor).ok_or(PacketError::BufferTooSmall)? = self.reason_code;
        cursor += 1;

        if version == MqttVersion::V5 {
            write_properties(&mut cursor, buf, &self.properties)?;
        }

        let remaining_len = cursor - content_start;
        let len_bytes = write_variable_byte_integer_len(
            buf.get_mut(remaining_len_pos..)
                .ok_or(PacketError::BufferTooSmall)?,
            remaining_len,
        )?;
        let header_len = 1 + len_bytes;
        buf.copy_within(content_start..cursor, header_len);
        Ok(header_len + remaining_len)
    }
}

// --- PUBLISH ---
impl<'a> EncodePacket for Publish<'a> {
    fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, PacketError> {
        let mut flags = 0x30; // 0x30 = PUBLISH
        if self.dup {
            flags |= 0x08;
        }
        flags |= (self.qos as u8) << 1;
        if self.retain {
            flags |= 0x01;
        }

        let mut cursor = 0;
        *buf.get_mut(cursor).ok_or(PacketError::BufferTooSmall)? = flags;
        cursor += 1;

        let remaining_len_pos = cursor;
        cursor += 4;
        let content_start = cursor;

        cursor += write_utf8_string(
            buf.get_mut(cursor..).ok_or(PacketError::BufferTooSmall)?,
            self.topic,
        )?;

        if self.qos != QoS::AtMostOnce {
            let pid = self.packet_id.unwrap_or(1);
            buf.get_mut(cursor..cursor + 2)
                .ok_or(PacketError::BufferTooSmall)?
                .copy_from_slice(&pid.to_be_bytes());
            cursor += 2;
        }

        if version == MqttVersion::V5 {
            write_properties(&mut cursor, buf, &self.properties)?;
        }

        let end = cursor + self.payload.len();
        buf.get_mut(cursor..end)
            .ok_or(PacketError::BufferTooSmall)?
            .copy_from_slice(self.payload);
        cursor = end;

        let remaining_len = cursor - content_start;
        let len_bytes = write_variable_byte_integer_len(
            buf.get_mut(remaining_len_pos..)
                .ok_or(PacketError::BufferTooSmall)?,
            remaining_len,
        )?;
        let header_len = 1 + len_bytes;
        buf.copy_within(content_start..cursor, header_len);
        Ok(header_len + remaining_len)
    }
}

impl<'a> DecodePacket<'a> for Publish<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, PacketError> {
        if buf.is_empty() {
            return Err(PacketError::IncompletePacket);
        }
        let header = buf[0];
        let dup = (header & 0x08) != 0;
        let qos = QoS::from((header >> 1) & 0x03);
        let retain = (header & 0x01) != 0;

        let mut cursor = 1;
        let remaining_len = read_variable_byte_integer(&mut cursor, buf)?;
        let content_start = cursor;
        let content_end = content_start + remaining_len;

        if buf.len() < content_end {
            return Err(PacketError::IncompletePacket);
        }

        let topic = read_utf8_string(&mut cursor, buf)?;

        let packet_id = if qos != QoS::AtMostOnce {
            if cursor + 2 > content_end {
                return Err(PacketError::IncompletePacket);
            }
            let pid = u16::from_be_bytes([buf[cursor], buf[cursor + 1]]);
            cursor += 2;
            Some(pid)
        } else {
            None
        };

        let properties = if version == MqttVersion::V5 && cursor < content_end {
            read_properties(&mut cursor, buf)?
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

// --- Helper macro for PUBACK, PUBREC, PUBREL, PUBCOMP ---
macro_rules! impl_ack_packet {
    ($type:ident, $first_byte:expr) => {
        impl<'a> EncodePacket for $type<'a> {
            fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, PacketError> {
                *buf.get_mut(0).ok_or(PacketError::BufferTooSmall)? = $first_byte;
                let mut cursor = 1;
                let remaining_len_pos = cursor;
                cursor += 4;
                let content_start = cursor;

                buf.get_mut(cursor..cursor + 2)
                    .ok_or(PacketError::BufferTooSmall)?
                    .copy_from_slice(&self.packet_id.to_be_bytes());
                cursor += 2;

                if version == MqttVersion::V5
                    && (!self.properties.is_empty() || self.reason_code != 0)
                {
                    *buf.get_mut(cursor).ok_or(PacketError::BufferTooSmall)? = self.reason_code;
                    cursor += 1;
                    write_properties(&mut cursor, buf, &self.properties)?;
                }

                let remaining_len = cursor - content_start;
                let len_bytes = write_variable_byte_integer_len(
                    buf.get_mut(remaining_len_pos..)
                        .ok_or(PacketError::BufferTooSmall)?,
                    remaining_len,
                )?;
                let header_len = 1 + len_bytes;
                buf.copy_within(content_start..cursor, header_len);
                Ok(header_len + remaining_len)
            }
        }

        impl<'a> DecodePacket<'a> for $type<'a> {
            fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, PacketError> {
                let mut cursor = 1;
                let remaining_len = read_variable_byte_integer(&mut cursor, buf)?;
                let content_end = cursor + remaining_len;
                if content_end > buf.len() || cursor + 2 > content_end {
                    return Err(PacketError::IncompletePacket);
                }
                let packet_id = u16::from_be_bytes([buf[cursor], buf[cursor + 1]]);
                cursor += 2;

                let mut reason_code = 0;
                let mut properties = Vec::new();
                if version == MqttVersion::V5 && cursor < content_end {
                    reason_code = *buf.get(cursor).ok_or(PacketError::IncompletePacket)?;
                    cursor += 1;
                    if cursor < content_end {
                        properties = read_properties(&mut cursor, buf)?;
                    }
                }

                Ok($type {
                    packet_id,
                    reason_code,
                    properties,
                })
            }
        }
    };
}

impl_ack_packet!(PubAck, 0x40);
impl_ack_packet!(PubRec, 0x50);
impl_ack_packet!(PubRel, 0x62); // 0x62 has reserved bit set
impl_ack_packet!(PubComp, 0x70);

// --- SUBSCRIBE ---
impl<'a> EncodePacket for Subscribe<'a> {
    fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, PacketError> {
        *buf.get_mut(0).ok_or(PacketError::BufferTooSmall)? = 0x82; // 0x82 = SUBSCRIBE
        let mut cursor = 1;
        let remaining_len_pos = cursor;
        cursor += 4;
        let content_start = cursor;

        buf.get_mut(cursor..cursor + 2)
            .ok_or(PacketError::BufferTooSmall)?
            .copy_from_slice(&self.packet_id.to_be_bytes());
        cursor += 2;

        if version == MqttVersion::V5 {
            write_properties(&mut cursor, buf, &self.properties)?;
        }

        for (topic, qos) in &self.topics {
            cursor += write_utf8_string(
                buf.get_mut(cursor..).ok_or(PacketError::BufferTooSmall)?,
                topic,
            )?;
            *buf.get_mut(cursor).ok_or(PacketError::BufferTooSmall)? = *qos as u8;
            cursor += 1;
        }

        let remaining_len = cursor - content_start;
        let len_bytes = write_variable_byte_integer_len(
            buf.get_mut(remaining_len_pos..)
                .ok_or(PacketError::BufferTooSmall)?,
            remaining_len,
        )?;
        let header_len = 1 + len_bytes;
        buf.copy_within(content_start..cursor, header_len);
        Ok(header_len + remaining_len)
    }
}

impl<'a> DecodePacket<'a> for Subscribe<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, PacketError> {
        let mut cursor = 1;
        let remaining_len = read_variable_byte_integer(&mut cursor, buf)?;
        let content_end = cursor + remaining_len;
        if content_end > buf.len() || cursor + 2 > content_end {
            return Err(PacketError::IncompletePacket);
        }
        let packet_id = u16::from_be_bytes([buf[cursor], buf[cursor + 1]]);
        cursor += 2;

        let properties = if version == MqttVersion::V5 && cursor < content_end {
            read_properties(&mut cursor, buf)?
        } else {
            Vec::new()
        };

        let mut topics = Vec::new();
        while cursor < content_end {
            let topic = read_utf8_string(&mut cursor, buf)?;
            let qos_byte = *buf.get(cursor).ok_or(PacketError::IncompletePacket)?;
            let qos = QoS::from(qos_byte);
            cursor += 1;
            topics
                .push((topic, qos))
                .map_err(|_| PacketError::BatchCapacityExceeded)?;
        }

        Ok(Subscribe {
            packet_id,
            topics,
            properties,
        })
    }
}

// --- SUBACK ---
impl<'a> DecodePacket<'a> for SubAck<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, PacketError> {
        let mut cursor = 1;
        let remaining_len = read_variable_byte_integer(&mut cursor, buf)?;
        let content_end = cursor + remaining_len;
        if content_end > buf.len() || cursor + 2 > content_end {
            return Err(PacketError::IncompletePacket);
        }
        let packet_id = u16::from_be_bytes([buf[cursor], buf[cursor + 1]]);
        cursor += 2;

        let properties = if version == MqttVersion::V5 && cursor < content_end {
            read_properties(&mut cursor, buf)?
        } else {
            Vec::new()
        };

        let mut reason_codes = Vec::new();
        while cursor < content_end {
            let byte = *buf.get(cursor).ok_or(PacketError::IncompletePacket)?;
            reason_codes
                .push(byte)
                .map_err(|_| PacketError::BatchCapacityExceeded)?;
            cursor += 1;
        }

        Ok(SubAck {
            packet_id,
            reason_codes,
            properties,
        })
    }
}

impl<'a> EncodePacket for SubAck<'a> {
    fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, PacketError> {
        *buf.get_mut(0).ok_or(PacketError::BufferTooSmall)? = 0x90;
        let mut cursor = 1;
        let remaining_len_pos = cursor;
        cursor += 4;
        let content_start = cursor;

        buf.get_mut(cursor..cursor + 2)
            .ok_or(PacketError::BufferTooSmall)?
            .copy_from_slice(&self.packet_id.to_be_bytes());
        cursor += 2;

        if version == MqttVersion::V5 {
            write_properties(&mut cursor, buf, &self.properties)?;
        }

        for rc in &self.reason_codes {
            *buf.get_mut(cursor).ok_or(PacketError::BufferTooSmall)? = *rc;
            cursor += 1;
        }

        let remaining_len = cursor - content_start;
        let len_bytes = write_variable_byte_integer_len(
            buf.get_mut(remaining_len_pos..)
                .ok_or(PacketError::BufferTooSmall)?,
            remaining_len,
        )?;
        let header_len = 1 + len_bytes;
        buf.copy_within(content_start..cursor, header_len);
        Ok(header_len + remaining_len)
    }
}

// --- UNSUBSCRIBE ---
impl<'a> EncodePacket for Unsubscribe<'a> {
    fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, PacketError> {
        *buf.get_mut(0).ok_or(PacketError::BufferTooSmall)? = 0xA2;
        let mut cursor = 1;
        let remaining_len_pos = cursor;
        cursor += 4;
        let content_start = cursor;

        buf.get_mut(cursor..cursor + 2)
            .ok_or(PacketError::BufferTooSmall)?
            .copy_from_slice(&self.packet_id.to_be_bytes());
        cursor += 2;

        if version == MqttVersion::V5 {
            write_properties(&mut cursor, buf, &self.properties)?;
        }

        for topic in &self.topics {
            cursor += write_utf8_string(
                buf.get_mut(cursor..).ok_or(PacketError::BufferTooSmall)?,
                topic,
            )?;
        }

        let remaining_len = cursor - content_start;
        let len_bytes = write_variable_byte_integer_len(
            buf.get_mut(remaining_len_pos..)
                .ok_or(PacketError::BufferTooSmall)?,
            remaining_len,
        )?;
        let header_len = 1 + len_bytes;
        buf.copy_within(content_start..cursor, header_len);
        Ok(header_len + remaining_len)
    }
}

impl<'a> DecodePacket<'a> for Unsubscribe<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, PacketError> {
        let mut cursor = 1;
        let remaining_len = read_variable_byte_integer(&mut cursor, buf)?;
        let content_end = cursor + remaining_len;
        if content_end > buf.len() || cursor + 2 > content_end {
            return Err(PacketError::IncompletePacket);
        }
        let packet_id = u16::from_be_bytes([buf[cursor], buf[cursor + 1]]);
        cursor += 2;

        let properties = if version == MqttVersion::V5 && cursor < content_end {
            read_properties(&mut cursor, buf)?
        } else {
            Vec::new()
        };

        let mut topics = Vec::new();
        while cursor < content_end {
            let topic = read_utf8_string(&mut cursor, buf)?;
            topics
                .push(topic)
                .map_err(|_| PacketError::BatchCapacityExceeded)?;
        }

        Ok(Unsubscribe {
            packet_id,
            topics,
            properties,
        })
    }
}

// --- UNSUBACK ---
impl<'a> DecodePacket<'a> for UnsubAck<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, PacketError> {
        let mut cursor = 1;
        let remaining_len = read_variable_byte_integer(&mut cursor, buf)?;
        let content_end = cursor + remaining_len;
        if content_end > buf.len() || cursor + 2 > content_end {
            return Err(PacketError::IncompletePacket);
        }
        let packet_id = u16::from_be_bytes([buf[cursor], buf[cursor + 1]]);
        cursor += 2;

        let properties = if version == MqttVersion::V5 && cursor < content_end {
            read_properties(&mut cursor, buf)?
        } else {
            Vec::new()
        };

        let mut reason_codes = Vec::new();
        while cursor < content_end {
            let byte = *buf.get(cursor).ok_or(PacketError::IncompletePacket)?;
            reason_codes
                .push(byte)
                .map_err(|_| PacketError::BatchCapacityExceeded)?;
            cursor += 1;
        }

        Ok(UnsubAck {
            packet_id,
            reason_codes,
            properties,
        })
    }
}

impl<'a> EncodePacket for UnsubAck<'a> {
    fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, PacketError> {
        *buf.get_mut(0).ok_or(PacketError::BufferTooSmall)? = 0xB0;
        let mut cursor = 1;
        let remaining_len_pos = cursor;
        cursor += 4;
        let content_start = cursor;

        buf.get_mut(cursor..cursor + 2)
            .ok_or(PacketError::BufferTooSmall)?
            .copy_from_slice(&self.packet_id.to_be_bytes());
        cursor += 2;

        if version == MqttVersion::V5 {
            write_properties(&mut cursor, buf, &self.properties)?;
        }

        for rc in &self.reason_codes {
            *buf.get_mut(cursor).ok_or(PacketError::BufferTooSmall)? = *rc;
            cursor += 1;
        }

        let remaining_len = cursor - content_start;
        let len_bytes = write_variable_byte_integer_len(
            buf.get_mut(remaining_len_pos..)
                .ok_or(PacketError::BufferTooSmall)?,
            remaining_len,
        )?;
        let header_len = 1 + len_bytes;
        buf.copy_within(content_start..cursor, header_len);
        Ok(header_len + remaining_len)
    }
}

// --- PINGREQ ---
impl EncodePacket for PingReq {
    fn encode(&self, buf: &mut [u8], _version: MqttVersion) -> Result<usize, PacketError> {
        if buf.len() < 2 {
            return Err(PacketError::BufferTooSmall);
        }
        *buf.get_mut(0).ok_or(PacketError::BufferTooSmall)? = 0xC0;
        *buf.get_mut(1).ok_or(PacketError::BufferTooSmall)? = 0x00;
        Ok(2)
    }
}

// --- PINGRESP ---
impl EncodePacket for PingResp {
    fn encode(&self, buf: &mut [u8], _version: MqttVersion) -> Result<usize, PacketError> {
        if buf.len() < 2 {
            return Err(PacketError::BufferTooSmall);
        }
        *buf.get_mut(0).ok_or(PacketError::BufferTooSmall)? = 0xD0;
        *buf.get_mut(1).ok_or(PacketError::BufferTooSmall)? = 0x00;
        Ok(2)
    }
}

// --- DISCONNECT ---
impl<'a> DecodePacket<'a> for Disconnect<'a> {
    fn decode(buf: &'a [u8], version: MqttVersion) -> Result<Self, PacketError> {
        let mut cursor = 1;
        let mut reason_code = 0;
        let mut properties = Vec::new();

        if buf.len() > 1 {
            let remaining_len = read_variable_byte_integer(&mut cursor, buf)?;
            let content_end = cursor + remaining_len;
            if content_end > buf.len() {
                return Err(PacketError::IncompletePacket);
            }
            if version == MqttVersion::V5 && cursor < content_end {
                reason_code = *buf.get(cursor).ok_or(PacketError::IncompletePacket)?;
                cursor += 1;
                if cursor < content_end {
                    properties = read_properties(&mut cursor, buf)?;
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
    fn encode(&self, buf: &mut [u8], version: MqttVersion) -> Result<usize, PacketError> {
        *buf.get_mut(0).ok_or(PacketError::BufferTooSmall)? = 0xE0;
        let mut cursor = 1;
        let remaining_len_pos = cursor;
        cursor += 4;
        let content_start = cursor;

        if version == MqttVersion::V5 && (!self.properties.is_empty() || self.reason_code != 0) {
            *buf.get_mut(cursor).ok_or(PacketError::BufferTooSmall)? = self.reason_code;
            cursor += 1;
            write_properties(&mut cursor, buf, &self.properties)?;
        }

        let remaining_len = cursor - content_start;
        let len_bytes = write_variable_byte_integer_len(
            buf.get_mut(remaining_len_pos..)
                .ok_or(PacketError::BufferTooSmall)?,
            remaining_len,
        )?;
        let header_len = 1 + len_bytes;
        buf.copy_within(content_start..cursor, header_len);
        Ok(header_len + remaining_len)
    }
}

/// Decodes a raw byte buffer into a specific `MqttPacket`.
pub fn decode<'a>(
    buf: &'a [u8],
    version: MqttVersion,
) -> Result<Option<MqttPacket<'a>>, PacketError> {
    if buf.is_empty() {
        return Ok(None);
    }

    let packet_type = buf[0] >> 4;
    let packet = match packet_type {
        1 => MqttPacket::Connect(Connect::decode(buf, version)?),
        2 => MqttPacket::ConnAck(ConnAck::decode(buf, version)?),
        3 => MqttPacket::Publish(Publish::decode(buf, version)?),
        4 => MqttPacket::PubAck(PubAck::decode(buf, version)?),
        5 => MqttPacket::PubRec(PubRec::decode(buf, version)?),
        6 => MqttPacket::PubRel(PubRel::decode(buf, version)?),
        7 => MqttPacket::PubComp(PubComp::decode(buf, version)?),
        8 => MqttPacket::Subscribe(Subscribe::decode(buf, version)?),
        9 => MqttPacket::SubAck(SubAck::decode(buf, version)?),
        10 => MqttPacket::Unsubscribe(Unsubscribe::decode(buf, version)?),
        11 => MqttPacket::UnsubAck(UnsubAck::decode(buf, version)?),
        12 => MqttPacket::PingReq,
        13 => MqttPacket::PingResp,
        14 => MqttPacket::Disconnect(Disconnect::decode(buf, version)?),
        _ => return Err(PacketError::InvalidPacketType(packet_type)),
    };

    Ok(Some(packet))
}

/// Iterator over raw packet frames contained inside a stream slice.
/// Allows zero-copy multi-packet parsing from a single network read.
pub struct RawPacketFrameIter<'a> {
    buffer: &'a [u8],
    offset: usize,
}

impl<'a> RawPacketFrameIter<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        Self { buffer, offset: 0 }
    }
}

impl<'a> Iterator for RawPacketFrameIter<'a> {
    type Item = Result<&'a [u8], PacketError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.buffer.len() {
            return None;
        }

        let slice = &self.buffer[self.offset..];
        if slice.len() < 2 {
            return None; // Incomplete header
        }

        match peek_variable_byte_integer(&slice[1..]) {
            Ok((remaining_len, len_bytes)) => {
                let total_packet_len = 1 + len_bytes + remaining_len;
                if slice.len() < total_packet_len {
                    // Packet is partially received
                    return None;
                }
                let packet_slice = &slice[..total_packet_len];
                self.offset += total_packet_len;
                Some(Ok(packet_slice))
            }
            Err(e) => Some(Err(e)),
        }
    }
}
