//! # MQTT v5 Properties Codec

use crate::error::PacketError;
use crate::varint::{read_variable_byte_integer, write_variable_byte_integer};
use heapless::Vec;

/// Represents an MQTT v5 user or system property.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Property<'a> {
    pub id: u8,
    pub data: &'a [u8],
}

/// Reads MQTT v5 properties from the buffer.
pub fn read_properties<'a>(
    cursor: &mut usize,
    buf: &'a [u8],
) -> Result<Vec<Property<'a>, 8>, PacketError> {
    let mut properties = Vec::new();
    let prop_len = read_variable_byte_integer(cursor, buf)?;
    let prop_end = *cursor + prop_len;

    if prop_end > buf.len() {
        return Err(PacketError::IncompletePacket);
    }

    while *cursor < prop_end {
        let id = buf[*cursor];
        *cursor += 1;
        let data_start = *cursor;
        let data_len = match id {
            // Byte properties
            0x01 | 0x17 | 0x24 | 0x25 | 0x28 | 0x29 | 0x2A => 1,
            // 2-byte integer properties
            0x13 | 0x21 | 0x22 => 2,
            // 4-byte integer properties
            0x02 | 0x11 | 0x18 | 0x23 | 0x27 => 4,
            // UTF-8 string or binary data
            0x03 | 0x08 | 0x12 | 0x15 | 0x1A | 0x1C | 0x1F | 0x09 | 0x16 => {
                let len = u16::from_be_bytes([
                    *buf.get(data_start).ok_or(PacketError::MalformedPacket)?,
                    *buf.get(data_start + 1)
                        .ok_or(PacketError::MalformedPacket)?,
                ]) as usize;
                2 + len
            }
            // User Property (pair of UTF-8 strings)
            0x26 => {
                let key_len = u16::from_be_bytes([
                    *buf.get(data_start).ok_or(PacketError::MalformedPacket)?,
                    *buf.get(data_start + 1)
                        .ok_or(PacketError::MalformedPacket)?,
                ]) as usize;
                let val_start = data_start + 2 + key_len;
                let val_len = u16::from_be_bytes([
                    *buf.get(val_start).ok_or(PacketError::MalformedPacket)?,
                    *buf.get(val_start + 1).ok_or(PacketError::MalformedPacket)?,
                ]) as usize;
                2 + key_len + 2 + val_len
            }
            // Variable Byte Integer properties
            0x0B => {
                let mut temp_cursor = data_start;
                let _ = read_variable_byte_integer(&mut temp_cursor, buf)?;
                temp_cursor - data_start
            }
            _ => 1,
        };

        if data_start + data_len > prop_end {
            return Err(PacketError::MalformedPacket);
        }
        *cursor += data_len;
        properties
            .push(Property {
                id,
                data: &buf[data_start..data_start + data_len],
            })
            .map_err(|_| PacketError::TooManyProperties)?;
    }
    Ok(properties)
}

/// Writes MQTT v5 properties to the buffer.
pub fn write_properties(
    cursor: &mut usize,
    buf: &mut [u8],
    properties: &[Property],
) -> Result<(), PacketError> {
    let mut total_prop_len = 0;
    for prop in properties {
        total_prop_len += 1 + prop.data.len();
    }

    write_variable_byte_integer(cursor, buf, total_prop_len)?;
    for prop in properties {
        *buf.get_mut(*cursor).ok_or(PacketError::BufferTooSmall)? = prop.id;
        *cursor += 1;
        let end = *cursor + prop.data.len();
        buf.get_mut(*cursor..end)
            .ok_or(PacketError::BufferTooSmall)?
            .copy_from_slice(prop.data);
        *cursor += prop.data.len();
    }
    Ok(())
}
