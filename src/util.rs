//! # MQTT Serialization Utilities
//!
//! This module provides helper functions for reading and writing MQTT-specific data types
//! from and to byte buffers, such as variable-byte integers, length-prefixed strings,
//! v5 properties, and multi-packet stream framing.

use crate::error::{MqttError, ProtocolError};
use crate::packet;
use crate::transport;
use heapless::Vec;

/// Reads a variable-byte integer from the buffer, advancing the cursor.
pub fn read_variable_byte_integer(
    cursor: &mut usize,
    buf: &[u8],
) -> Result<usize, MqttError<transport::ErrorPlaceHolder>> {
    let mut multiplier = 1;
    let mut value = 0;
    let mut i = 0;
    loop {
        let encoded_byte = *buf
            .get(*cursor + i)
            .ok_or(MqttError::Protocol(ProtocolError::IncompletePacket))?;
        value += (encoded_byte & 127) as usize * multiplier;
        if (encoded_byte & 128) == 0 {
            break;
        }
        multiplier *= 128;
        i += 1;
        if i >= 4 {
            return Err(MqttError::Protocol(ProtocolError::MalformedPacket));
        }
    }
    *cursor += i + 1;
    Ok(value)
}

/// Peeks at a variable-byte integer without advancing the caller's cursor, returning `(value, bytes_consumed)`.
pub fn peek_variable_byte_integer(
    buf: &[u8],
) -> Result<(usize, usize), MqttError<transport::ErrorPlaceHolder>> {
    let mut multiplier = 1;
    let mut value = 0;
    let mut i = 0;
    loop {
        let encoded_byte = *buf
            .get(i)
            .ok_or(MqttError::Protocol(ProtocolError::IncompletePacket))?;
        value += (encoded_byte & 127) as usize * multiplier;
        if (encoded_byte & 128) == 0 {
            break;
        }
        multiplier *= 128;
        i += 1;
        if i >= 4 {
            return Err(MqttError::Protocol(ProtocolError::MalformedPacket));
        }
    }
    Ok((value, i + 1))
}

/// Writes a variable-byte integer to the buffer, advancing the cursor.
pub fn write_variable_byte_integer(
    cursor: &mut usize,
    buf: &mut [u8],
    mut val: usize,
) -> Result<(), MqttError<transport::ErrorPlaceHolder>> {
    loop {
        let mut encoded_byte = (val % 128) as u8;
        val /= 128;
        if val > 0 {
            encoded_byte |= 128;
        }
        *buf.get_mut(*cursor)
            .ok_or(MqttError::BufferTooSmall)? = encoded_byte;
        *cursor += 1;
        if val == 0 {
            break;
        }
    }
    Ok(())
}

/// Writes a variable-byte integer starting at index 0 of `buf` and returns the number of bytes written.
pub fn write_variable_byte_integer_len(
    buf: &mut [u8],
    mut val: usize,
) -> Result<usize, MqttError<transport::ErrorPlaceHolder>> {
    let mut i = 0;
    loop {
        let mut encoded_byte = (val % 128) as u8;
        val /= 128;
        if val > 0 {
            encoded_byte |= 128;
        }
        *buf.get_mut(i).ok_or(MqttError::BufferTooSmall)? = encoded_byte;
        i += 1;
        if val == 0 {
            break;
        }
    }
    Ok(i)
}

/// Reads a UTF-8 encoded string (prefixed with a 2-byte length) from the buffer.
pub fn read_utf8_string<'a>(
    cursor: &mut usize,
    buf: &'a [u8],
) -> Result<&'a str, MqttError<transport::ErrorPlaceHolder>> {
    if *cursor + 2 > buf.len() {
        return Err(MqttError::Protocol(ProtocolError::IncompletePacket));
    }
    let len = u16::from_be_bytes([buf[*cursor], buf[*cursor + 1]]) as usize;
    *cursor += 2;
    if *cursor + len > buf.len() {
        return Err(MqttError::Protocol(ProtocolError::IncompletePacket));
    }
    let s = core::str::from_utf8(&buf[*cursor..*cursor + len])
        .map_err(|_| MqttError::Protocol(ProtocolError::InvalidUtf8String))?;
    *cursor += len;
    Ok(s)
}

/// Writes a UTF-8 encoded string (prefixed with a 2-byte length) to the buffer.
pub fn write_utf8_string(
    buf: &mut [u8],
    s: &str,
) -> Result<usize, MqttError<transport::ErrorPlaceHolder>> {
    let len = s.len();
    if len > u16::MAX as usize {
        return Err(MqttError::Protocol(ProtocolError::PayloadTooLarge));
    }
    let len_bytes = (len as u16).to_be_bytes();

    let required_space = 2 + len;
    let slice = buf
        .get_mut(0..required_space)
        .ok_or(MqttError::BufferTooSmall)?;

    slice[0..2].copy_from_slice(&len_bytes);
    slice[2..].copy_from_slice(s.as_bytes());
    Ok(required_space)
}

/// Reads a byte slice (prefixed with a 2-byte length) from the buffer.
pub fn read_binary_data<'a>(
    cursor: &mut usize,
    buf: &'a [u8],
) -> Result<&'a [u8], MqttError<transport::ErrorPlaceHolder>> {
    if *cursor + 2 > buf.len() {
        return Err(MqttError::Protocol(ProtocolError::IncompletePacket));
    }
    let len = u16::from_be_bytes([buf[*cursor], buf[*cursor + 1]]) as usize;
    *cursor += 2;
    if *cursor + len > buf.len() {
        return Err(MqttError::Protocol(ProtocolError::IncompletePacket));
    }
    let data = &buf[*cursor..*cursor + len];
    *cursor += len;
    Ok(data)
}

/// Writes a byte slice (prefixed with a 2-byte length) to the buffer.
pub fn write_binary_data(
    buf: &mut [u8],
    data: &[u8],
) -> Result<usize, MqttError<transport::ErrorPlaceHolder>> {
    let len = data.len();
    if len > u16::MAX as usize {
        return Err(MqttError::Protocol(ProtocolError::PayloadTooLarge));
    }
    let len_bytes = (len as u16).to_be_bytes();
    let required_space = 2 + len;
    let slice = buf
        .get_mut(0..required_space)
        .ok_or(MqttError::BufferTooSmall)?;
    slice[0..2].copy_from_slice(&len_bytes);
    slice[2..].copy_from_slice(data);
    Ok(required_space)
}

/// Reads MQTT v5 properties from the buffer.
pub fn read_properties<'a>(
    cursor: &mut usize,
    buf: &'a [u8],
) -> Result<Vec<packet::Property<'a>, 8>, MqttError<transport::ErrorPlaceHolder>> {
    let mut properties = Vec::new();
    let prop_len = read_variable_byte_integer(cursor, buf)?;
    let prop_end = *cursor + prop_len;

    if prop_end > buf.len() {
        return Err(MqttError::Protocol(ProtocolError::IncompletePacket));
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
                    *buf.get(data_start).ok_or(MqttError::Protocol(ProtocolError::MalformedPacket))?,
                    *buf.get(data_start + 1).ok_or(MqttError::Protocol(ProtocolError::MalformedPacket))?,
                ]) as usize;
                2 + len
            }
            // User Property (pair of UTF-8 strings)
            0x26 => {
                let key_len = u16::from_be_bytes([buf[data_start], buf[data_start + 1]]) as usize;
                let val_start = data_start + 2 + key_len;
                let val_len = u16::from_be_bytes([buf[val_start], buf[val_start + 1]]) as usize;
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
            return Err(MqttError::Protocol(ProtocolError::MalformedPacket));
        }
        *cursor += data_len;
        properties
            .push(packet::Property {
                id,
                data: &buf[data_start..data_start + data_len],
            })
            .map_err(|_| MqttError::Protocol(ProtocolError::TooManyProperties))?;
    }
    Ok(properties)
}

/// Writes MQTT v5 properties to the buffer.
pub fn write_properties(
    cursor: &mut usize,
    buf: &mut [u8],
    properties: &[packet::Property],
) -> Result<(), MqttError<transport::ErrorPlaceHolder>> {
    let mut total_prop_len = 0;
    for prop in properties {
        total_prop_len += 1 + prop.data.len();
    }

    write_variable_byte_integer(cursor, buf, total_prop_len)?;
    for prop in properties {
        *buf.get_mut(*cursor).ok_or(MqttError::BufferTooSmall)? = prop.id;
        *cursor += 1;
        let end = *cursor + prop.data.len();
        buf.get_mut(*cursor..end)
            .ok_or(MqttError::BufferTooSmall)?
            .copy_from_slice(prop.data);
        *cursor += prop.data.len();
    }
    Ok(())
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
    type Item = Result<&'a [u8], MqttError<transport::ErrorPlaceHolder>>;

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
