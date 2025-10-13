//! Utility functions for the MQTT client.

use crate::error::{MqttError, ProtocolError};

pub fn is_valid_publish_topic(topic: &str) -> bool {
    !topic.contains('+') && !topic.contains('#')
}

pub fn write_utf8_string<T>(buf: &mut [u8], s: &str) -> Result<usize, MqttError<T>> {
    let len = s.len();
    if buf.len() < 2 + len {
        return Err(MqttError::BufferTooSmall);
    }
    buf[0..2].copy_from_slice(&(len as u16).to_be_bytes());
    buf[2..2 + len].copy_from_slice(s.as_bytes());
    Ok(2 + len)
}

pub fn read_utf8_string<'a, T>(buf: &'a [u8]) -> Result<(&'a str, usize), MqttError<T>> {
    if buf.len() < 2 {
        return Err(MqttError::BufferTooSmall);
    }
    let len = u16::from_be_bytes([buf[0], buf[1]]) as usize;
    if buf.len() < 2 + len {
        return Err(MqttError::BufferTooSmall);
    }
    let s = core::str::from_utf8(&buf[2..2 + len])
        .map_err(|_| MqttError::Protocol(ProtocolError::InvalidUtf8))?;
    Ok((s, 2 + len))
}

pub fn encode_variable_byte_integer<T>(
    buf: &mut [u8],
    mut val: u32,
) -> Result<usize, MqttError<T>> {
    let mut i = 0;
    loop {
        if i >= 4 {
            return Err(MqttError::BufferTooSmall); // Should not happen with u32
        }
        let mut encoded_byte = (val % 128) as u8;
        val /= 128;
        if val > 0 {
            encoded_byte |= 128;
        }
        buf[i] = encoded_byte;
        i += 1;
        if val == 0 {
            break;
        }
    }
    Ok(i)
}

pub fn decode_variable_byte_integer<T>(buf: &[u8]) -> Result<(u32, usize), MqttError<T>> {
    let mut multiplier = 1;
    let mut value = 0;
    let mut i = 0;
    loop {
        if i >= buf.len() {
            return Err(MqttError::BufferTooSmall);
        }
        let encoded_byte = buf[i];
        value += (encoded_byte & 127) as u32 * multiplier;
        if multiplier > 128 * 128 * 128 {
            return Err(MqttError::Protocol(
                ProtocolError::InvalidVariableByteInteger,
            ));
        }
        multiplier *= 128;
        i += 1;
        if (encoded_byte & 128) == 0 {
            break;
        }
    }
    Ok((value, i))
}

