//! # Variable Byte Integer Codec
//!
//! Implements MQTT variable byte integer decoding and encoding (1 to 4 bytes, up to 268,435,455).

use crate::error::PacketError;

/// Reads a variable-byte integer from the buffer, advancing the cursor.
pub fn read_variable_byte_integer(cursor: &mut usize, buf: &[u8]) -> Result<usize, PacketError> {
    let mut multiplier = 1;
    let mut value = 0;
    let mut i = 0;
    loop {
        let encoded_byte = *buf.get(*cursor + i).ok_or(PacketError::IncompletePacket)?;
        value += (encoded_byte & 127) as usize * multiplier;
        if (encoded_byte & 128) == 0 {
            break;
        }
        multiplier *= 128;
        i += 1;
        if i >= 4 {
            return Err(PacketError::MalformedPacket);
        }
    }
    *cursor += i + 1;
    Ok(value)
}

/// Peeks at a variable-byte integer without advancing the caller's cursor, returning `(value, bytes_consumed)`.
pub fn peek_variable_byte_integer(buf: &[u8]) -> Result<(usize, usize), PacketError> {
    let mut multiplier = 1;
    let mut value = 0;
    let mut i = 0;
    loop {
        let encoded_byte = *buf.get(i).ok_or(PacketError::IncompletePacket)?;
        value += (encoded_byte & 127) as usize * multiplier;
        if (encoded_byte & 128) == 0 {
            break;
        }
        multiplier *= 128;
        i += 1;
        if i >= 4 {
            return Err(PacketError::MalformedPacket);
        }
    }
    Ok((value, i + 1))
}

/// Writes a variable-byte integer to the buffer, advancing the cursor.
pub fn write_variable_byte_integer(
    cursor: &mut usize,
    buf: &mut [u8],
    mut val: usize,
) -> Result<(), PacketError> {
    loop {
        let mut encoded_byte = (val % 128) as u8;
        val /= 128;
        if val > 0 {
            encoded_byte |= 128;
        }
        *buf.get_mut(*cursor).ok_or(PacketError::BufferTooSmall)? = encoded_byte;
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
) -> Result<usize, PacketError> {
    let mut i = 0;
    loop {
        let mut encoded_byte = (val % 128) as u8;
        val /= 128;
        if val > 0 {
            encoded_byte |= 128;
        }
        *buf.get_mut(i).ok_or(PacketError::BufferTooSmall)? = encoded_byte;
        i += 1;
        if val == 0 {
            break;
        }
    }
    Ok(i)
}

/// Reads a UTF-8 encoded string (prefixed with a 2-byte big-endian length) from the buffer.
pub fn read_utf8_string<'a>(cursor: &mut usize, buf: &'a [u8]) -> Result<&'a str, PacketError> {
    if *cursor + 2 > buf.len() {
        return Err(PacketError::IncompletePacket);
    }
    let len = u16::from_be_bytes([buf[*cursor], buf[*cursor + 1]]) as usize;
    *cursor += 2;
    if *cursor + len > buf.len() {
        return Err(PacketError::IncompletePacket);
    }
    let s = core::str::from_utf8(&buf[*cursor..*cursor + len])
        .map_err(|_| PacketError::InvalidUtf8String)?;
    *cursor += len;
    Ok(s)
}

/// Writes a UTF-8 encoded string (prefixed with a 2-byte big-endian length) to the buffer.
pub fn write_utf8_string(buf: &mut [u8], s: &str) -> Result<usize, PacketError> {
    let len = s.len();
    if len > u16::MAX as usize {
        return Err(PacketError::PayloadTooLarge);
    }
    let len_bytes = (len as u16).to_be_bytes();
    let required_space = 2 + len;
    let slice = buf
        .get_mut(0..required_space)
        .ok_or(PacketError::BufferTooSmall)?;

    slice[0..2].copy_from_slice(&len_bytes);
    slice[2..].copy_from_slice(s.as_bytes());
    Ok(required_space)
}

/// Reads binary data (prefixed with a 2-byte big-endian length) from the buffer.
pub fn read_binary_data<'a>(cursor: &mut usize, buf: &'a [u8]) -> Result<&'a [u8], PacketError> {
    if *cursor + 2 > buf.len() {
        return Err(PacketError::IncompletePacket);
    }
    let len = u16::from_be_bytes([buf[*cursor], buf[*cursor + 1]]) as usize;
    *cursor += 2;
    if *cursor + len > buf.len() {
        return Err(PacketError::IncompletePacket);
    }
    let data = &buf[*cursor..*cursor + len];
    *cursor += len;
    Ok(data)
}

/// Writes binary data (prefixed with a 2-byte big-endian length) to the buffer.
pub fn write_binary_data(buf: &mut [u8], data: &[u8]) -> Result<usize, PacketError> {
    let len = data.len();
    if len > u16::MAX as usize {
        return Err(PacketError::PayloadTooLarge);
    }
    let len_bytes = (len as u16).to_be_bytes();
    let required_space = 2 + len;
    let slice = buf
        .get_mut(0..required_space)
        .ok_or(PacketError::BufferTooSmall)?;
    slice[0..2].copy_from_slice(&len_bytes);
    slice[2..].copy_from_slice(data);
    Ok(required_space)
}
