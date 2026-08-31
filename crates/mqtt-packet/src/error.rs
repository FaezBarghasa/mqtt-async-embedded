//! # Error types for MQTT packet encoding and decoding

/// Error encountered during MQTT packet serialization or deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum PacketError {
    /// Provided buffer is too small to encode or decode the packet.
    BufferTooSmall,
    /// Packet is incomplete in the current buffer slice.
    IncompletePacket,
    /// Packet structure is malformed or invalid according to MQTT specification.
    MalformedPacket,
    /// Invalid MQTT control packet type identifier.
    InvalidPacketType(u8),
    /// Invalid UTF-8 string encoding.
    InvalidUtf8String,
    /// Property collection exceeded capacity limits.
    TooManyProperties,
    /// Array or list capacity exceeded.
    BatchCapacityExceeded,
    /// Payload exceeds maximum protocol limits.
    PayloadTooLarge,
    /// Requested QoS level is not supported.
    UnsupportedQoS,
}

impl core::fmt::Display for PacketError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::BufferTooSmall => write!(f, "Buffer is too small"),
            Self::IncompletePacket => write!(f, "Packet is incomplete"),
            Self::MalformedPacket => write!(f, "Packet structure is malformed"),
            Self::InvalidPacketType(t) => write!(f, "Invalid MQTT packet type: {t}"),
            Self::InvalidUtf8String => write!(f, "Invalid UTF-8 string"),
            Self::TooManyProperties => write!(f, "Too many MQTT v5 properties"),
            Self::BatchCapacityExceeded => write!(f, "Batch or array capacity exceeded"),
            Self::PayloadTooLarge => write!(f, "Payload exceeds maximum allowed length"),
            Self::UnsupportedQoS => write!(f, "Unsupported QoS level"),
        }
    }
}

impl core::error::Error for PacketError {}
