//! Unified error types for the MQTT client.

use crate::transport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u8)]
pub enum ConnectReasonCode {
    Success = 0,
    UnspecifiedError = 128,
    MalformedPacket = 129,
    ProtocolError = 130,
    ImplementationSpecificError = 131,
    UnsupportedProtocolVersion = 132,
    ClientIdentifierNotValid = 133,
    BadUserNameOrPassword = 134,
    NotAuthorized = 135,
    ServerUnavailable = 136,
    ServerBusy = 137,
    Banned = 138,
    // ... other V5 codes
    V3(u8),
}

impl From<u8> for ConnectReasonCode {
    fn from(val: u8) -> Self {
        match val {
            0 => Self::Success,
            128 => Self::UnspecifiedError,
            129 => Self::MalformedPacket,
            130 => Self::ProtocolError,
            131 => Self::ImplementationSpecificError,
            132 => Self::UnsupportedProtocolVersion,
            133 => Self::ClientIdentifierNotValid,
            134 => Self::BadUserNameOrPassword,
            135 => Self::NotAuthorized,
            136 => Self::ServerUnavailable,
            137 => Self::ServerBusy,
            138 => Self::Banned,
            _ => Self::V3(val),
        }
    }
}

/// The main error type for the MQTT client.
#[derive(Debug)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MqttError<T> {
    /// An error occurred in the underlying transport.
    Transport(T),
    /// A protocol-level error occurred.
    Protocol(ProtocolError),
    /// The connection was refused by the broker.
    ConnectionRefused(ConnectReasonCode),
    /// The client is not currently connected.
    NotConnected,
    /// The provided buffer was too small.
    BufferTooSmall,
    /// The operation would block, but the transport is non-blocking.
    WouldBlock,
    /// The operation timed out.
    Timeout,
}

/// Protocol-level errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ProtocolError {
    /// An invalid packet type was received.
    InvalidPacketType(u8),
    /// The server sent an invalid response.
    InvalidResponse,
    /// A packet was received with a packet identifier that does not match any in-flight packets.
    UnmatchedPacketId,
    /// An invalid UTF-8 string was encountered.
    InvalidUtf8,
    /// An invalid variable byte integer was encountered.
    InvalidVariableByteInteger,
    #[cfg(feature = "v5")]
    InvalidPropertyIdentifier(u8),
}

impl<T: transport::TransportError> From<T> for MqttError<T> {
    fn from(e: T) -> Self {
        MqttError::Transport(e)
    }
}

