//! # Error Types
//!
//! This module defines the error types used throughout the MQTT client library,
//! providing detailed information about potential failures, from transport issues
//! to protocol violations and QUIC stream errors.

use crate::transport;

/// A placeholder error type used in generic contexts where the specific transport
/// error is not yet known.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ErrorPlaceHolder;

impl transport::TransportError for ErrorPlaceHolder {}

/// The primary error enum for the MQTT client.
///
/// It is generic over the transport error type `T`, allowing it to wrap
/// specific errors from the underlying network transport (e.g., TCP, UART, QUIC).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MqttError<T> {
    /// An error occurred in the underlying transport layer.
    Transport(T),
    /// A protocol-level error occurred, indicating a violation of the MQTT specification.
    Protocol(ProtocolError),
    /// The connection was refused by the broker. The enclosed code provides the reason.
    ConnectionRefused(ConnectReasonCode),
    /// The client is not currently connected to the broker.
    NotConnected,
    /// The buffer provided for an operation was too small.
    BufferTooSmall,
    /// An operation timed out.
    Timeout,
    /// Multi-packet burst buffer or queue is full.
    BatchCapacityExceeded,
    /// An error occurred on a QUIC stream or datagram channel.
    QuicError(QuicErrorKind),
}

/// Specific QUIC / H3 transport errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum QuicErrorKind {
    StreamClosed,
    StreamReset(u32),
    ConnectionLost,
    DatagramTooLarge,
    UnsupportedOperation,
}

impl<T: transport::TransportError> From<T> for MqttError<T> {
    fn from(err: T) -> Self {
        MqttError::Transport(err)
    }
}

impl<T: transport::TransportError> MqttError<T> {
    /// Helper method to convert an `MqttError` with a placeholder transport error
    /// into an `MqttError` with a specific transport error type `T`.
    pub fn cast_transport_error<E: transport::TransportError>(
        other: MqttError<E>,
    ) -> MqttError<T> {
        match other {
            MqttError::Protocol(p) => MqttError::Protocol(p),
            MqttError::ConnectionRefused(c) => MqttError::ConnectionRefused(c),
            MqttError::NotConnected => MqttError::NotConnected,
            MqttError::BufferTooSmall => MqttError::BufferTooSmall,
            MqttError::Timeout => MqttError::Timeout,
            MqttError::BatchCapacityExceeded => MqttError::BatchCapacityExceeded,
            MqttError::QuicError(q) => MqttError::QuicError(q),
            MqttError::Transport(_) => MqttError::Protocol(ProtocolError::MalformedPacket),
        }
    }
}

/// Represents the reason codes for a connection refusal (`CONNACK`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u8)]
pub enum ConnectReasonCode {
    /// The connection was accepted.
    Success = 0,
    /// The broker does not support the requested MQTT protocol version.
    UnacceptableProtocolVersion = 1,
    /// The client identifier is not valid.
    IdentifierRejected = 2,
    /// The broker is unavailable.
    ServerUnavailable = 3,
    /// The username or password is not valid.
    BadUserNameOrPassword = 4,
    /// The client is not authorized to connect.
    NotAuthorized = 5,
    /// An unknown or unspecified error occurred.
    Other(u8),
}

impl From<u8> for ConnectReasonCode {
    fn from(val: u8) -> Self {
        match val {
            0 => Self::Success,
            1 => Self::UnacceptableProtocolVersion,
            2 => Self::IdentifierRejected,
            3 => Self::ServerUnavailable,
            4 => Self::BadUserNameOrPassword,
            5 => Self::NotAuthorized,
            _ => Self::Other(val),
        }
    }
}

/// Enumerates specific MQTT protocol errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ProtocolError {
    /// An invalid packet type was received.
    InvalidPacketType(u8),
    /// The server sent an invalid or unexpected response.
    InvalidResponse,
    /// A packet was received that was not correctly formed.
    MalformedPacket,
    /// Packet is incomplete in the current buffer slice (needs more stream bytes).
    IncompletePacket,
    /// The payload of a message exceeds the maximum allowable size.
    PayloadTooLarge,
    /// A string was not valid UTF-8.
    InvalidUtf8String,
    /// An MQTT v5 packet contained too many properties.
    TooManyProperties,
    /// Topic filter is invalid or exceeds buffer limits.
    InvalidTopic,
}


