//! # Error types for embedded MQTT operations

use crate::transport::TransportError;
use mqtt_packet::PacketError;

/// Placeholder transport error for generic contexts.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ErrorPlaceHolder;

impl TransportError for ErrorPlaceHolder {}

/// Primary error type for embedded MQTT client operations.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MqttError<T> {
    /// Transport-level I/O failure.
    Transport(T),
    /// Protocol violation or malformed packet.
    Protocol(ProtocolError),
    /// Broker rejected the connection handshake.
    ConnectionRefused(ConnectReasonCode),
    /// Client is not connected to a broker.
    NotConnected,
    /// Provided buffer is insufficient for packet serialization or reception.
    BufferTooSmall,
    /// Network or keep-alive operation timed out.
    Timeout,
    /// Multi-packet burst or inflight queue exceeded configured capacity.
    BatchCapacityExceeded,
    /// Inflight message queue is full (QoS 1 / QoS 2).
    InflightQueueFull,
    /// QUIC transport error.
    QuicError(QuicErrorKind),
}

/// QUIC-specific error kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum QuicErrorKind {
    StreamClosed,
    StreamReset(u32),
    ConnectionLost,
    DatagramTooLarge,
    UnsupportedOperation,
}

impl<T: TransportError> From<T> for MqttError<T> {
    fn from(err: T) -> Self {
        MqttError::Transport(err)
    }
}

impl<T: TransportError> From<PacketError> for MqttError<T> {
    fn from(err: PacketError) -> Self {
        match err {
            PacketError::BufferTooSmall => MqttError::BufferTooSmall,
            PacketError::IncompletePacket => MqttError::Protocol(ProtocolError::IncompletePacket),
            PacketError::MalformedPacket => MqttError::Protocol(ProtocolError::MalformedPacket),
            PacketError::InvalidPacketType(t) => MqttError::Protocol(ProtocolError::InvalidPacketType(t)),
            PacketError::InvalidUtf8String => MqttError::Protocol(ProtocolError::InvalidUtf8String),
            PacketError::TooManyProperties => MqttError::Protocol(ProtocolError::TooManyProperties),
            PacketError::BatchCapacityExceeded => MqttError::BatchCapacityExceeded,
            PacketError::PayloadTooLarge => MqttError::Protocol(ProtocolError::PayloadTooLarge),
            PacketError::UnsupportedQoS => MqttError::Protocol(ProtocolError::UnsupportedQoS),
        }
    }
}

impl<T: TransportError> MqttError<T> {
    pub fn cast_transport_error<E: TransportError>(other: MqttError<E>) -> MqttError<T> {
        match other {
            MqttError::Protocol(p) => MqttError::Protocol(p),
            MqttError::ConnectionRefused(c) => MqttError::ConnectionRefused(c),
            MqttError::NotConnected => MqttError::NotConnected,
            MqttError::BufferTooSmall => MqttError::BufferTooSmall,
            MqttError::Timeout => MqttError::Timeout,
            MqttError::BatchCapacityExceeded => MqttError::BatchCapacityExceeded,
            MqttError::InflightQueueFull => MqttError::InflightQueueFull,
            MqttError::QuicError(q) => MqttError::QuicError(q),
            MqttError::Transport(_) => MqttError::Protocol(ProtocolError::MalformedPacket),
        }
    }
}

/// Reasons for broker connection rejection in `CONNACK`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u8)]
pub enum ConnectReasonCode {
    Success = 0,
    UnacceptableProtocolVersion = 1,
    IdentifierRejected = 2,
    ServerUnavailable = 3,
    BadUserNameOrPassword = 4,
    NotAuthorized = 5,
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

/// MQTT protocol violation errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum ProtocolError {
    InvalidPacketType(u8),
    InvalidResponse,
    MalformedPacket,
    IncompletePacket,
    PayloadTooLarge,
    InvalidUtf8String,
    TooManyProperties,
    InvalidTopic,
    UnsupportedQoS,
}
