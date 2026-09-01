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
            PacketError::InvalidPacketType(t) => {
                MqttError::Protocol(ProtocolError::InvalidPacketType(t))
            }
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

use core::fmt;

impl<T: fmt::Display> fmt::Display for MqttError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(t) => write!(f, "Transport I/O error: {t}"),
            Self::Protocol(p) => write!(f, "Protocol violation: {p}"),
            Self::ConnectionRefused(c) => write!(f, "Connection refused: {c}"),
            Self::NotConnected => write!(f, "Client is not connected"),
            Self::BufferTooSmall => write!(f, "Buffer is too small for operation"),
            Self::Timeout => write!(f, "Network/keep-alive operation timed out"),
            Self::BatchCapacityExceeded => write!(f, "Batch message capacity exceeded"),
            Self::InflightQueueFull => write!(f, "Inflight queue is full"),
            Self::QuicError(q) => write!(f, "QUIC error: {q}"),
        }
    }
}

#[cfg(feature = "std")]
impl<T: std::error::Error + 'static> std::error::Error for MqttError<T> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(t) => Some(t),
            _ => None,
        }
    }
}

impl fmt::Display for ConnectReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Success => write!(f, "Success"),
            Self::UnacceptableProtocolVersion => write!(f, "Unacceptable Protocol Version"),
            Self::IdentifierRejected => write!(f, "Identifier Rejected"),
            Self::ServerUnavailable => write!(f, "Server Unavailable"),
            Self::BadUserNameOrPassword => write!(f, "Bad Username or Password"),
            Self::NotAuthorized => write!(f, "Not Authorized"),
            Self::Other(code) => write!(f, "Other reason code ({code})"),
        }
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPacketType(t) => write!(f, "Invalid packet type byte: 0x{t:02X}"),
            Self::InvalidResponse => write!(f, "Invalid broker response packet"),
            Self::MalformedPacket => write!(f, "Malformed or truncated packet"),
            Self::IncompletePacket => write!(f, "Incomplete packet frame received"),
            Self::PayloadTooLarge => write!(f, "Payload exceeds maximum allowed length"),
            Self::InvalidUtf8String => write!(f, "Invalid UTF-8 string in packet"),
            Self::TooManyProperties => write!(f, "Number of MQTT 5.0 properties exceeded capacity"),
            Self::InvalidTopic => write!(f, "Topic contains invalid characters or wildcards"),
            Self::UnsupportedQoS => write!(f, "Requested QoS level is not supported"),
        }
    }
}

impl fmt::Display for QuicErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StreamClosed => write!(f, "QUIC stream closed"),
            Self::StreamReset(code) => write!(f, "QUIC stream reset (code {code})"),
            Self::ConnectionLost => write!(f, "QUIC connection lost"),
            Self::DatagramTooLarge => write!(f, "QUIC datagram exceeds MTU size"),
            Self::UnsupportedOperation => write!(f, "QUIC operation not supported"),
        }
    }
}
