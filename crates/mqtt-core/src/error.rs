//! # Error Types for the Universal MQTT Engine
//!
//! Provides a unified, hierarchical error model that maps cleanly across
//! embedded (`no_std`) and host (`std`) environments.

use crate::traits::TransportError;
use core::fmt;
use mqtt_packet::PacketError;

/// Reasons for broker connection rejection in `CONNACK` or disconnect reasons in MQTT 5.0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u8)]
pub enum ReasonCode {
    Success = 0,
    UnacceptableProtocolVersion = 1,
    IdentifierRejected = 2,
    ServerUnavailable = 3,
    BadUserNameOrPassword = 4,
    NotAuthorized = 5,
    ServerBusy = 137,
    BadAuthenticationMethod = 140,
    KeepAliveTimeout = 141,
    SessionTakenOver = 142,
    TopicFilterInvalid = 143,
    TopicNameInvalid = 144,
    ReceiveMaximumExceeded = 147,
    TopicAliasInvalid = 148,
    PacketTooLarge = 149,
    MessageRateTooHigh = 150,
    QuotaExceeded = 151,
    AdministrativeAction = 152,
    PayloadFormatInvalid = 153,
    RetainNotSupported = 154,
    QoSNotSupported = 155,
    UseAnotherServer = 156,
    ServerMoved = 157,
    SharedSubscriptionsNotSupported = 158,
    ConnectionRateExceeded = 159,
    MaximumConnectTime = 160,
    SubscriptionIdentifiersNotSupported = 161,
    WildcardSubscriptionsNotSupported = 162,
    Other(u8),
}

impl From<u8> for ReasonCode {
    fn from(val: u8) -> Self {
        match val {
            0 => Self::Success,
            1 => Self::UnacceptableProtocolVersion,
            2 => Self::IdentifierRejected,
            3 => Self::ServerUnavailable,
            4 => Self::BadUserNameOrPassword,
            5 => Self::NotAuthorized,
            137 => Self::ServerBusy,
            140 => Self::BadAuthenticationMethod,
            141 => Self::KeepAliveTimeout,
            142 => Self::SessionTakenOver,
            143 => Self::TopicFilterInvalid,
            144 => Self::TopicNameInvalid,
            147 => Self::ReceiveMaximumExceeded,
            148 => Self::TopicAliasInvalid,
            149 => Self::PacketTooLarge,
            150 => Self::MessageRateTooHigh,
            151 => Self::QuotaExceeded,
            152 => Self::AdministrativeAction,
            153 => Self::PayloadFormatInvalid,
            154 => Self::RetainNotSupported,
            155 => Self::QoSNotSupported,
            156 => Self::UseAnotherServer,
            157 => Self::ServerMoved,
            158 => Self::SharedSubscriptionsNotSupported,
            159 => Self::ConnectionRateExceeded,
            160 => Self::MaximumConnectTime,
            161 => Self::SubscriptionIdentifiersNotSupported,
            162 => Self::WildcardSubscriptionsNotSupported,
            _ => Self::Other(val),
        }
    }
}

impl fmt::Display for ReasonCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Success => write!(f, "Success (0x00)"),
            Self::UnacceptableProtocolVersion => write!(f, "Unacceptable Protocol Version (0x01)"),
            Self::IdentifierRejected => write!(f, "Client Identifier Rejected (0x02)"),
            Self::ServerUnavailable => write!(f, "Server Unavailable (0x03)"),
            Self::BadUserNameOrPassword => write!(f, "Bad Username or Password (0x04)"),
            Self::NotAuthorized => write!(f, "Not Authorized (0x05)"),
            Self::ServerBusy => write!(f, "Server Busy (0x89)"),
            Self::BadAuthenticationMethod => write!(f, "Bad Authentication Method (0x8C)"),
            Self::KeepAliveTimeout => write!(f, "Keep Alive Timeout (0x8D)"),
            Self::SessionTakenOver => write!(f, "Session Taken Over (0x8E)"),
            Self::TopicFilterInvalid => write!(f, "Topic Filter Invalid (0x8F)"),
            Self::TopicNameInvalid => write!(f, "Topic Name Invalid (0x90)"),
            Self::ReceiveMaximumExceeded => write!(f, "Receive Maximum Exceeded (0x93)"),
            Self::TopicAliasInvalid => write!(f, "Topic Alias Invalid (0x94)"),
            Self::PacketTooLarge => write!(f, "Packet Too Large (0x95)"),
            Self::MessageRateTooHigh => write!(f, "Message Rate Too High (0x96)"),
            Self::QuotaExceeded => write!(f, "Quota Exceeded (0x97)"),
            Self::AdministrativeAction => write!(f, "Administrative Action (0x98)"),
            Self::PayloadFormatInvalid => write!(f, "Payload Format Invalid (0x99)"),
            Self::RetainNotSupported => write!(f, "Retain Not Supported (0x9A)"),
            Self::QoSNotSupported => write!(f, "QoS Not Supported (0x9B)"),
            Self::UseAnotherServer => write!(f, "Use Another Server (0x9C)"),
            Self::ServerMoved => write!(f, "Server Moved (0x9D)"),
            Self::SharedSubscriptionsNotSupported => {
                write!(f, "Shared Subscriptions Not Supported (0x9E)")
            }
            Self::ConnectionRateExceeded => write!(f, "Connection Rate Exceeded (0x9F)"),
            Self::MaximumConnectTime => write!(f, "Maximum Connect Time (0xA0)"),
            Self::SubscriptionIdentifiersNotSupported => {
                write!(f, "Subscription Identifiers Not Supported (0xA1)")
            }
            Self::WildcardSubscriptionsNotSupported => {
                write!(f, "Wildcard Subscriptions Not Supported (0xA2)")
            }
            Self::Other(c) => write!(f, "Reason Code 0x{c:02X}"),
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
    StateMismatch,
    UnexpectedPacket,
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
            Self::StateMismatch => write!(f, "Action invalid in current connection state"),
            Self::UnexpectedPacket => write!(f, "Received unexpected packet for current state"),
        }
    }
}

/// Codec errors occurring during serialization or deserialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum CodecError {
    BufferTooSmall,
    IncompletePacket,
    MalformedPacket,
    InvalidPacketType(u8),
    InvalidUtf8String,
    TooManyProperties,
    BatchCapacityExceeded,
    PayloadTooLarge,
    UnsupportedQoS,
}

impl From<PacketError> for CodecError {
    fn from(err: PacketError) -> Self {
        match err {
            PacketError::BufferTooSmall => Self::BufferTooSmall,
            PacketError::IncompletePacket => Self::IncompletePacket,
            PacketError::MalformedPacket => Self::MalformedPacket,
            PacketError::InvalidPacketType(t) => Self::InvalidPacketType(t),
            PacketError::InvalidUtf8String => Self::InvalidUtf8String,
            PacketError::TooManyProperties => Self::TooManyProperties,
            PacketError::BatchCapacityExceeded => Self::BatchCapacityExceeded,
            PacketError::PayloadTooLarge => Self::PayloadTooLarge,
            PacketError::UnsupportedQoS => Self::UnsupportedQoS,
        }
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BufferTooSmall => write!(f, "Buffer is too small for packet"),
            Self::IncompletePacket => write!(f, "Incomplete packet data"),
            Self::MalformedPacket => write!(f, "Malformed packet structure"),
            Self::InvalidPacketType(t) => write!(f, "Invalid packet type 0x{t:02X}"),
            Self::InvalidUtf8String => write!(f, "Invalid UTF-8 string"),
            Self::TooManyProperties => write!(f, "Too many MQTT 5 properties"),
            Self::BatchCapacityExceeded => write!(f, "Batch capacity exceeded"),
            Self::PayloadTooLarge => write!(f, "Payload too large"),
            Self::UnsupportedQoS => write!(f, "Unsupported QoS level"),
        }
    }
}

/// Errors originating from storage operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum StorageErrorKind {
    NotFound,
    Corrupted,
    Full,
    IoFailure,
}

impl fmt::Display for StorageErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => write!(f, "Storage key not found"),
            Self::Corrupted => write!(f, "Storage data corrupted"),
            Self::Full => write!(f, "Storage capacity full"),
            Self::IoFailure => write!(f, "Storage I/O failure"),
        }
    }
}

/// Errors originating from hardware crypto or TLS engines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum CryptoErrorKind {
    HandshakeFailed,
    CertificateInvalid,
    EncryptionError,
    DecryptionError,
    HardwareFault,
    UnsupportedAlgorithm,
}

impl fmt::Display for CryptoErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HandshakeFailed => write!(f, "TLS handshake failed"),
            Self::CertificateInvalid => write!(f, "Certificate validation failed"),
            Self::EncryptionError => write!(f, "Encryption operation error"),
            Self::DecryptionError => write!(f, "Decryption operation error"),
            Self::HardwareFault => write!(f, "Hardware crypto engine fault"),
            Self::UnsupportedAlgorithm => write!(f, "Unsupported cryptographic algorithm"),
        }
    }
}

/// Primary unified error type across all runtimes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MqttError<E> {
    /// Transport-level I/O failure.
    Transport(E),
    /// Protocol violation or malformed packet.
    Protocol(ProtocolError),
    /// Packet serialization / deserialization error.
    Codec(CodecError),
    /// Storage failure.
    Storage(StorageErrorKind),
    /// Cryptographic or TLS error.
    Crypto(CryptoErrorKind),
    /// Broker rejected the connection handshake.
    ConnectionRefused(ReasonCode),
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
    /// Packet ID collision detected with unacknowledged in-flight packet.
    CollisionDetected(u16),
}

impl<E: TransportError> From<E> for MqttError<E> {
    fn from(err: E) -> Self {
        Self::Transport(err)
    }
}

impl<E: TransportError> From<PacketError> for MqttError<E> {
    fn from(err: PacketError) -> Self {
        Self::Codec(err.into())
    }
}

impl<E: TransportError> From<CodecError> for MqttError<E> {
    fn from(err: CodecError) -> Self {
        Self::Codec(err)
    }
}

impl<E: TransportError> From<ProtocolError> for MqttError<E> {
    fn from(err: ProtocolError) -> Self {
        Self::Protocol(err)
    }
}

impl<E: fmt::Display> fmt::Display for MqttError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(e) => write!(f, "Transport error: {e}"),
            Self::Protocol(p) => write!(f, "Protocol error: {p}"),
            Self::Codec(c) => write!(f, "Codec error: {c}"),
            Self::Storage(s) => write!(f, "Storage error: {s}"),
            Self::Crypto(cr) => write!(f, "Crypto error: {cr}"),
            Self::ConnectionRefused(r) => write!(f, "Connection refused: {r}"),
            Self::NotConnected => write!(f, "Client is not connected"),
            Self::BufferTooSmall => write!(f, "Buffer is too small for operation"),
            Self::Timeout => write!(f, "Operation timed out"),
            Self::BatchCapacityExceeded => write!(f, "Batch message capacity exceeded"),
            Self::InflightQueueFull => write!(f, "Inflight queue is full"),
            Self::CollisionDetected(id) => write!(f, "Packet ID collision detected for ID {id}"),
        }
    }
}

#[cfg(feature = "std")]
impl<E: std::error::Error + 'static> std::error::Error for MqttError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(e) => Some(e),
            _ => None,
        }
    }
}
