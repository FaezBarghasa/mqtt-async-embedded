//! # Core Types for the Tokio MQTT Client
//!
//! Provides owned, thread-safe, and zero-copy packet representations,
//! subscription streams, connection status events, and session data recovery policies.

use std::fmt;
use std::pin::Pin;
use std::string::String;
use std::task::{Context, Poll};
use std::time::Duration;
use std::vec::Vec;

use bytes::Bytes;
use futures_util::Stream;
use tokio::sync::{mpsc, oneshot};

use crate::error::ProtocolError;
use crate::packet::QoS;

/// Represents an owned MQTT Publish message.
///
/// Uses [`bytes::Bytes`] for zero-copy payload sharing across tasks and subscriptions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishMessage {
    pub topic: String,
    pub payload: Bytes,
    pub qos: QoS,
    pub retain: bool,
    pub dup: bool,
    pub packet_id: Option<u16>,
    pub user_properties: Vec<(String, String)>,
}

impl PublishMessage {
    /// Creates a new QoS 0 message with default settings.
    pub fn new(topic: impl Into<String>, payload: impl Into<Bytes>) -> Self {
        Self {
            topic: topic.into(),
            payload: payload.into(),
            qos: QoS::AtMostOnce,
            retain: false,
            dup: false,
            packet_id: None,
            user_properties: Vec::new(),
        }
    }

    /// Sets the Quality of Service level.
    pub fn with_qos(mut self, qos: QoS) -> Self {
        self.qos = qos;
        self
    }

    /// Sets the retain flag.
    pub fn with_retain(mut self, retain: bool) -> Self {
        self.retain = retain;
        self
    }

    /// Sets the duplicate (DUP) delivery flag.
    pub fn with_dup(mut self, dup: bool) -> Self {
        self.dup = dup;
        self
    }

    /// Adds a user property (MQTT v5.0).
    pub fn with_user_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.user_properties.push((key.into(), value.into()));
        self
    }

    /// Returns the payload as a UTF-8 string slice if valid.
    pub fn payload_as_str(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.payload)
    }
}

/// Policy for session recovery and in-flight message resending upon reconnection.
#[derive(Debug, Clone)]
pub struct DataRecoveryPolicy {
    /// Automatically resend unacknowledged in-flight QoS 1 and QoS 2 messages with DUP=true on reconnect.
    pub resend_unacked_inflight: bool,
    /// Automatically restore all active topic subscriptions on reconnect.
    pub auto_resubscribe: bool,
    /// Maximum number of in-flight QoS 1 / QoS 2 messages tracked concurrently.
    pub max_inflight: usize,
}

impl Default for DataRecoveryPolicy {
    fn default() -> Self {
        Self {
            resend_unacked_inflight: true,
            auto_resubscribe: true,
            max_inflight: 256,
        }
    }
}

/// Represents the connection status of the MQTT client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    /// The client is currently disconnected from the broker.
    Disconnected,
    /// The client is establishing a TCP/TLS/QUIC connection and completing the MQTT handshake.
    Connecting,
    /// The client is fully connected and authenticated.
    Connected,
    /// The client lost connection and is waiting for the next backoff retry.
    Reconnecting {
        attempt: usize,
        next_retry: Duration,
    },
    /// The client has been explicitly stopped or disconnected by user request.
    Stopped,
}

impl fmt::Display for ConnectionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disconnected => write!(f, "Disconnected"),
            Self::Connecting => write!(f, "Connecting"),
            Self::Connected => write!(f, "Connected"),
            Self::Reconnecting {
                attempt,
                next_retry,
            } => write!(
                f,
                "Reconnecting (attempt {attempt}, retry in {:.2?})",
                next_retry
            ),
            Self::Stopped => write!(f, "Stopped"),
        }
    }
}

/// An asynchronous stream of [`PublishMessage`] instances delivered for a specific topic subscription.
///
/// Implements [`futures_util::Stream`], enabling rich stream combinators (`filter`, `map`, `take`, `fold`).
#[derive(Debug)]
pub struct TopicSubscription {
    topic_filter: String,
    receiver: mpsc::Receiver<PublishMessage>,
}

impl TopicSubscription {
    pub(crate) fn new(topic_filter: String, receiver: mpsc::Receiver<PublishMessage>) -> Self {
        Self {
            topic_filter,
            receiver,
        }
    }

    /// The topic filter string associated with this subscription.
    pub fn topic_filter(&self) -> &str {
        &self.topic_filter
    }

    /// Asynchronously receives the next message on this topic subscription.
    ///
    /// Returns `None` if the subscription has been closed or the client is dropped.
    pub async fn recv(&mut self) -> Option<PublishMessage> {
        self.receiver.recv().await
    }

    /// Closes the subscription channel.
    pub fn close(&mut self) {
        self.receiver.close();
    }
}

impl Stream for TopicSubscription {
    type Item = PublishMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

/// Errors specific to Tokio MQTT client operations.
#[derive(Debug)]
pub enum ClientError {
    /// An I/O error occurred on the network transport.
    Io(std::io::Error),
    /// A protocol-level error occurred during parsing or packet encoding.
    Protocol(ProtocolError),
    /// The broker rejected the connection with a reason code.
    ConnectionRefused(u8),
    /// The provided topic or topic filter is invalid.
    InvalidTopic(String),
    /// The client request channel or offline queue is full.
    QueueFull,
    /// The client is disconnected and offline queuing is disabled or rejected.
    NotConnected,
    /// The client has been closed or dropped.
    ClientClosed,
    /// Operation timed out waiting for acknowledgment or broker response.
    Timeout,
    /// TLS configuration or handshake error.
    Tls(String),
    /// QUIC transport error.
    Quic(String),
    /// A channel or future was cancelled.
    Cancelled,
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "I/O error: {e}"),
            Self::Protocol(p) => write!(f, "Protocol error: {p:?}"),
            Self::ConnectionRefused(code) => write!(f, "Connection refused with reason {code}"),
            Self::InvalidTopic(t) => write!(f, "Invalid topic filter: '{t}'"),
            Self::QueueFull => write!(f, "Client request queue is full"),
            Self::NotConnected => write!(f, "Client is not connected"),
            Self::ClientClosed => write!(f, "Client is closed"),
            Self::Timeout => write!(f, "Operation timed out"),
            Self::Tls(msg) => write!(f, "TLS error: {msg}"),
            Self::Quic(msg) => write!(f, "QUIC error: {msg}"),
            Self::Cancelled => write!(f, "Operation cancelled"),
        }
    }
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<ProtocolError> for ClientError {
    fn from(p: ProtocolError) -> Self {
        Self::Protocol(p)
    }
}

/// Internal requests dispatched across channels to the `EventLoop`.
pub(crate) enum ClientRequest {
    Publish {
        message: PublishMessage,
        ack_sender: Option<oneshot::Sender<Result<(), ClientError>>>,
    },
    PublishBatch {
        messages: Vec<PublishMessage>,
        ack_sender: Option<oneshot::Sender<Result<usize, ClientError>>>,
    },
    PublishDatagram {
        topic: String,
        payload: Bytes,
        ack_sender: Option<oneshot::Sender<Result<(), ClientError>>>,
    },
    Subscribe {
        topic: String,
        qos: QoS,
        resp_sender: oneshot::Sender<Result<u16, ClientError>>,
        stream_sender: Option<mpsc::Sender<PublishMessage>>,
    },
    Unsubscribe {
        topic: String,
        resp_sender: oneshot::Sender<Result<u16, ClientError>>,
    },
    Disconnect {
        resp_sender: oneshot::Sender<Result<(), ClientError>>,
    },
}
