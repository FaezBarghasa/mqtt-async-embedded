//! # `mqtt-tokio`
//!
//! A high-performance, asynchronous MQTT client for Tokio, with QUIC support,
//! zero-copy multi-packet batching, topic subscription streams, offline queues,
//! and session data recovery.

#![forbid(unsafe_code)]

pub mod client;
pub mod eventloop;
pub mod fallback;
pub mod options;
pub mod router;
pub mod stream;
pub mod transport;
pub mod types;

pub use client::{AsyncClient, Client};
pub use eventloop::EventLoop;
pub use fallback::SmartTransport;
pub use options::{
    ClientOptions, DropStrategy, OfflineQueuePolicy, ReconnectPolicy,
    TransportTarget,
};
pub use router::{TopicRouter, validate_topic_filter};
pub use stream::{
    DataStreamConsumer, DataStreamProducer, SensorDataType, StreamChunk,
};
pub use transport::{AsyncTransport, BoxedTransport, connect_transport};
pub use types::{
    ClientError, ConnectionStatus, DataRecoveryPolicy, PublishMessage, TopicSubscription,
};
