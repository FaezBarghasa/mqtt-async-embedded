//! # Tokio-Native High-Performance MQTT Client
//!
//! A standard (`std`) / `tokio`-based MQTT client designed for high throughput,
//! multi-packet batching, zero-copy payload sharing, session data recovery,
//! topic-filter stream routing, and universal transport support (TCP, TLS, QUIC, Unix).

pub mod client;
pub mod error;
pub mod eventloop;
pub mod options;
pub mod router;
pub mod transport;
pub mod types;

// Re-export core types for easy access
pub use client::{AsyncClient, Client};
pub use error::ClientError;
pub use eventloop::EventLoop;
pub use options::{
    ClientOptions, DropStrategy, OfflineQueuePolicy, ReconnectPolicy, TransportTarget,
};
pub use router::{validate_publish_topic, validate_topic_filter, TopicRouter};
pub use types::{ConnectionStatus, DataRecoveryPolicy, PublishMessage, TopicSubscription};
