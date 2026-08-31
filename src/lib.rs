//! # Async MQTT Client for Embedded Systems
//!
//! `mqtt-async-embedded` is a `no_std` compatible, asynchronous MQTT client designed for embedded
//! systems, built upon the [Embassy](https://embassy.dev/) async ecosystem with multi-packet burst
//! batching and QUIC/H3 real-time transport support.
//!
//! ## Core Features
//!
//! - **`no_std` & `no_alloc`:** Designed to run on bare-metal microcontrollers without dynamic memory allocation.
//! - **Fully Async:** Built with native `async/await` in Rust 2024.
//! - **Multi-Packet Batching:** Burst publish (`publish_batch`) and multi-event polling (`poll_batch`) for highest throughput.
//! - **QUIC / HTTP/3 Support:** Stream multiplexing and ultra-fast real-time datagrams (`MqttQuicTransport`).
//! - **MQTT v3.1.1 and v5 Support:** Selected dynamically or via options.
//! - **Transport Agnostic:** `MqttTransport` trait allows running over TCP, UART, SPI, or QUIC.

#![no_std]

#[cfg(feature = "std")]
extern crate std;

pub mod client;
pub mod error;
pub mod packet;
pub mod transport;
pub mod util;

#[cfg(feature = "tokio-client")]
pub mod tokio_client;

// Re-export key types for easier access at the crate root.
pub use client::{
    MqttClient, MqttEvent, MqttOptions, MqttStreamWriter, MqttVersion, PublishMessage,
    QuicMqttClient, StreamMode,
};
pub use error::{MqttError, ProtocolError};
pub use packet::{Property, QoS, UnsubAck, Unsubscribe, Will};
pub use transport::{
    EmbeddedIoError, EmbeddedIoSplitTransport, EmbeddedIoTransport, MqttQuicTransport,
    MqttTransport, SplitIoError, TransportError,
};

#[cfg(feature = "tokio-client")]
pub use tokio_client::{
    AsyncClient as TokioAsyncClient, Client as TokioClient, ClientError as TokioClientError,
    ClientOptions as TokioClientOptions, ConnectionStatus as TokioConnectionStatus,
    DataRecoveryPolicy, DropStrategy, OfflineQueuePolicy, PublishMessage as TokioPublishMessage,
    ReconnectPolicy, TopicRouter, TopicSubscription,
};
