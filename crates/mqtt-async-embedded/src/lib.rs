//! # Async MQTT Client for Embedded Systems
//!
//! `mqtt-async-embedded` is a `no_std` compatible, asynchronous MQTT client designed for embedded
//! systems, built upon the [Embassy](https://embassy.dev/) async ecosystem with multi-packet burst
//! batching and QUIC/H3 real-time transport support.
//!
//! ## Core Features
//!
//! - **`no_std` & `no_alloc`:** Runs on bare-metal microcontrollers without dynamic memory allocation.
//! - **Fully Async:** Built with native `async/await` in Rust 2024.
//! - **Multi-Packet Batching:** Burst publish (`publish_batch`) and multi-event polling (`poll_batch`).
//! - **QUIC / HTTP/3 Support:** Stream multiplexing and real-time datagrams (`MqttQuicTransport`).
//! - **MQTT v3.1.1 and v5 Support:** Selected dynamically or via options.
//! - **Transport Agnostic:** `MqttTransport` trait allows running over TCP, UART, SPI, or QUIC.

#![no_std]
#![forbid(unsafe_code)]

#[cfg(feature = "std")]
extern crate std;

pub use mqtt_core as core;
pub use mqtt_crypto as crypto;
pub use mqtt_packet as packet;
pub use mqtt_packet as util;
pub use mqtt_storage as storage;

pub mod client {
    pub use mqtt_embedded::client::*;
    pub use mqtt_packet::MqttVersion;
}

pub mod error {
    pub use mqtt_core::error as core_error;
    pub use mqtt_embedded::error::*;
    pub use mqtt_packet::PacketError;
}

pub mod traits {
    pub use mqtt_core::traits::*;
    pub use mqtt_crypto::traits::*;
}

pub mod inflight {
    pub use mqtt_embedded::inflight::*;
}

pub mod stream_writer {
    pub use mqtt_embedded::stream_writer::*;
}

pub mod transport {
    pub use mqtt_embedded::transport::*;
}

#[cfg(feature = "tokio-client")]
pub use mqtt_tokio as tokio_client;

#[cfg(feature = "tokio-client")]
pub use mqtt_bridges as bridges;

// Re-export key types at root level for seamless backwards compatibility
pub use mqtt_embedded::{
    ConnectionState, EmbeddedIoError, EmbeddedIoSplitTransport, EmbeddedIoTransport,
    ErrorPlaceHolder, MqttClient, MqttError, MqttEvent, MqttOptions, MqttQuicRecvStream,
    MqttQuicSendStream, MqttQuicTransport, MqttStreamWriter, MqttTransport, ProtocolError,
    PublishMessage, QuicErrorKind, QuicMqttClient, SplitIoError, StreamMode, TransportError,
};

pub use mqtt_packet::{
    ConnAck, Connect, DecodePacket, Disconnect, EncodePacket, MqttPacket, MqttVersion, PacketError,
    PingReq, PingResp, Property, PubAck, PubComp, PubRec, PubRel, Publish, QoS, RawPacketFrameIter,
    SubAck, Subscribe, UnsubAck, Unsubscribe, Will, decode,
};

#[cfg(feature = "transport-smoltcp")]
pub use mqtt_embedded::TcpTransport;

#[cfg(feature = "tokio-client")]
pub use mqtt_tokio::{
    AsyncClient as TokioAsyncClient, Client as TokioClient, ClientError as TokioClientError,
    ClientOptions as TokioClientOptions, ConnectionStatus as TokioConnectionStatus,
    DataRecoveryPolicy, DropStrategy, OfflineQueuePolicy, PublishMessage as TokioPublishMessage,
    ReconnectPolicy, SmartTransport, TopicRouter, TopicSubscription,
};

#[cfg(feature = "tokio-client")]
pub use mqtt_bridges::{
    CameraMjpegBridge, MqttBroadcastHub, SlintStreamBinding, TelemetrySseBridge, WebClientStream,
};
