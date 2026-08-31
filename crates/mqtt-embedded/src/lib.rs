//! # `mqtt-embedded`
//!
//! A zero-allocation, `no_std`, `no_alloc` asynchronous MQTT client for microcontrollers
//! (STM32, ESP32, RISC-V) and Embassy.

#![no_std]
#![forbid(unsafe_code)]

#[cfg(feature = "std")]
extern crate std;

pub mod client;
pub mod error;
pub mod inflight;
pub mod stream_writer;
pub mod transport;

pub use client::{
    ConnectionState, MqttClient, MqttEvent, MqttOptions, PublishMessage, QuicMqttClient, StreamMode,
};
pub use error::{ConnectReasonCode, ErrorPlaceHolder, MqttError, ProtocolError, QuicErrorKind};
pub use inflight::{InflightEntry, InflightQueue, InflightStatus};
pub use stream_writer::MqttStreamWriter;
pub use transport::{
    EmbeddedIoError, EmbeddedIoSplitTransport, EmbeddedIoTransport, MqttQuicRecvStream,
    MqttQuicSendStream, MqttQuicTransport, MqttTransport, SplitIoError, TlsTransport,
    TransportError,
};

#[cfg(feature = "transport-smoltcp")]
pub use transport::TcpTransport;
