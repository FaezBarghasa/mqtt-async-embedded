//! # MQTT Async Embedded
//!
//! An `async`, `no_std`-compatible MQTT client for embedded systems using the [Embassy](https://embassy.dev/) async ecosystem.
//!
//! ## Features
//!
//! - **Asynchronous:** Built on `async/await` and designed for the Embassy ecosystem.
//! - **`no_std` by default:** Suitable for bare-metal and resource-constrained devices.
//! - **Hardware Agnostic:** Uses `embedded-hal-async` traits to support various communication transports.
//! - **Memory Efficient:** Leverages `heapless` to avoid dynamic memory allocation.
//! - **MQTT v3.1.1 and v5 Support:** Protocol version can be selected via feature flags.
//! - **QoS 0 & 1:** Support for "at most once" and "at least once" message delivery.
//!
//! ## Getting Started
//!
//! To use this library, you need a transport that implements the `MqttTransport` trait.
//!
//! ### Example
//!
//! ```rust,no_run
//! use mqtt_async_embedded::{MqttClient, MqttOptions, QoS};
//! use embassy_net::tcp::TcpSocket;
//! use embassy_time::Duration;
//!
//! // Assume `socket` is an already connected `TcpSocket`
//! async fn run_mqtt(mut socket: TcpSocket<'_>) {
//!     let options = MqttOptions::new("my-embedded-device")
//!         .set_keep_alive(Duration::from_secs(30));
//!
//!     let mut client: MqttClient<_, 1024, 1024> = MqttClient::new(socket, options);
//!
//!     // Connect to the broker
//!     client.connect().await.unwrap();
//!
//!     // Publish a message
//!     client.publish("sensors/temp", b"25.3", QoS::AtLeastOnce, &[]).await.unwrap();
//!
//!     // Disconnect
//!     client.disconnect().await.unwrap();
//! }
//! ```

#![no_std]
#![doc = include_str!("../README.md")]

// Allow using std for desktop testing.
#[cfg(feature = "std")]
extern crate std;

pub mod client;
pub mod error;
pub mod packet;
pub mod transport;
pub mod util;

// Re-export the primary client-facing types for convenience.
pub use client::{MqttClient, MqttOptions};
pub use packet::QoS;