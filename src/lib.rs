//! # Async MQTT Client for Embedded Systems
//!
//! `mqtt-async-embedded` is a `no_std` compatible, asynchronous MQTT client designed for embedded
//! systems, built upon the [Embassy](https://embassy.dev/) async ecosystem.
//!
//! ## Core Features
//!
//! - **`no_std` & `no_alloc`:** Designed to run on bare-metal microcontrollers without requiring a
//!   standard library or dynamic memory allocation. Buffers are managed using `heapless`.
//! - **Fully Async:** Built with `async/await` and leverages the Embassy ecosystem for timers
//!   and networking, ensuring non-blocking operations.
//! - **Rust 2024 Edition:** Uses native `async fn` in traits, removing the need for `async-trait`.
//! - **MQTT v3.1.1 and v5 Support:** Supports both major versions of the MQTT protocol, selectable
//!   via feature flags.
//! - **Transport Agnostic:** A flexible `MqttTransport` trait allows the client to run over any
//!   reliable, ordered, stream-based communication channel, including TCP, UART, or SPI.
//! - **QoS 0 & 1:** Implements "at most once" and "at least once" delivery guarantees.
//!
//! ## Usage
//!
//! To use the client, you need to provide a transport implementation, configure the client options,
//! and then run the `poll` method continuously to handle keep-alives and incoming messages.
//!
//! ```no_run
//! # use mqtt_async_embedded::client::{MqttClient, MqttOptions};
//! # use mqtt_async_embedded::packet::QoS;
//! # use mqtt_async_embedded::transport::MqttTransport;
//! # use core::future::Future;
//! #
//! # struct MyTransport;
//! # impl MqttTransport for MyTransport {
//! #     type Error = ();
//! #     async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error> { Ok(()) }
//! #     async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> { Ok(0) }
//! # }
//! #
//! # async fn run() -> Result<(), mqtt_async_embedded::error::MqttError<()>> {
//! let transport = MyTransport;
//! let options = MqttOptions::new("my-device-id", "mqtt.broker.com", 1883);
//! let mut client = MqttClient::<_, 5, 256>::new(transport, options);
//!
//! client.connect().await?;
//! client.publish("sensors/temperature", b"25.3", QoS::AtLeastOnce).await?;
//!
//! loop {
//!     // Poll the client to process incoming messages and send keep-alives.
//!     if let Some(event) = client.poll().await? {
//!         // Handle incoming publish packets, ACKs, etc.
//!         println!("Received event: {:?}", event);
//!     }
//! }
//! # Ok(())
//! # }
//! ```

#![no_std]
// The `async_fn_in_trait` feature is now stable in the 2024 edition, so this is no longer needed.

pub mod client;
pub mod error;
pub mod packet;
pub mod transport;
pub mod util;

// Re-export key types for easier access at the crate root.
pub use client::{MqttClient, MqttOptions};
pub use packet::QoS;

