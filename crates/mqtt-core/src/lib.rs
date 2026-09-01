//! # `mqtt-core`
//!
//! Pure `no_std`, `no_alloc` protocol state machine, foundational traits,
//! error hierarchy, and collision management for MQTT.

#![no_std]
#![forbid(unsafe_code)]
#![allow(async_fn_in_trait)]

#[cfg(feature = "std")]
extern crate std;

pub mod error;
pub mod inflight;
pub mod state;
pub mod traits;

pub use error::{
    CodecError, CryptoErrorKind, MqttError, ProtocolError, ReasonCode, StorageErrorKind,
};
pub use inflight::{InflightEntry, InflightQueue, InflightStatus};
pub use state::{ConnState, StateAction, StateEvent, transition};
pub use traits::{Clock, Storage, Transport, TransportError, VectoredTransport, ZeroCopyTransport};
