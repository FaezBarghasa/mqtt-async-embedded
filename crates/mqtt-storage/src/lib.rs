//! # `mqtt-storage`
//!
//! Zero-allocation `no_std` persistence traits and static in-memory store
//! for MQTT sessions and offline queuing.

#![no_std]
#![forbid(unsafe_code)]

#[cfg(feature = "std")]
extern crate std;

pub mod mem;

pub use mem::{MemStorageError, StaticMemStore};
