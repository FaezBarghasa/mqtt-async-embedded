//! # `mqtt-crypto`
//!
//! Zero-allocation `no_std` traits for hardware cryptographic offloading
//! and TLS session abstractions.

#![no_std]
#![forbid(unsafe_code)]
#![allow(async_fn_in_trait)]

#[cfg(feature = "std")]
extern crate std;

pub mod traits;

pub use traits::{CryptoBackend, TlsSession};
