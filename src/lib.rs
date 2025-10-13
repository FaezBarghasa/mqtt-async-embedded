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
pub use packet::QoS; // Export QoS directly from the packet module