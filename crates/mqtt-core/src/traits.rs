//! # Foundational Traits for MQTT Transports, Clocks, and Storage
//!
//! Provides the core abstraction layer decoupling the protocol state machine
//! from physical I/O runtimes (Embassy, Tokio, io_uring, bare-metal UART).

use core::fmt::Debug;

/// Common error trait required for all transport-level errors.
pub trait TransportError: Debug {}

impl TransportError for () {}

/// A dummy unit error type implementing `TransportError` and `Display`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnitTransportError;

impl core::fmt::Display for UnitTransportError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Unit transport error")
    }
}

impl TransportError for UnitTransportError {}

/// Basic asynchronous transport abstraction for full-duplex byte stream I/O.
pub trait Transport {
    type Error: TransportError;

    /// Sends a slice of bytes to the network transport.
    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error>;

    /// Receives bytes from the network transport into the provided buffer.
    ///
    /// Returns the number of bytes read. Returns 0 on EOF / socket closure.
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;
}

/// Extended transport supporting vectored (scatter-gather) write operations.
pub trait VectoredTransport: Transport {
    /// Writes multiple non-contiguous buffers in a single operation.
    async fn send_vectored(&mut self, bufs: &[&[u8]]) -> Result<(), Self::Error> {
        for buf in bufs {
            self.send(buf).await?;
        }
        Ok(())
    }
}

/// Zero-copy receive transport abstraction.
pub trait ZeroCopyTransport: Transport {
    type Buffer: AsRef<[u8]>;

    /// Receives a zero-copy buffer slice directly from the underlying transport ring.
    async fn recv_zero_copy(&mut self) -> Result<Self::Buffer, Self::Error>;
}

/// Abstract monotonic clock for timeouts, keep-alive timers, and backoff jitter.
pub trait Clock {
    type Instant: Copy + Ord;
    type Duration: Copy;

    /// Returns the current monotonic timestamp.
    fn now(&self) -> Self::Instant;

    /// Calculates the elapsed duration between two instants.
    fn elapsed(&self, earlier: Self::Instant) -> Self::Duration;
}

/// Abstract storage trait for session and offline queue persistence.
pub trait Storage {
    type Error: Debug + core::fmt::Display;

    /// Persists a key-value record to durable storage.
    async fn persist(&mut self, key: &[u8], data: &[u8]) -> Result<(), Self::Error>;

    /// Loads a record from storage by key.
    async fn load(&mut self, key: &[u8], buf: &mut [u8]) -> Result<Option<usize>, Self::Error>;

    /// Removes a record from storage by key.
    async fn remove(&mut self, key: &[u8]) -> Result<(), Self::Error>;
}
