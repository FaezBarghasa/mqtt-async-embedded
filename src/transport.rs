//! # MQTT Transport Abstraction
//!
//! This module defines the traits and implementations for abstracting the
//! underlying transport layer (e.g., TCP, UART) used by the MQTT client.
//!
//! The core of this module is the `MqttTransport` trait, which provides a
//! generic interface for sending and receiving raw byte buffers. This allows
//! the MQTT client to be hardware-agnostic and support various communication
//! channels.

use crate::error::MqttError;
use embassy_time::{Duration, Timer};
use futures::future::select;
use futures::pin_mut;
use futures::FutureExt;
use embedded_io_async::Write;

/// A trait for transport-specific errors.
///
/// This allows the main `MqttError` to be generic over the transport error type.
pub trait TransportError: core::fmt::Debug {}

/// An asynchronous MQTT transport.
///
/// This trait abstracts the underlying communication channel (e.g., TCP, UART)
/// to send and receive raw byte buffers.
pub trait MqttTransport {
    /// The specific error type for this transport.
    type Error: TransportError;

    /// Sends a buffer of bytes over the transport.
    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error>;

    /// Receives bytes from the transport into the provided buffer.
    /// Returns the number of bytes read.
    async fn recv<'a>(&mut self, buf: &'a mut [u8]) -> Result<usize, Self::Error>;

    /// Reads exactly `len` bytes into the buffer.
    /// Includes a timeout mechanism.
    async fn read_exact<'a>(
        &mut self,
        buf: &'a mut [u8],
        len: usize,
        timeout: Duration,
    ) -> Result<(), MqttError<Self::Error>> {
        let read_fut = async {
            let mut offset = 0;
            while offset < len {
                match self.recv(&mut buf[offset..len]).await {
                    Ok(0) => return Err(MqttError::Protocol(super::error::ProtocolError::InvalidResponse)), // Connection closed
                    Ok(n) => offset += n,
                    Err(e) => return Err(MqttError::Transport(e)),
                }
            }
            Ok(())
        };

        let timer = Timer::after(timeout).map(|_| Err(MqttError::Timeout));

        pin_mut!(read_fut);
        pin_mut!(timer);

        match select(read_fut, timer).await {
            futures::future::Either::Left((res, _)) => res,
            futures::future::Either::Right((res, _)) => res,
        }
    }
}

/// A sample transport error for TCP connections.
#[cfg(feature = "transport-smoltcp")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpTransportError(pub embassy_net::tcp::Error);

#[cfg(feature = "transport-smoltcp")]
impl TransportError for TcpTransportError {}

#[cfg(feature = "transport-smoltcp")]
impl From<embassy_net::tcp::Error> for TcpTransportError {
    fn from(e: embassy_net::tcp::Error) -> Self {
        TcpTransportError(e)
    }
}

/// An `MqttTransport` implementation for `embassy-net` TCP sockets.
#[cfg(feature = "transport-smoltcp")]
impl<'a> MqttTransport for embassy_net::tcp::TcpSocket<'a> {
    type Error = TcpTransportError;

    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.write_all(buf).await.map_err(Into::into)
    }

    async fn recv<'b>(&mut self, buf: &'b mut [u8]) -> Result<usize, Self::Error> {
        self.read(buf).await.map_err(Into::into)
    }
}