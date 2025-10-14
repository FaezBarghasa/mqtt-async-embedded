//! # MQTT Transport Abstraction
//!
//! This module defines the `MqttTransport` trait, which abstracts the underlying
//! communication channel (like TCP, UART, etc.), allowing the MQTT client to be
//! hardware and network-stack agnostic.
//!
//! With the Rust 2024 Edition, this trait uses native `async fn`, removing the
//! need for the `#[async_trait]` macro.


/// A placeholder error type used in contexts where the actual transport error is not known,
/// such as in the `EncodePacket` trait.
#[derive(Debug, Copy, Clone)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ErrorPlaceHolder;

/// A trait representing a transport for MQTT packets.
///
/// This trait abstracts over any reliable, ordered, stream-based communication channel.
// `async fn` in traits is now stable in Rust 2024, so `#[async_trait]` is not needed.
pub trait MqttTransport {
    /// The error type returned by the transport.
    type Error: core::fmt::Debug;

    /// Sends a buffer of data over the transport.
    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error>;

    /// Receives data from the transport into a buffer.
    ///
    /// Returns the number of bytes read.
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;
}

// Allow the placeholder to be treated as a transport error for generic contexts.
impl TransportError for ErrorPlaceHolder {}

/// A marker trait for transport-related errors.
pub trait TransportError: core::fmt::Debug {}

/// An example TCP transport implementation using `embassy-net`.
#[cfg(feature = "transport-smoltcp")]
pub struct TcpTransport<'a> {
    socket: embassy_net::tcp::TcpSocket<'a>,
    timeout: Duration,
}

#[cfg(feature = "transport-smoltcp")]
impl<'a> TcpTransport<'a> {
    /// Creates a new `TcpTransport` with the given socket and timeout.
    pub fn new(socket: embassy_net::tcp::TcpSocket<'a>, timeout: Duration) -> Self {
        Self { socket, timeout }
    }

    /// A helper function to perform a read with a timeout.
    async fn read_with_timeout<'b>(
        &'b mut self,
        buf: &'b mut [u8],
    ) -> Result<Result<usize, MqttError<embassy_net::tcp::Error>>, MqttError<embassy_net::tcp::Error>>
    {
        // Use `select` to race the read operation against a timer.
        let read_fut = self.socket.read(buf).map(Ok);
        let timer = Timer::after(self.timeout).map(|_| Err(MqttError::Timeout));

        match futures::future::select(read_fut, timer).await {
            futures::future::Either::Left((Ok(Ok(n)), _)) => {
                if n == 0 {
                    // If the peer closes the connection, read returns 0.
                    Err(MqttError::Protocol(super::error::ProtocolError::InvalidResponse))
                } else {
                    Ok(Ok(n))
                }
            }
            futures::future::Either::Left((Ok(Err(e)), _)) => Ok(Err(MqttError::Transport(e))),
            futures::future::Either::Right((Err(e), _)) => Err(e),
            _ => unreachable!(),
        }
    }
}

#[cfg(feature = "transport-smoltcp")]
impl<'a> MqttTransport for TcpTransport<'a> {
    type Error = MqttError<embassy_net::tcp::Error>;

    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.socket.write_all(buf).await.map_err(MqttError::Transport)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        match self.read_with_timeout(buf).await {
            Ok(Ok(n)) => Ok(n),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(e),
        }
    }
}

