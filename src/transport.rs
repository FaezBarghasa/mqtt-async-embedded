#![allow(async_fn_in_trait)]

use core::fmt::Debug;

#[cfg(feature = "transport-quic")]
use std::format;
#[cfg(feature = "transport-quic")]
use std::string::String;

pub use crate::error::ErrorPlaceHolder;

/// A trait representing a transport error.
pub trait TransportError: Debug {}

impl TransportError for () {}
impl TransportError for core::convert::Infallible {}

#[cfg(feature = "std")]
impl TransportError for std::io::Error {}

/// A trait representing a stream-based transport for MQTT packets.
pub trait MqttTransport {
    /// The error type returned by the transport.
    type Error: TransportError;

    /// Sends a buffer of data over the transport.
    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error>;

    /// Receives data from the transport into a buffer. Returns bytes read.
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;

    /// Fast-path vectored write: sends multiple contiguous slices in order without intermediate buffer copies.
    async fn send_vectored(&mut self, bufs: &[&[u8]]) -> Result<(), Self::Error> {
        for b in bufs {
            if !b.is_empty() {
                self.send(b).await?;
            }
        }
        Ok(())
    }
}

/// A trait representing an MQTT-over-QUIC transport.
/// Provides stream multiplexing (eliminating Head-of-Line blocking) and fast unreliable datagrams.
pub trait MqttQuicTransport {
    type Error: TransportError;
    type SendStream: MqttQuicSendStream<Error = Self::Error>;
    type RecvStream: MqttQuicRecvStream<Error = Self::Error>;

    /// Opens a bidirectional stream (ideal for control packets CONNECT/CONNACK or QoS 1 request-response).
    async fn open_bi_stream(&mut self)
    -> Result<(Self::SendStream, Self::RecvStream), Self::Error>;

    /// Opens a unidirectional send stream (ideal for QoS 0 telemetry bursts).
    async fn open_uni_stream(&mut self) -> Result<Self::SendStream, Self::Error>;

    /// Accepts an incoming bidirectional stream initiated by the broker.
    async fn accept_bi_stream(
        &mut self,
    ) -> Result<(Self::SendStream, Self::RecvStream), Self::Error>;

    /// Sends an unreliable QUIC datagram (fastest possible transfer for real-time sensor streams).
    async fn send_datagram(&mut self, data: &[u8]) -> Result<(), Self::Error>;

    /// Receives an unreliable QUIC datagram.
    async fn recv_datagram(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;
}

/// QUIC Send Stream abstraction.
pub trait MqttQuicSendStream {
    type Error: TransportError;
    async fn write(&mut self, buf: &[u8]) -> Result<(), Self::Error>;
    async fn finish(&mut self) -> Result<(), Self::Error>;
}

/// QUIC Recv Stream abstraction.
pub trait MqttQuicRecvStream {
    type Error: TransportError;
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;
}

// ---------------------------------------------------------------------------
// Universal embedded-io-async Adapters (esp-hal, esp-idf, embassy-net, UART)
// ---------------------------------------------------------------------------

/// Transport error wrapper for errors implementing `embedded_io_async::Error` or `core::fmt::Debug`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum EmbeddedIoError<E> {
    Io(E),
}

impl<E: core::fmt::Debug> TransportError for EmbeddedIoError<E> {}

/// Universal MQTT transport adapter for any stream implementing `embedded_io_async::Read` and `embedded_io_async::Write`.
///
/// Provides seamless out-of-the-box compatibility with:
/// - **ESP32-S series** (ESP32-S2, ESP32-S3) via `esp-hal`, `esp-wifi`, or `esp-idf-svc`
/// - **ESP32-C series** (ESP32-C2, ESP32-C3, ESP32-C6) via `esp-hal`, `esp-wifi`, or `esp-idf-svc`
/// - **Embassy** network stacks (`embassy-net::tcp::TcpSocket`)
/// - Asynchronous UART modems and cellular serial channels
pub struct EmbeddedIoTransport<S> {
    stream: S,
}

impl<S> EmbeddedIoTransport<S> {
    pub fn new(stream: S) -> Self {
        Self { stream }
    }

    pub fn into_inner(self) -> S {
        self.stream
    }

    pub fn inner(&self) -> &S {
        &self.stream
    }

    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.stream
    }
}

impl<S> MqttTransport for EmbeddedIoTransport<S>
where
    S: embedded_io_async::Read + embedded_io_async::Write,
{
    type Error = EmbeddedIoError<S::Error>;

    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.stream
            .write_all(buf)
            .await
            .map_err(EmbeddedIoError::Io)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.stream.read(buf).await.map_err(EmbeddedIoError::Io)
    }
}

/// Universal MQTT transport adapter for independent reader and writer streams (e.g. split UART RX/TX or split TCP streams).
pub struct EmbeddedIoSplitTransport<R, W> {
    reader: R,
    writer: W,
}

impl<R, W> EmbeddedIoSplitTransport<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }

    pub fn into_inner(self) -> (R, W) {
        (self.reader, self.writer)
    }

    pub fn reader(&self) -> &R {
        &self.reader
    }

    pub fn writer(&self) -> &W {
        &self.writer
    }

    pub fn reader_mut(&mut self) -> &mut R {
        &mut self.reader
    }

    pub fn writer_mut(&mut self) -> &mut W {
        &mut self.writer
    }
}

/// Transport error for split reader/writer streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum SplitIoError<RE, WE> {
    Read(RE),
    Write(WE),
}

impl<RE: core::fmt::Debug, WE: core::fmt::Debug> TransportError for SplitIoError<RE, WE> {}

impl<R, W> MqttTransport for EmbeddedIoSplitTransport<R, W>
where
    R: embedded_io_async::Read,
    W: embedded_io_async::Write,
{
    type Error = SplitIoError<R::Error, W::Error>;

    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.writer
            .write_all(buf)
            .await
            .map_err(SplitIoError::Write)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.reader.read(buf).await.map_err(SplitIoError::Read)
    }
}

// ---------------------------------------------------------------------------
// Smoltcp / embassy-net TCP transport implementation
// ---------------------------------------------------------------------------

#[cfg(feature = "transport-smoltcp")]
impl TransportError for embassy_net::tcp::Error {}

#[cfg(feature = "transport-smoltcp")]
pub struct TcpTransport<'a> {
    socket: embassy_net::tcp::TcpSocket<'a>,
    #[allow(dead_code)]
    timeout: embassy_time::Duration,
}

#[cfg(feature = "transport-smoltcp")]
impl<'a> TcpTransport<'a> {
    pub fn new(socket: embassy_net::tcp::TcpSocket<'a>, timeout: embassy_time::Duration) -> Self {
        Self { socket, timeout }
    }
}

#[cfg(feature = "transport-smoltcp")]
impl<'a> MqttTransport for TcpTransport<'a> {
    type Error = embassy_net::tcp::Error;

    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        use embedded_io_async::Write;
        self.socket.write_all(buf).await
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.socket.read(buf).await
    }
}

// ---------------------------------------------------------------------------
// QUIC Transport implementation (Host / std / Linux embedded using Quinn)
// ---------------------------------------------------------------------------

#[cfg(feature = "transport-quic")]
#[derive(Debug)]
pub struct QuinnError(pub String);

#[cfg(feature = "transport-quic")]
impl TransportError for QuinnError {}

#[cfg(feature = "transport-quic")]
pub struct QuinnQuicTransport {
    pub connection: quinn::Connection,
}

#[cfg(feature = "transport-quic")]
impl QuinnQuicTransport {
    pub fn new(connection: quinn::Connection) -> Self {
        Self { connection }
    }
}

#[cfg(feature = "transport-quic")]
pub struct QuinnSendStream(pub quinn::SendStream);

#[cfg(feature = "transport-quic")]
impl MqttQuicSendStream for QuinnSendStream {
    type Error = QuinnError;

    async fn write(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.0
            .write_all(buf)
            .await
            .map_err(|e| QuinnError(format!("Write error: {e}")))
    }

    async fn finish(&mut self) -> Result<(), Self::Error> {
        self.0
            .finish()
            .map_err(|e| QuinnError(format!("Finish error: {e}")))
    }
}

#[cfg(feature = "transport-quic")]
pub struct QuinnRecvStream(pub quinn::RecvStream);

#[cfg(feature = "transport-quic")]
impl MqttQuicRecvStream for QuinnRecvStream {
    type Error = QuinnError;

    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        match self
            .0
            .read(buf)
            .await
            .map_err(|e| QuinnError(format!("Read error: {e}")))?
        {
            Some(n) => Ok(n),
            None => Ok(0),
        }
    }
}

#[cfg(feature = "transport-quic")]
impl MqttQuicTransport for QuinnQuicTransport {
    type Error = QuinnError;
    type SendStream = QuinnSendStream;
    type RecvStream = QuinnRecvStream;

    async fn open_bi_stream(
        &mut self,
    ) -> Result<(Self::SendStream, Self::RecvStream), Self::Error> {
        let (send, recv) = self
            .connection
            .open_bi()
            .await
            .map_err(|e| QuinnError(format!("Open bi error: {e}")))?;
        Ok((QuinnSendStream(send), QuinnRecvStream(recv)))
    }

    async fn open_uni_stream(&mut self) -> Result<Self::SendStream, Self::Error> {
        let send = self
            .connection
            .open_uni()
            .await
            .map_err(|e| QuinnError(format!("Open uni error: {e}")))?;
        Ok(QuinnSendStream(send))
    }

    async fn accept_bi_stream(
        &mut self,
    ) -> Result<(Self::SendStream, Self::RecvStream), Self::Error> {
        let (send, recv) = self
            .connection
            .accept_bi()
            .await
            .map_err(|e| QuinnError(format!("Accept bi error: {e}")))?;
        Ok((QuinnSendStream(send), QuinnRecvStream(recv)))
    }

    async fn send_datagram(&mut self, data: &[u8]) -> Result<(), Self::Error> {
        self.connection
            .send_datagram(bytes::Bytes::copy_from_slice(data))
            .map_err(|e| QuinnError(format!("Datagram error: {e}")))
    }

    async fn recv_datagram(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        let dgram = self
            .connection
            .read_datagram()
            .await
            .map_err(|e| QuinnError(format!("Datagram recv error: {e}")))?;
        if dgram.len() > buf.len() {
            return Err(QuinnError("Buffer too small for datagram".into()));
        }
        buf[..dgram.len()].copy_from_slice(&dgram);
        Ok(dgram.len())
    }
}
