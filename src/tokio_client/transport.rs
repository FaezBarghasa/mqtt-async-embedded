//! # Tokio Transport Connectors and Framed Stream
//!
//! Provides platform-native connection drivers for **Linux**, **Windows**, and **Android**:
//! - **Linux Driver**: TCP with nodelay, Rustls TLS, QUIC (`quinn`), and Unix Domain Sockets (`tokio::net::UnixStream`).
//! - **Windows Driver**: TCP, Rustls TLS, QUIC (`quinn`), and Windows Named Pipes (`tokio::net::windows::named_pipe`).
//! - **Android Driver**: TCP, Rustls TLS, QUIC (`quinn`), and Linux/Android abstract namespace domain sockets.

use std::boxed::Box;
use std::format;

#[cfg(feature = "transport-quic")]
use std::pin::Pin;
#[cfg(feature = "transport-quic")]
use std::task::{Context, Poll};
#[cfg(feature = "transport-quic")]
use tokio::io::ReadBuf;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::tokio_client::options::TransportTarget;
use crate::tokio_client::types::ClientError;

/// A trait object for any asynchronous, cancel-safe, thread-safe byte transport stream.
pub type BoxedTransport = Box<dyn AsyncTransport + Send + Unpin>;

/// Unified async transport trait combining [`AsyncRead`] and [`AsyncWrite`].
pub trait AsyncTransport: AsyncRead + AsyncWrite {
    /// Sends an unreliable QUIC datagram if supported by the underlying transport.
    fn send_datagram(&mut self, _data: &[u8]) -> Result<(), ClientError> {
        Err(ClientError::Quic(
            "Datagrams are only supported over QUIC transports".into(),
        ))
    }
}

impl AsyncTransport for TcpStream {}

#[cfg(unix)]
impl AsyncTransport for tokio::net::UnixStream {}

#[cfg(windows)]
impl AsyncTransport for tokio::net::windows::named_pipe::NamedPipeClient {}

#[cfg(feature = "tokio-tls")]
impl AsyncTransport for tokio_rustls::client::TlsStream<TcpStream> {}

#[cfg(feature = "transport-quic")]
pub struct QuicTransportStream {
    pub send_stream: quinn::SendStream,
    pub recv_stream: quinn::RecvStream,
    pub connection: quinn::Connection,
}

#[cfg(feature = "transport-quic")]
impl AsyncRead for QuicTransportStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.recv_stream).poll_read(cx, buf)
    }
}

#[cfg(feature = "transport-quic")]
impl AsyncWrite for QuicTransportStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match Pin::new(&mut self.send_stream).poll_write(cx, buf) {
            Poll::Ready(Ok(n)) => Poll::Ready(Ok(n)),
            Poll::Ready(Err(e)) => {
                Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.send_stream.finish() {
            Ok(()) => Poll::Ready(Ok(())),
            Err(e) => Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e))),
        }
    }
}

#[cfg(feature = "transport-quic")]
impl AsyncTransport for QuicTransportStream {
    fn send_datagram(&mut self, data: &[u8]) -> Result<(), ClientError> {
        self.connection
            .send_datagram(bytes::Bytes::copy_from_slice(data))
            .map_err(|e| ClientError::Quic(format!("Datagram send failed: {e}")))
    }
}

/// Connects to the specified cross-platform target and returns a boxed async transport stream.
pub async fn connect_transport(target: &TransportTarget) -> Result<BoxedTransport, ClientError> {
    match target {
        TransportTarget::Tcp { host, port } => {
            let addr = format!("{host}:{port}");
            let stream = TcpStream::connect(&addr).await?;
            let _ = stream.set_nodelay(true);
            Ok(Box::new(stream))
        }
        #[cfg(feature = "tokio-tls")]
        TransportTarget::Tls {
            host,
            port,
            server_name,
        } => {
            use std::sync::Arc;
            use tokio_rustls::TlsConnector;
            use tokio_rustls::rustls::ClientConfig;
            use tokio_rustls::rustls::pki_types::ServerName;

            let root_store = tokio_rustls::rustls::RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            };

            let config = ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth();

            let connector = TlsConnector::from(Arc::new(config));
            let addr = format!("{host}:{port}");
            let tcp_stream = TcpStream::connect(&addr).await?;
            let _ = tcp_stream.set_nodelay(true);

            let domain = ServerName::try_from(server_name.as_str())
                .map_err(|e| ClientError::Tls(format!("Invalid DNS name: {e}")))?
                .to_owned();

            let tls_stream = connector
                .connect(domain, tcp_stream)
                .await
                .map_err(|e| ClientError::Tls(format!("TLS handshake failed: {e}")))?;

            Ok(Box::new(tls_stream))
        }
        #[cfg(feature = "transport-quic")]
        TransportTarget::Quic {
            host,
            port,
            server_name,
        } => {
            use std::net::SocketAddr;
            use std::sync::Arc;

            let mut endpoint = quinn::Endpoint::client(
                "[::]:0"
                    .parse()
                    .unwrap_or_else(|_| "0.0.0.0:0".parse().expect("Valid fallback bind address")),
            )
            .map_err(|e| ClientError::Quic(format!("Failed to create QUIC endpoint: {e}")))?;

            let crypto = rustls::ClientConfig::builder()
                .with_root_certificates(rustls::RootCertStore::empty())
                .with_no_client_auth();

            let client_config = quinn::ClientConfig::new(Arc::new(
                quinn::crypto::rustls::QuicClientConfig::try_from(crypto)
                    .map_err(|e| ClientError::Quic(format!("Crypto config error: {e}")))?,
            ));

            endpoint.set_default_client_config(client_config);

            let remote_addr: SocketAddr = tokio::net::lookup_host(format!("{host}:{port}"))
                .await?
                .next()
                .ok_or_else(|| {
                    ClientError::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "Could not resolve host",
                    ))
                })?;

            let connection = endpoint
                .connect(remote_addr, server_name)
                .map_err(|e| ClientError::Quic(format!("QUIC connect error: {e}")))?
                .await
                .map_err(|e| ClientError::Quic(format!("QUIC connection failed: {e}")))?;

            let (send_stream, recv_stream) = connection
                .open_bi()
                .await
                .map_err(|e| ClientError::Quic(format!("Failed to open control stream: {e}")))?;

            Ok(Box::new(QuicTransportStream {
                send_stream,
                recv_stream,
                connection,
            }))
        }
        TransportTarget::Unix { path } => {
            #[cfg(unix)]
            {
                let stream = if let Some(abstract_name) = path.strip_prefix('@') {
                    // Linux & Android abstract namespace socket
                    tokio::net::UnixStream::connect(format!("\0{abstract_name}")).await?
                } else {
                    tokio::net::UnixStream::connect(path).await?
                };
                Ok(Box::new(stream))
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                Err(ClientError::Io(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "Unix domain sockets are not supported on this platform",
                )))
            }
        }
        TransportTarget::NamedPipe { path } => {
            #[cfg(windows)]
            {
                use tokio::net::windows::named_pipe::ClientOptions;
                let client = ClientOptions::new().open(path)?;
                Ok(Box::new(client))
            }
            #[cfg(not(windows))]
            {
                let _ = path;
                Err(ClientError::Io(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "Windows Named Pipes are only supported on Windows targets",
                )))
            }
        }
    }
}
