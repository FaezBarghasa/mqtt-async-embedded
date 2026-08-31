//! # Web Server Streaming Bridge (Axum, Actix-web, SSE, MJPEG)
//!
//! Provides high-performance stream bridges and multi-client broadcast hubs
//! for integrating MQTT telemetry, audio, and security camera video feeds into
//! web servers such as **Axum** and **Actix-web**.
//!
//! ## Features:
//! - **`MqttBroadcastHub`**: Multi-client fanout hub distributing a single MQTT topic feed
//!   to thousands of concurrent HTTP / WebSocket / SSE client connections with zero topic congestion.
//! - **`CameraMjpegBridge`**: Formats JPEG frame streams from edge cameras into
//!   standard `multipart/x-mixed-replace; boundary=frame` streams playable in standard `<img>` tags.
//! - **`TelemetrySseBridge`**: Server-Sent Events (SSE) stream formatter for real-time web dashboards.

use std::format;
use std::pin::Pin;
use std::string::String;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{Bytes, BytesMut};
use futures_util::Stream;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

use mqtt_packet::QoS;
use mqtt_tokio::{AsyncClient, ClientError};

/// A multi-client broadcast hub distributing a single MQTT stream to multiple HTTP/WebSocket web clients.
#[derive(Clone)]
pub struct MqttBroadcastHub {
    topic: Arc<str>,
    sender: broadcast::Sender<Bytes>,
    _worker_handle: Arc<JoinHandle<()>>,
}

impl MqttBroadcastHub {
    /// Spawns a new broadcast hub for the given MQTT topic filter.
    pub async fn new(
        client: &AsyncClient,
        topic: impl Into<String>,
        qos: QoS,
        broadcast_capacity: usize,
    ) -> Result<Self, ClientError> {
        let topic_str = topic.into();
        let topic_arc: Arc<str> = Arc::from(topic_str.clone());
        let mut sub_stream = client.subscribe_stream(&topic_str, qos).await?;
        let (tx, _rx) = broadcast::channel(broadcast_capacity);

        let tx_clone = tx.clone();
        let handle = tokio::spawn(async move {
            while let Some(msg) = sub_stream.recv().await {
                // Ignore send error if no web clients are currently listening
                let _ = tx_clone.send(msg.payload);
            }
        });

        Ok(Self {
            topic: topic_arc,
            sender: tx,
            _worker_handle: Arc::new(handle),
        })
    }

    /// Subscribes a new web client, returning a dedicated async [`Stream`].
    pub fn subscribe(&self) -> WebClientStream {
        let mut bcast_rx = self.sender.subscribe();
        let (mpsc_tx, mpsc_rx) = mpsc::channel(64);

        tokio::spawn(async move {
            loop {
                match bcast_rx.recv().await {
                    Ok(payload) => {
                        if mpsc_tx.send(payload).await.is_err() {
                            // Web client disconnected
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Skip lagged frames to maintain real-time low latency
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        WebClientStream { receiver: mpsc_rx }
    }

    /// Returns the topic filter of this broadcast hub.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Number of currently active web client streams.
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

/// A standard [`Stream`] yielding chunks for Axum / Actix HTTP response bodies or WebSockets.
pub struct WebClientStream {
    receiver: mpsc::Receiver<Bytes>,
}

impl Stream for WebClientStream {
    type Item = Result<Bytes, ClientError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(bytes)) => Poll::Ready(Some(Ok(bytes))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// An MJPEG (Motion JPEG) security camera stream bridge formatting frames for browser `<img>` elements.
pub struct CameraMjpegBridge {
    stream: WebClientStream,
    boundary: &'static str,
}

impl CameraMjpegBridge {
    /// Standard MIME content-type header for MJPEG HTTP responses.
    pub const CONTENT_TYPE: &'static str = "multipart/x-mixed-replace; boundary=frame";

    /// Wraps a broadcast hub stream into an MJPEG multipart HTTP stream.
    pub fn new(hub: &MqttBroadcastHub) -> Self {
        Self {
            stream: hub.subscribe(),
            boundary: "frame",
        }
    }

    /// Custom boundary constructor.
    pub fn with_boundary(hub: &MqttBroadcastHub, boundary: &'static str) -> Self {
        Self {
            stream: hub.subscribe(),
            boundary,
        }
    }
}

impl Stream for CameraMjpegBridge {
    type Item = Result<Bytes, ClientError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(jpeg_bytes))) => {
                let header = format!(
                    "--{}\r\nContent-Type: image/jpeg\r\nContent-Length: {}\r\n\r\n",
                    self.boundary,
                    jpeg_bytes.len()
                );
                let mut chunk = BytesMut::with_capacity(header.len() + jpeg_bytes.len() + 2);
                chunk.extend_from_slice(header.as_bytes());
                chunk.extend_from_slice(&jpeg_bytes);
                chunk.extend_from_slice(b"\r\n");
                Poll::Ready(Some(Ok(chunk.freeze())))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// A Server-Sent Events (SSE) telemetry stream bridge.
pub struct TelemetrySseBridge {
    stream: WebClientStream,
}

impl TelemetrySseBridge {
    /// Standard MIME content-type header for Server-Sent Events.
    pub const CONTENT_TYPE: &'static str = "text/event-stream";

    /// Wraps a broadcast hub stream into an SSE event stream.
    pub fn new(hub: &MqttBroadcastHub) -> Self {
        Self {
            stream: hub.subscribe(),
        }
    }
}

impl Stream for TelemetrySseBridge {
    type Item = Result<Bytes, ClientError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(payload))) => {
                let mut event = BytesMut::with_capacity(payload.len() + 16);
                event.extend_from_slice(b"data: ");
                event.extend_from_slice(&payload);
                event.extend_from_slice(b"\n\n");
                Poll::Ready(Some(Ok(event.freeze())))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
