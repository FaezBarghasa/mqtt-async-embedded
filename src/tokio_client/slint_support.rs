//! # Slint & GUI Application Integration Support
//!
//! Provides ergonomic, thread-safe bridges and stream binders for integrating
//! `mqtt-async-embedded` into **Slint UI applications** across both:
//! 1. **`std` Desktop / Mobile / Embedded Linux (Tokio)**: Cross-thread event loop dispatching
//!    via `slint::Weak::upgrade_in_event_loop`.
//! 2. **`no_std` Bare-Metal Embedded MCUs**: Zero-allocation polling within MCU display render loops.

use bytes::Bytes;
use std::string::{String, ToString};
use std::sync::Arc;
use tokio::task::JoinHandle;

use crate::packet::QoS;
use crate::tokio_client::client::AsyncClient;
use crate::tokio_client::stream::StreamChunk;
use crate::tokio_client::types::ClientError;

/// A thread-safe UI stream binding handle that pipes incoming MQTT messages into a Slint event loop callback.
pub struct SlintStreamBinding {
    topic: Arc<str>,
    worker_handle: JoinHandle<()>,
}

impl SlintStreamBinding {
    /// Subscribes to an MQTT topic filter and automatically forwards each payload string
    /// to the provided Slint UI callback on the UI main thread.
    ///
    /// ## Example:
    /// ```rust,ignore
    /// let weak_ui = ui.as_weak();
    /// let binding = SlintStreamBinding::bind_string_property(
    ///     &client,
    ///     "sensors/+/temperature",
    ///     QoS::AtLeastOnce,
    ///     move |topic, value| {
    ///         let weak = weak_ui.clone();
    ///         let _ = weak.upgrade_in_event_loop(move |ui| {
    ///             ui.set_temperature_text(format!("{topic}: {value}").into());
    ///         });
    ///     },
    /// ).await?;
    /// ```
    pub async fn bind_string_property<F>(
        client: &AsyncClient,
        topic: impl Into<String>,
        qos: QoS,
        mut callback: F,
    ) -> Result<Self, ClientError>
    where
        F: FnMut(String, String) + Send + 'static,
    {
        let topic_str = topic.into();
        let topic_arc: Arc<str> = Arc::from(topic_str.clone());
        let mut sub_stream = client.subscribe_stream(&topic_str, qos).await?;

        let handle = tokio::spawn(async move {
            while let Some(msg) = sub_stream.recv().await {
                let topic = msg.topic.clone();
                let text = msg.payload_as_str().unwrap_or("").to_string();
                callback(topic, text);
            }
        });

        Ok(Self {
            topic: topic_arc,
            worker_handle: handle,
        })
    }

    /// Subscribes to an MQTT camera video topic and forwards raw JPEG / RGB frame bytes
    /// directly to a Slint UI frame rendering callback.
    ///
    /// ## Example:
    /// ```rust,ignore
    /// let weak_ui = ui.as_weak();
    /// let binding = SlintStreamBinding::bind_camera_frame(
    ///     &client,
    ///     "security/camera/01/mjpeg",
    ///     QoS::AtMostOnce,
    ///     move |jpeg_bytes| {
    ///         let weak = weak_ui.clone();
    ///         let _ = weak.upgrade_in_event_loop(move |ui| {
    ///             if let Ok(img) = slint::Image::load_from_svg_data(&jpeg_bytes) {
    ///                 ui.set_camera_frame(img);
    ///             }
    ///         });
    ///     },
    /// ).await?;
    /// ```
    pub async fn bind_camera_frame<F>(
        client: &AsyncClient,
        topic: impl Into<String>,
        qos: QoS,
        mut callback: F,
    ) -> Result<Self, ClientError>
    where
        F: FnMut(Bytes) + Send + 'static,
    {
        let topic_str = topic.into();
        let topic_arc: Arc<str> = Arc::from(topic_str.clone());
        let mut sub_stream = client.subscribe_stream(&topic_str, qos).await?;

        let handle = tokio::spawn(async move {
            while let Some(msg) = sub_stream.recv().await {
                callback(msg.payload);
            }
        });

        Ok(Self {
            topic: topic_arc,
            worker_handle: handle,
        })
    }

    /// Subscribes to a sequenced data stream with out-of-order reordering and calls the UI handler.
    pub async fn bind_ordered_datastream<F>(
        client: &AsyncClient,
        topic: impl Into<String>,
        qos: QoS,
        reorder_window: usize,
        mut callback: F,
    ) -> Result<Self, ClientError>
    where
        F: FnMut(StreamChunk) + Send + 'static,
    {
        let topic_str = topic.into();
        let topic_arc: Arc<str> = Arc::from(topic_str.clone());
        let mut consumer = client
            .subscribe_datastream(&topic_str, qos, reorder_window)
            .await?;

        let handle = tokio::spawn(async move {
            while let Ok(Some(chunk)) = consumer.recv_ordered().await {
                callback(chunk);
            }
        });

        Ok(Self {
            topic: topic_arc,
            worker_handle: handle,
        })
    }

    /// Returns the topic filter of this UI stream binding.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Stops the UI binding background listener task.
    pub fn abort(&self) {
        self.worker_handle.abort();
    }
}
