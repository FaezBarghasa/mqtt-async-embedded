//! # `mqtt-bridges`
//!
//! Web server bridges (Axum, Actix-web, SSE, MJPEG) and Slint GUI integrations for `mqtt-tokio`.

#![forbid(unsafe_code)]

pub mod slint_support;
pub mod web;

pub use slint_support::SlintStreamBinding;
pub use web::{CameraMjpegBridge, MqttBroadcastHub, TelemetrySseBridge, WebClientStream};
