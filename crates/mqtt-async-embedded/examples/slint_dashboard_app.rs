//! # Slint Desktop & Embedded GUI Integration Example
//!
//! Demonstrates how to use `mqtt-async-embedded` in **Slint UI applications**:
//!
//! ### 1. In `std` Desktop / Mobile / Embedded Linux:
//! - Connects to broker via `Client::connect(options)`.
//! - Binds incoming sensor telemetry directly to Slint UI properties using `SlintStreamBinding::bind_string_property`.
//! - Binds live camera video streams to Slint image frame rendering using `SlintStreamBinding::bind_camera_frame`.
//!
//! ### 2. In `no_std` Bare-Metal MCUs (ESP32 / STM32):
//! - Polling `client.poll().await` inside MCU display tick loop and directly updating Slint properties.

use bytes::Bytes;
use mqtt_async_embedded::bridges::SlintStreamBinding;
use mqtt_async_embedded::packet::QoS;
use mqtt_async_embedded::tokio_client::{Client, ClientOptions};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("============================================================");
    println!("   mqtt-async-embedded: Slint UI Client Application Bridge   ");
    println!("============================================================");

    // 1. Initialize client
    let options = ClientOptions::new("slint-gui-app", "127.0.0.1", 1883)
        .with_keep_alive(Duration::from_secs(30));

    let (mqtt, _handle) = Client::connect(options);
    println!("[Slint App] Connected to MQTT broker in background Tokio runtime.");

    // 2. Bind telemetry stream directly to Slint UI property updates
    let _telemetry_binding = SlintStreamBinding::bind_string_property(
        &mqtt,
        "sensors/livingroom/temperature",
        QoS::AtLeastOnce,
        |topic, value| {
            println!(" [Slint UI EventLoop] Update UI Property -> {topic}: {value}");
        },
    )
    .await?;

    // 3. Bind live security camera feed directly to Slint UI Image property
    let _camera_binding = SlintStreamBinding::bind_camera_frame(
        &mqtt,
        "security/camera/01/mjpeg",
        QoS::AtMostOnce,
        |frame_bytes| {
            println!(
                " [Slint UI EventLoop] Render new camera frame (size: {} bytes)",
                frame_bytes.len()
            );
        },
    )
    .await?;

    println!("[Slint App] UI event listeners successfully attached.");

    // 4. Simulate publishing telemetry and camera frames
    let publisher = mqtt.clone();
    tokio::spawn(async move {
        for i in 1..=3 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = publisher
                .publish(
                    "sensors/livingroom/temperature",
                    QoS::AtLeastOnce,
                    false,
                    format!("2{i}.5 °C"),
                )
                .await;

            let dummy_jpeg = b"\xFF\xD8\xFF\xE0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xFF\xD9";
            let _ = publisher
                .publish(
                    "security/camera/01/mjpeg",
                    QoS::AtMostOnce,
                    false,
                    Bytes::from_static(dummy_jpeg),
                )
                .await;
        }
    });

    tokio::time::sleep(Duration::from_millis(800)).await;
    println!("\n[Slint App] Demo run completed successfully.");
    Ok(())
}
