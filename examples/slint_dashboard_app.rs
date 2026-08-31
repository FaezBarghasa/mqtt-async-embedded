//! # Slint Desktop & Embedded GUI Integration Example
//!
//! Demonstrates how to use `mqtt-async-embedded` in **Slint UI applications**:
//!
//! ### 1. In `std` Desktop / Mobile / Embedded Linux:
//! - Connects to broker via `Client::connect(options)`.
//! - Binds incoming sensor telemetry directly to Slint UI properties using `bind_slint_property`.
//! - Binds live camera video streams to Slint image frame rendering using `bind_slint_camera`.
//!
//! ### 2. In `no_std` Bare-Metal MCUs (ESP32 / STM32):
//! - Polling `client.poll().await` inside MCU display tick loop and directly updating Slint properties.

use bytes::Bytes;
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
    // In a real Slint app:
    //   let ui = AppWindow::new()?;
    //   let weak_ui = ui.as_weak();
    //   let _telemetry_sub = mqtt.bind_slint_property("sensors/livingroom/temperature", QoS::AtLeastOnce, move |_topic, text| {
    //       let weak = weak_ui.clone();
    //       let _ = weak.upgrade_in_event_loop(move |ui| {
    //           ui.set_temperature_reading(text.into());
    //       });
    //   }).await?;

    let _telemetry_binding = mqtt
        .bind_slint_property(
            "sensors/livingroom/temperature",
            QoS::AtLeastOnce,
            |topic, value| {
                println!(" [Slint UI EventLoop] Update UI Property -> {topic}: {value}");
            },
        )
        .await?;

    // 3. Bind live security camera feed directly to Slint UI Image property
    // In a real Slint app:
    //   let weak_ui = ui.as_weak();
    //   let _camera_sub = mqtt.bind_slint_camera("security/camera/01/mjpeg", QoS::AtMostOnce, move |jpeg_bytes| {
    //       let weak = weak_ui.clone();
    //       let _ = weak.upgrade_in_event_loop(move |ui| {
    //           if let Ok(image) = slint::Image::load_from_svg_data(&jpeg_bytes) {
    //               ui.set_camera_frame(image);
    //           }
    //       });
    //   }).await?;

    let _camera_binding = mqtt
        .bind_slint_camera("security/camera/01/mjpeg", QoS::AtMostOnce, |frame_bytes| {
            println!(
                " [Slint UI EventLoop] Render new camera frame ({} bytes)",
                frame_bytes.len()
            );
        })
        .await?;

    // 4. Simulate edge publishers
    let pub_client = mqtt.clone();
    tokio::spawn(async move {
        let fake_jpeg =
            b"\xFF\xD8\xFF\xE0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xFF\xD9";
        for i in 1..=3 {
            let temp_str = format!("{}.5 C", 21 + i);
            let _ = pub_client
                .publish(
                    "sensors/livingroom/temperature",
                    QoS::AtMostOnce,
                    false,
                    temp_str,
                )
                .await;
            let _ = pub_client
                .publish(
                    "security/camera/01/mjpeg",
                    QoS::AtMostOnce,
                    false,
                    Bytes::from_static(fake_jpeg),
                )
                .await;
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    });

    tokio::time::sleep(Duration::from_millis(600)).await;
    println!("[Slint App] Slint UI bindings demonstrated successfully.");

    Ok(())
}
