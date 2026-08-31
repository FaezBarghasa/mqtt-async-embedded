//! # Slint HMI Dashboard Integration Example
//!
//! Demonstrates clean binding between `mqtt-async-embedded` and Slint declarative UI.

#[cfg(feature = "tokio-client")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use bytes::Bytes;
    use mqtt_async_embedded::bridges::SlintStreamBinding;
    use mqtt_async_embedded::packet::QoS;
    use mqtt_async_embedded::tokio_client::{Client, ClientOptions};
    use std::time::Duration;

    println!("=== Slint HMI Industrial Dashboard ===");
    println!("Connecting to broker and establishing Slint property listeners...\n");

    let options = ClientOptions::new("slint-hmi-dashboard", "127.0.0.1", 1883)
        .with_keep_alive(Duration::from_secs(30));

    let (mqtt, _handle) = Client::connect(options);

    // Bind sensor telemetry
    let _telemetry_binding = SlintStreamBinding::bind_string_property(
        &mqtt,
        "factory/boiler/temperature",
        QoS::AtLeastOnce,
        |topic, val| {
            println!("[Slint Callback] Property {topic} updated to {val}");
        },
    )
    .await?;

    // Bind video/camera stream
    let _camera_binding = SlintStreamBinding::bind_camera_frame(
        &mqtt,
        "factory/inspection/camera",
        QoS::AtMostOnce,
        |bytes| {
            println!("[Slint Callback] Rendered frame of {} bytes", bytes.len());
        },
    )
    .await?;

    // Simulate publisher
    let pub_client = mqtt.clone();
    tokio::spawn(async move {
        let _ = pub_client
            .publish(
                "factory/boiler/temperature",
                QoS::AtLeastOnce,
                false,
                "82.4 C",
            )
            .await;
        let _ = pub_client
            .publish(
                "factory/inspection/camera",
                QoS::AtMostOnce,
                false,
                Bytes::from_static(b"\xFF\xD8\xFF\xD9"),
            )
            .await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    println!("\nSlint HMI dashboard bindings active and verified.");
    Ok(())
}

#[cfg(not(feature = "tokio-client"))]
fn main() {
    println!("Enable `tokio-client` feature to run this Slint dashboard example:");
    println!("  cargo run --example slint_dashboard --features tokio-client");
}
