//! # Tokio MQTT Client - Basic Publish and Subscribe Stream Demo
//!
//! Demonstrates:
//! - Managed background driver with `Client::connect()`
//! - Direct topic-filtered stream routing with wildcard (`home/+/temperature`)
//! - Multi-packet burst batch publishing
//! - Zero-copy payload sharing using `bytes::Bytes`

use bytes::Bytes;
use mqtt_async_embedded::packet::QoS;
use mqtt_async_embedded::tokio_client::{Client, ClientOptions, PublishMessage};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("============================================================");
    println!("       mqtt-async-embedded: High-Performance Tokio Client   ");
    println!("============================================================");

    // 1. Configure client options with reconnect policy and offline queueing
    let options = ClientOptions::new("tokio-demo-client-01", "127.0.0.1", 1883)
        .with_keep_alive(Duration::from_secs(30));

    // 2. Connect with auto-managed background Tokio driver task
    let (client, _driver_handle) = Client::connect(options);

    println!("[*] Client started. Subscribing to wildcard topic: 'home/+/temperature'...");

    // 3. Create a dedicated topic-filtered stream subscription
    let mut temp_stream = client
        .subscribe_stream("home/+/temperature", QoS::AtLeastOnce)
        .await?;

    // Spawn a subscriber task reading exclusively from this stream
    let sub_task = tokio::spawn(async move {
        println!("[Subscriber] Listening for incoming telemetry...");
        while let Some(msg) = temp_stream.recv().await {
            println!(
                " -> [Received] Topic: '{}' | Payload: '{}' | QoS: {:?}",
                msg.topic,
                msg.payload_as_str().unwrap_or("<binary>"),
                msg.qos
            );
        }
    });

    // 4. Publish individual messages
    println!("[Publisher] Sending individual messages...");
    client
        .publish("home/kitchen/temperature", QoS::AtMostOnce, false, "23.4 C")
        .await?;

    client
        .publish_with_ack(
            "home/livingroom/temperature",
            QoS::AtLeastOnce,
            false,
            "21.8 C",
        )
        .await?;

    // 5. Fast-path multi-packet burst publish
    println!("[Publisher] Sending multi-packet batch burst...");
    let batch = vec![
        PublishMessage::new("home/bedroom/temperature", Bytes::from_static(b"19.5 C")),
        PublishMessage::new("home/garage/temperature", Bytes::from_static(b"15.2 C")),
        PublishMessage::new("home/basement/temperature", Bytes::from_static(b"17.0 C")),
    ];

    let count = client.publish_batch(batch).await?;
    println!(
        "[Publisher] Batch sent successfully ({} messages in 1 syscall)",
        count
    );

    // Allow some time for messages to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // 6. Graceful shutdown
    client.disconnect().await?;
    let _ = sub_task.abort();
    println!("[*] Disconnected gracefully. Demo complete!");

    Ok(())
}
