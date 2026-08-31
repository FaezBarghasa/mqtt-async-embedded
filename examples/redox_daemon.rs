//! # Redox OS Microkernel Gateway Daemon Example
//!
//! Demonstrates running a background MQTT gateway daemon compiled natively
//! for Redox OS (`x86_64-unknown-redox`) using `mqtt-async-embedded` / `mqtt-tokio`.

#[cfg(feature = "tokio-client")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use bytes::Bytes;
    use mqtt_async_embedded::packet::QoS;
    use mqtt_async_embedded::tokio_client::{
        Client, ClientOptions, DropStrategy, OfflineQueuePolicy,
    };
    use std::time::Duration;

    println!("=== Redox OS Microkernel Edge Gateway Daemon ===");
    println!("Operating System Target: Redox OS (x86_64-unknown-redox)");
    println!("Session Resilience: Offline queueing + Automatic reconnect\n");

    let options = ClientOptions::new("redox-edge-daemon", "127.0.0.1", 1883)
        .with_keep_alive(Duration::from_secs(30))
        .with_offline_queue(OfflineQueuePolicy {
            capacity: 1024,
            drop_strategy: DropStrategy::DropOldest,
        });

    let (client, _handle) = Client::connect(options);

    println!("Publishing gateway heartbeat telemetry on Redox OS...");
    let payload =
        Bytes::from_static(b"{\"os\":\"redox\",\"kernel\":\"microkernel\",\"uptime_sec\":1204}");
    client
        .publish("gateway/redox/heartbeat", QoS::AtLeastOnce, false, payload)
        .await?;

    println!("Redox daemon operational and connected successfully.");
    Ok(())
}

#[cfg(not(feature = "tokio-client"))]
fn main() {
    println!("Enable `tokio-client` feature to run this Redox OS daemon example:");
    println!("  cargo run --example redox_daemon --features tokio-client");
}
