//! # Tokio MQTT Client - Reconnect Resilience & Data Recovery Demo
//!
//! Demonstrates:
//! - Auto-reconnection with configurable exponential backoff and jitter
//! - Offline queueing when network goes down
//! - Session data recovery: in-flight QoS 1/2 message resending (`DUP=true`)
//! - Automatic topic subscription restoration upon reconnect
//! - Connection state monitoring via `watch::Receiver<ConnectionStatus>`

use mqtt_async_embedded::packet::QoS;
use mqtt_async_embedded::tokio_client::{
    Client, ClientOptions, DataRecoveryPolicy, DropStrategy, OfflineQueuePolicy, ReconnectPolicy,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("============================================================");
    println!("   mqtt-async-embedded: Reconnection Resilience & Recovery  ");
    println!("============================================================");

    // 1. Configure robust resilience policies
    let options = ClientOptions::new("resilient-edge-node", "127.0.0.1", 1883)
        .with_keep_alive(Duration::from_secs(15))
        // Exponential backoff reconnect policy
        .with_reconnect(ReconnectPolicy {
            enabled: true,
            initial_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(10),
            multiplier: 1.8,
            max_retries: None, // retry indefinitely
        })
        // Buffer up to 1,000 messages offline during dropouts
        .with_offline_queue(OfflineQueuePolicy {
            capacity: 1000,
            drop_strategy: DropStrategy::DropOldest,
        })
        // Automatic session data recovery
        .with_recovery(DataRecoveryPolicy {
            resend_unacked_inflight: true,
            auto_resubscribe: true,
            max_inflight: 128,
        });

    // 2. Start client
    let (client, _handle) = Client::connect(options);

    // 3. Monitor connection status in background
    let mut status_rx = client.status();
    tokio::spawn(async move {
        while status_rx.changed().await.is_ok() {
            let status = *status_rx.borrow_and_update();
            println!("[Status Monitor] Connection state changed: {}", status);
        }
    });

    // 4. Subscribing to critical telemetry
    println!("[*] Subscribing to 'fleet/+/status'...");
    let mut fleet_stream = client
        .subscribe_stream("fleet/+/status", QoS::AtLeastOnce)
        .await?;

    tokio::spawn(async move {
        while let Some(msg) = fleet_stream.recv().await {
            println!(
                " -> [Fleet Stream] Topic: {} | Payload: {}",
                msg.topic,
                msg.payload_as_str().unwrap_or("")
            );
        }
    });

    // 5. Simulate periodic telemetry publishes
    println!(
        "[*] Publishing telemetry. (Even if broker is temporarily down, messages are queued offline)..."
    );
    for i in 1..=5 {
        let payload = format!("Vehicle telemetry heartbeat #{}", i);
        let _ = client
            .publish("fleet/truck-42/status", QoS::AtLeastOnce, false, payload)
            .await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;
    client.disconnect().await?;
    println!("[*] Disconnected. Reconnection demo complete!");

    Ok(())
}
