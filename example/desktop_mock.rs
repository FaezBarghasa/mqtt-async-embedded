//! Host-based example using std::net::TcpStream.
//!
//! This example demonstrates how to run the MQTT client on a desktop machine.
//! It uses `tokio` to provide an async runtime and `std::net::TcpStream` for the transport.
//! A background task is spawned to handle the MQTT polling loop.
//!
//! You will need an MQTT broker running on localhost:1883.
//!
#![allow(unused_imports)]
#![allow(dead_code)]

use embassy_executor::Spawner;
use embassy_sync::blocking_mutex::raw::NoopRawMutex;
use embassy_sync::mutex::Mutex;
use embassy_time::{Duration, Timer};
use log::{error, info};
use mqtt_async_embedded::client::{MqttClient, MqttOptions};
use mqtt_async_embedded::packet::QoS;
use mqtt_async_embedded::transport::std_tcp::StdTcpTransport;
use static_cell::StaticCell;

// Type alias for the client to simplify signatures.
type Client<'a> = MqttClient<'a, StdTcpTransport, 1024, 1024>;

// Static cell for holding the MQTT client, wrapped in a Mutex for safe concurrent access.
static CLIENT: StaticCell<Mutex<NoopRawMutex, Client>> = StaticCell::new();

/// The main application task.
#[embassy_executor::main]
async fn main(spawner: Spawner) {
    // Basic logging setup.
    env_logger::builder()
        .filter(None, log::LevelFilter::Info)
        .init();

    // Spawn the MQTT polling task to run in the background.
    spawner.spawn(mqtt_poll_task()).unwrap();

    // Initialize the TCP transport and connect to the broker.
    let transport = StdTcpTransport::new("localhost:1883")
        .await
        .expect("Failed to connect to broker");

    // Configure the MQTT client.
    let options = MqttOptions::new("desktop-client-123");
    let client = MqttClient::new(transport, options);

    // Initialize the static client instance.
    let client = CLIENT.init(Mutex::new(client));

    info!("Connecting to MQTT broker...");
    {
        // Lock the client to perform the connect operation.
        let mut client_guard = client.lock().await;
        client_guard.connect().await.unwrap();
    }
    info!("Connected!");

    info!("Subscribing to 'test/topic'...");
    {
        let mut client_guard = client.lock().await;
        client_guard
            .subscribe("test/topic", QoS::AtMostOnce)
            .await
            .unwrap();
    }
    info!("Subscribed!");

    // Main loop to publish messages periodically.
    let mut count = 0;
    loop {
        let msg = format!("Hello from desktop! Count: {}", count);
        info!("Publishing: '{}'", &msg);
        {
            let mut client_guard = client.lock().await;
            client_guard
                .publish("test/topic", msg.as_bytes(), QoS::AtMostOnce)
                .await
                .unwrap();
        }
        count += 1;
        Timer::after(Duration::from_secs(5)).await;
    }
}

/// The background task for polling the MQTT client.
///
/// This task runs in a continuous loop, calling the client's `poll` method.
/// This is essential for handling keep-alives and processing incoming messages.
#[embassy_executor::task]
async fn mqtt_poll_task() {
    let client = CLIENT.get();
    loop {
        // Lock the client to perform the poll operation.
        let mut client_guard = client.lock().await;

        // The `poll` method handles incoming packets and sends keep-alive pings.
        match client_guard.poll().await {
            Ok(Some(packet)) => {
                info!("Received packet: {:?}", packet);
            }
            Ok(None) => {
                // No packet received, everything is normal.
            }
            Err(e) => {
                error!("MQTT poll error: {:?}", e);
                // In a real application, you might want to handle reconnection here.
                Timer::after(Duration::from_secs(1)).await;
            }
        }
    }
}

