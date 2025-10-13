//! An example of running the MQTT client on a desktop using std::net::TcpStream.
//!
//! To run this, you need a local MQTT broker (like Mosquitto) running on port 1883.
//! `cargo run --example desktop_mock --features std`

#![allow(unused_imports)]

use embassy_executor::Spawner;
use embassy_net::tcp::TcpSocket;
use embassy_net::{Ipv4Address, Stack, StackResources};
use embassy_time::{Duration, Timer};
use log::*;
use mqtt_async_embedded::{MqttClient, MqttOptions, QoS};

// This requires a custom executor setup for std environment.
// For simplicity, we use a basic async runtime like `tokio`.
// This setup shows how the client can be adapted.

use mqtt_async_embedded::transport::{MqttTransport, TcpTransportError, TransportError};

// A mock transport using std::net for desktop testing.
struct StdTcpTransport {
    stream: std::net::TcpStream,
}

impl StdTcpTransport {
    fn new(addr: &str) -> std::io::Result<Self> {
        let stream = std::net::TcpStream::connect(addr)?;
        stream.set_nonblocking(true)?;
        Ok(Self { stream })
    }
}

// Dummy error for the mock transport
#[derive(Debug)]
struct StdTransportError(std::io::Error);
impl TransportError for StdTransportError {}

impl MqttTransport for StdTcpTransport {
    type Error = StdTransportError;

    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        use std::io::Write;
        self.stream.write_all(buf).map_err(StdTransportError)
    }

    async fn recv<'a>(&mut self, buf: &'a mut [u8]) -> Result<usize, Self::Error> {
        use std::io::Read;
        match self.stream.read(buf) {
            Ok(n) => Ok(n),
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // In a real async environment, we'd yield here.
                // We simulate this with a small sleep.
                Timer::after(Duration::from_millis(10)).await;
                Ok(0) // Indicate no data was read right now
            }
            Err(e) => Err(StdTransportError(e)),
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    info!("Starting desktop MQTT example...");

    // This address should point to your MQTT broker
    let broker_address = "127.0.0.1:1883";
    let transport = match StdTcpTransport::new(broker_address) {
        Ok(t) => t,
        Err(e) => {
            error!("Failed to connect to broker at {}: {}", broker_address, e);
            return;
        }
    };

    let options = MqttOptions::new("desktop_client_123");
    let mut client = MqttClient::new(transport, options);

    info!("Connecting to broker...");
    if let Err(e) = client.connect().await {
        error!("Failed to connect: {:?}", e);
        return;
    }
    info!("Connected!");

    info!("Publishing message...");
    let result = client
        .publish("sensors/temperature", b"27.5", QoS::AtLeastOnce)
        .await;

    if let Err(e) = result {
        error!("Failed to publish: {:?}", e);
    } else {
        info!("Published successfully!");
    }

    client.disconnect().await.unwrap();
    info!("Disconnected.");
}
