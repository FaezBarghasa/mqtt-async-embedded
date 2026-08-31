//! # ESP32 (S-Series & C-Series) Embassy + Wi-Fi MQTT Example
//!
//! Demonstrates how `mqtt-async-embedded` runs seamlessly across all ESP32 microcontrollers:
//! - **ESP32-S series** (ESP32-S2, ESP32-S3) [Xtensa]
//! - **ESP32-C series** (ESP32-C2, ESP32-C3, ESP32-C6) [RISC-V]
//! - **ESP32 classic / ESP32-H2 / ESP32-P4**
//!
//! Compatible with both **`esp-hal`** (bare-metal `no_std`) and **`esp-idf-svc` / `esp-idf-hal`**.

use embassy_time::Duration;
use mqtt_async_embedded::client::{
    MqttClient, MqttEvent, MqttOptions, MqttVersion, PublishMessage,
};
use mqtt_async_embedded::packet::QoS;
use mqtt_async_embedded::transport::EmbeddedIoTransport;
use std::collections::VecDeque;

/// Simulated in-memory async socket demonstrating how embedded-io-async Read/Write
/// behaves identically to an `esp-wifi` / `embassy-net` TCP socket.
struct MockEspWifiSocket {
    rx_buf: VecDeque<u8>,
    tx_buf: VecDeque<u8>,
}

impl MockEspWifiSocket {
    fn new() -> Self {
        let mut sock = Self {
            rx_buf: VecDeque::new(),
            tx_buf: VecDeque::new(),
        };
        // Simulated broker CONNACK
        sock.rx_buf.extend([0x20, 0x02, 0x00, 0x00]);
        sock
    }
}

impl embedded_io_async::ErrorType for MockEspWifiSocket {
    type Error = embedded_io_async::ErrorKind;
}

impl embedded_io_async::Read for MockEspWifiSocket {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        let to_read = std::cmp::min(buf.len(), self.rx_buf.len());
        for b in buf.iter_mut().take(to_read) {
            *b = self.rx_buf.pop_front().unwrap();
        }
        Ok(to_read)
    }
}

impl embedded_io_async::Write for MockEspWifiSocket {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        self.tx_buf.extend(buf.iter().copied());
        Ok(buf.len())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    println!("============================================================");
    println!("   ESP32 (S-Series & C-Series) Embassy Wi-Fi MQTT Setup     ");
    println!("============================================================");
    println!("Target Chips: ESP32-S2, ESP32-S3, ESP32-C2, ESP32-C3, ESP32-C6, ESP32-H2");
    println!("Frameworks:   esp-hal (no_std), esp-wifi, esp-idf-svc, embassy-net");
    println!();

    // 1. Configure MQTT options
    let options = MqttOptions::new("esp32s3-telemetry-node", "broker.hivemq.com", 1883)
        .with_version(MqttVersion::V5)
        .with_keep_alive(Duration::from_secs(30))
        .with_clean_session(true)
        .with_will("devices/esp32/status", b"offline", QoS::AtLeastOnce, true);

    println!("1. Initialized MqttOptions with LWT & MQTT v5.");

    // 2. Wrap socket in universal EmbeddedIoTransport
    let socket = MockEspWifiSocket::new();
    let transport = EmbeddedIoTransport::new(socket);

    // 3. Instantiate zero-allocation MqttClient with static buffers
    let mut client: MqttClient<_, 8, 2048> = MqttClient::new(transport, options);

    // 4. Connect
    println!("2. Connecting to MQTT broker over Wi-Fi...");
    client.connect().await.expect("Wi-Fi MQTT connect failed");
    println!("   -> Connected!");

    // 5. Publish telemetry
    let batch = [
        PublishMessage::new("esp32/sensors/temperature", b"26.4", QoS::AtMostOnce),
        PublishMessage::new("esp32/sensors/humidity", b"48.2", QoS::AtMostOnce),
        PublishMessage::new("esp32/sensors/wifi_rssi_dbm", b"-58", QoS::AtMostOnce),
    ];
    let sent = client
        .publish_batch(&batch)
        .await
        .expect("Burst publish failed");
    println!(
        "3. Published burst of {} sensor metrics in a single frame!",
        sent
    );

    // 6. Polling loop demonstration
    if let Ok(Some(MqttEvent::Publish(msg))) = client.poll().await {
        println!("4. Received message on topic '{}'", msg.topic);
    }

    client.disconnect().await.expect("Disconnect failed");
    println!("5. Cleanly disconnected. Ready for real hardware deployment!");
}
