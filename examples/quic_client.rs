use mqtt_async_embedded::{MqttOptions, QuicMqttClient};

#[tokio::main]
async fn main() {
    println!("=== MQTT over QUIC / H3 Client Example ===");
    println!("Demonstrating stream multiplexing and ultra-fast real-time datagram telemetry.");

    let _options = MqttOptions::new("quic-sensor-node", "mqtt.quic-broker.local", 14567);
    println!("Ready to connect over QUIC 0-RTT transport.");
}
