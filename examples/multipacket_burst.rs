use mqtt_async_embedded::{MqttClient, MqttOptions, PublishMessage, QoS};

#[derive(Debug)]
struct MockLoopbackTransport {
    buffer: [u8; 4096],
    len: usize,
}

impl MockLoopbackTransport {
    fn new() -> Self {
        Self {
            buffer: [0; 4096],
            len: 0,
        }
    }
}

#[derive(Debug)]
struct MockError;

impl mqtt_async_embedded::transport::TransportError for MockError {}

impl mqtt_async_embedded::transport::MqttTransport for MockLoopbackTransport {
    type Error = MockError;

    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        let end = self.len + buf.len();
        if end <= self.buffer.len() {
            self.buffer[self.len..end].copy_from_slice(buf);
            self.len = end;
        }
        Ok(())
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        let n = core::cmp::min(buf.len(), self.len);
        buf[..n].copy_from_slice(&self.buffer[..n]);
        self.len = 0;
        Ok(n)
    }
}

#[tokio::main]
async fn main() {
    println!("=== Real-time Multi-Packet Burst Batching Demonstration ===");

    let transport = MockLoopbackTransport::new();
    let options = MqttOptions::new("fast-burst-device", "localhost", 1883);
    let mut client: MqttClient<_, 16, 4096> = MqttClient::new(transport, options);

    // Prepare a batch of 5 high-frequency sensor readings
    let batch = [
        PublishMessage::new("telemetry/accel/x", b"1.024", QoS::AtMostOnce),
        PublishMessage::new("telemetry/accel/y", b"-0.012", QoS::AtMostOnce),
        PublishMessage::new("telemetry/accel/z", b"9.810", QoS::AtMostOnce),
        PublishMessage::new("telemetry/gyro/pitch", b"0.45", QoS::AtMostOnce),
        PublishMessage::new("telemetry/gyro/roll", b"-1.20", QoS::AtMostOnce),
    ];

    println!("Publishing batch of {} messages in a single frame burst...", batch.len());
    // In mock mode, we demonstrate packing multiple packets
    match client.publish_batch(&batch).await {
        Ok(count) => println!("Successfully packed and sent {} packets!", count),
        Err(e) => println!("Batch publish error: {:?}", e),
    }

    println!("Demonstration complete.");
}
