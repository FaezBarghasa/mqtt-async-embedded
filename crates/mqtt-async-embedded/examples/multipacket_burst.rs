use mqtt_async_embedded::client::MqttEvent;
use mqtt_async_embedded::packet::{self, EncodePacket, QoS};
use mqtt_async_embedded::{MqttClient, MqttOptions, MqttVersion, PublishMessage};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
struct MockBrokerTransport {
    incoming: Arc<Mutex<VecDeque<u8>>>,
    outgoing: Arc<Mutex<VecDeque<u8>>>,
}

impl MockBrokerTransport {
    fn new() -> Self {
        let transport = Self {
            incoming: Arc::new(Mutex::new(VecDeque::new())),
            outgoing: Arc::new(Mutex::new(VecDeque::new())),
        };
        // Pre-populate with CONNACK (session_present=false, reason_code=0)
        transport
            .incoming
            .lock()
            .unwrap()
            .extend([0x20, 0x02, 0x00, 0x00]);
        transport
    }

    fn queue_publish_burst(&self) {
        let p1 = packet::Publish::new("telemetry/accel/x", b"1.024", QoS::AtMostOnce);
        let p2 = packet::Publish::new("telemetry/accel/y", b"-0.012", QoS::AtMostOnce);
        let p3 = packet::Publish::new("telemetry/accel/z", b"9.810", QoS::AtMostOnce);

        let mut buf = [0u8; 512];
        let mut cursor = 0;
        cursor += p1.encode(&mut buf[cursor..], MqttVersion::V3).unwrap();
        cursor += p2.encode(&mut buf[cursor..], MqttVersion::V3).unwrap();
        cursor += p3.encode(&mut buf[cursor..], MqttVersion::V3).unwrap();

        self.incoming.lock().unwrap().extend(&buf[..cursor]);
    }
}

#[derive(Debug)]
struct MockError;
impl mqtt_async_embedded::transport::TransportError for MockError {}

impl mqtt_async_embedded::transport::MqttTransport for MockBrokerTransport {
    type Error = MockError;

    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.outgoing.lock().unwrap().extend(buf.iter().copied());
        Ok(())
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        let mut inc = self.incoming.lock().unwrap();
        let to_read = std::cmp::min(buf.len(), inc.len());
        for b in buf.iter_mut().take(to_read) {
            *b = inc.pop_front().unwrap();
        }
        Ok(to_read)
    }
}

#[tokio::main]
async fn main() {
    println!("============================================================");
    println!("   mqtt-async-embedded: Multi-Packet Burst Batching Demo    ");
    println!("============================================================");

    let transport = MockBrokerTransport::new();
    let broker_handle = transport.clone();

    let options = MqttOptions::new("fast-burst-device", "localhost", 1883);
    let mut client: MqttClient<_, 16, 4096> = MqttClient::new(transport, options);

    println!("1. Connecting to broker...");
    client.connect().await.expect("Connection failed");
    println!("   -> Connected successfully!");

    println!("\n2. Preparing 5 high-frequency sensor readings for burst publish...");
    let batch = [
        PublishMessage::new("telemetry/accel/x", b"1.024", QoS::AtMostOnce),
        PublishMessage::new("telemetry/accel/y", b"-0.012", QoS::AtMostOnce),
        PublishMessage::new("telemetry/accel/z", b"9.810", QoS::AtMostOnce),
        PublishMessage::new("telemetry/gyro/pitch", b"0.45", QoS::AtMostOnce),
        PublishMessage::new("telemetry/gyro/roll", b"-1.20", QoS::AtMostOnce),
    ];

    println!(
        "   Publishing batch of {} messages in a single frame...",
        batch.len()
    );
    let sent_count = client
        .publish_batch(&batch)
        .await
        .expect("Batch publish failed");
    println!(
        "   -> Successfully packed & transmitted {} packets in one frame!",
        sent_count
    );

    println!("\n3. Demonstrating zero-copy multi-packet burst polling (poll_batch)...");
    // Simulate broker delivering 3 telemetry publishes simultaneously to the client
    broker_handle.queue_publish_burst();

    {
        let events: heapless::Vec<MqttEvent<'_>, 8> =
            client.poll_batch().await.expect("Batch poll failed");
        println!(
            "   -> Received and parsed {} events in a single network read:",
            events.len()
        );

        for (idx, event) in events.iter().enumerate() {
            if let MqttEvent::Publish(pub_msg) = event {
                let payload_str = std::str::from_utf8(pub_msg.payload).unwrap_or("<invalid utf8>");
                println!(
                    "      [{}] Topic: {:<22} Payload: {}",
                    idx + 1,
                    pub_msg.topic,
                    payload_str
                );
            }
        }
    }

    println!("\n4. Disconnecting cleanly...");
    client.disconnect().await.expect("Disconnect failed");
    println!("   -> Disconnected. Demonstration complete!");
}
