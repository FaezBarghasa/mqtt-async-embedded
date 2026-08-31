use mqtt_async_embedded::client::MqttEvent;
use mqtt_async_embedded::packet::EncodePacket;
use mqtt_async_embedded::transport::{
    MqttQuicRecvStream, MqttQuicSendStream, MqttQuicTransport, TransportError,
};
use mqtt_async_embedded::{MqttOptions, QuicMqttClient};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
struct MockQuicError;
impl TransportError for MockQuicError {}

struct MockSendStream;
impl MqttQuicSendStream for MockSendStream {
    type Error = MockQuicError;
    async fn write(&mut self, _buf: &[u8]) -> Result<(), Self::Error> {
        Ok(())
    }
    async fn finish(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct MockRecvStream;
impl MqttQuicRecvStream for MockRecvStream {
    type Error = MockQuicError;
    async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, Self::Error> {
        Ok(0)
    }
}

/// Simulated QUIC / HTTP3 transport with stream multiplexing & datagram queue.
#[derive(Clone)]
struct MockQuicTransport {
    datagram_in: Arc<Mutex<VecDeque<std::vec::Vec<u8>>>>,
    datagram_out: Arc<Mutex<VecDeque<std::vec::Vec<u8>>>>,
}

impl MockQuicTransport {
    fn new() -> Self {
        Self {
            datagram_in: Arc::new(Mutex::new(VecDeque::new())),
            datagram_out: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn queue_incoming_datagram(&self, data: &[u8]) {
        self.datagram_in.lock().unwrap().push_back(data.to_vec());
    }
}

impl MqttQuicTransport for MockQuicTransport {
    type Error = MockQuicError;
    type SendStream = MockSendStream;
    type RecvStream = MockRecvStream;

    async fn open_bi_stream(
        &mut self,
    ) -> Result<(Self::SendStream, Self::RecvStream), Self::Error> {
        Ok((MockSendStream, MockRecvStream))
    }

    async fn open_uni_stream(&mut self) -> Result<Self::SendStream, Self::Error> {
        Ok(MockSendStream)
    }

    async fn accept_bi_stream(
        &mut self,
    ) -> Result<(Self::SendStream, Self::RecvStream), Self::Error> {
        Ok((MockSendStream, MockRecvStream))
    }

    async fn send_datagram(&mut self, data: &[u8]) -> Result<(), Self::Error> {
        self.datagram_out.lock().unwrap().push_back(data.to_vec());
        Ok(())
    }

    async fn recv_datagram(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        let mut dgram_queue = self.datagram_in.lock().unwrap();
        if let Some(packet) = dgram_queue.pop_front() {
            let n = std::cmp::min(buf.len(), packet.len());
            buf[..n].copy_from_slice(&packet[..n]);
            Ok(n)
        } else {
            Ok(0)
        }
    }
}

#[tokio::main]
async fn main() {
    println!("============================================================");
    println!("   mqtt-async-embedded: MQTT over QUIC / H3 Client Demo    ");
    println!("============================================================");

    let transport = MockQuicTransport::new();
    let broker_sim = transport.clone();

    let options = MqttOptions::new("quic-edge-node-01", "mqtt.quic-broker.local", 14567);
    let mut quic_client: QuicMqttClient<_, 2048> = QuicMqttClient::new(transport, options);

    println!("1. Publishing real-time telemetry via unreliable QUIC datagrams...");
    println!("   (Bypasses TCP head-of-line blocking, 0 connection handshake latency)");

    quic_client
        .publish_datagram("sensor/imu/quaternion", b"[0.707, 0.0, 0.707, 0.0]")
        .await
        .expect("Datagram publish failed");
    println!("   -> Datagram published successfully!");

    println!("\n2. Receiving real-time datagram telemetry from peer...");
    // Simulate peer sending an incoming datagram
    let sample_pub = mqtt_async_embedded::packet::Publish::new(
        "sensor/gps/coordinates",
        b"{\"lat\": 37.7749, \"lon\": -122.4194}",
        mqtt_async_embedded::packet::QoS::AtMostOnce,
    );
    let mut encoded_buf = [0u8; 256];
    let len = sample_pub
        .encode(
            &mut encoded_buf,
            mqtt_async_embedded::client::MqttVersion::V5,
        )
        .unwrap();
    broker_sim.queue_incoming_datagram(&encoded_buf[..len]);

    match quic_client.recv_datagram().await {
        Ok(Some(MqttEvent::Publish(pub_msg))) => {
            let payload = std::str::from_utf8(pub_msg.payload).unwrap_or("<binary>");
            println!(
                "   -> Received Datagram on topic '{}': {}",
                pub_msg.topic, payload
            );
        }
        Ok(_) => println!("   -> No datagram available."),
        Err(e) => println!("   -> Error receiving datagram: {:?}", e),
    }

    println!("\n3. Demonstration complete!");
}
