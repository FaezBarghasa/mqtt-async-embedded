//! # Protocol Compliance & Mock Broker Test Suite
//!
//! Validates MQTT v3.1.1 and MQTT v5.0 protocol behaviors against a mock in-process broker harness.

#![cfg(feature = "std")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use mqtt_async_embedded::transport::{MqttTransport, TransportError};
use mqtt_async_embedded::{MqttClient, MqttOptions, MqttVersion, PublishMessage, QoS};

/// Dedicated transport adapter for Tokio `TcpStream` in tests.
pub struct TokioTcpTransport {
    stream: TcpStream,
}

impl TokioTcpTransport {
    pub fn new(stream: TcpStream) -> Self {
        Self { stream }
    }
}

/// Custom error type for tests wrapping std::io::Error.
#[derive(Debug)]
pub struct TestIoError(std::io::Error);

impl core::fmt::Display for TestIoError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Test IO error: {}", self.0)
    }
}

impl TransportError for TestIoError {}

impl MqttTransport for TokioTcpTransport {
    type Error = TestIoError;

    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.stream.write_all(buf).await.map_err(TestIoError)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.stream.read(buf).await.map_err(TestIoError)
    }
}

/// In-process mock MQTT broker simulator for protocol compliance tests.
pub struct MockMqttBroker {
    addr: std::net::SocketAddr,
    _shutdown_tx: mpsc::Sender<()>,
    _is_v5: Arc<AtomicBool>,
}

impl MockMqttBroker {
    pub async fn start(is_v5: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        let is_v5_flag = Arc::new(AtomicBool::new(is_v5));
        let flag_clone = is_v5_flag.clone();

        tokio::spawn(async move {
            tokio::select! {
                _ = shutdown_rx.recv() => {}
                _ = async {
                    while let Ok((mut socket, _)) = listener.accept().await {
                        let v5 = flag_clone.load(Ordering::SeqCst);
                        tokio::spawn(async move {
                            let mut buf = [0u8; 1024];
                            // 1. Read CONNECT packet
                            let n = socket.read(&mut buf).await.unwrap_or(0);
                            if n == 0 { return; }

                            // 2. Respond with CONNACK (0x20, remaining len 2 or 3)
                            if v5 {
                                // v5 CONNACK: 0x20, len=3, session_present=0, reason_code=0, properties_len=0
                                let connack = [0x20, 0x03, 0x00, 0x00, 0x00];
                                let _ = socket.write_all(&connack).await;
                            } else {
                                // v3.1.1 CONNACK: 0x20, len=2, session_present=0, return_code=0
                                let connack = [0x20, 0x02, 0x00, 0x00];
                                let _ = socket.write_all(&connack).await;
                            }

                            // 3. Process loop for PUBLISH / SUBSCRIBE / PINGREQ
                            loop {
                                let n = socket.read(&mut buf).await.unwrap_or(0);
                                if n == 0 { break; }
                                let packet_type = buf[0] >> 4;

                                match packet_type {
                                    3 => {
                                        // PUBLISH: if QoS 1, reply with PUBACK
                                        let qos = (buf[0] >> 1) & 0x03;
                                        if qos == 1 {
                                            // Extract packet ID from variable header after topic string
                                            let topic_len = ((buf[2] as usize) << 8) | (buf[3] as usize);
                                            let pid_offset = 4 + topic_len;
                                            if n >= pid_offset + 2 {
                                                let pid_msb = buf[pid_offset];
                                                let pid_lsb = buf[pid_offset + 1];
                                                let puback = [0x40, 0x02, pid_msb, pid_lsb];
                                                let _ = socket.write_all(&puback).await;
                                            }
                                        }
                                    }
                                    8 => {
                                        // SUBSCRIBE: reply with SUBACK
                                        let pid_msb = buf[2];
                                        let pid_lsb = buf[3];
                                        let suback = [0x90, 0x03, pid_msb, pid_lsb, 0x00]; // QoS 0 granted
                                        let _ = socket.write_all(&suback).await;
                                    }
                                    12 => {
                                        // PINGREQ: reply with PINGRESP
                                        let pingresp = [0xD0, 0x00];
                                        let _ = socket.write_all(&pingresp).await;
                                    }
                                    14 => {
                                        // DISCONNECT
                                        break;
                                    }
                                    _ => {}
                                }
                            }
                        });
                    }
                } => {}
            }
        });

        Self {
            addr,
            _shutdown_tx: shutdown_tx,
            _is_v5: is_v5_flag,
        }
    }

    pub fn port(&self) -> u16 {
        self.addr.port()
    }
}

#[tokio::test]
async fn test_protocol_compliance_v3_1_1_full_handshake_and_pubsub() {
    let broker = MockMqttBroker::start(false).await;
    let stream = TcpStream::connect(("127.0.0.1", broker.port()))
        .await
        .unwrap();
    let transport = TokioTcpTransport::new(stream);

    let options = MqttOptions::new("client-v311", "127.0.0.1", broker.port())
        .with_version(MqttVersion::V3_1_1)
        .with_clean_session(true);

    let mut client: MqttClient<_, 8, 1024> = MqttClient::new(transport, options);
    client.connect().await.expect("v3.1.1 handshake failed");

    // Publish QoS 0
    client
        .publish("telemetry/v311/sensor", b"data-qos0", QoS::AtMostOnce)
        .await
        .unwrap();

    // Publish QoS 1
    client
        .publish("telemetry/v311/alert", b"alert-qos1", QoS::AtLeastOnce)
        .await
        .unwrap();

    // Subscribe
    client
        .subscribe(&[("telemetry/v311/#", QoS::AtMostOnce)])
        .await
        .unwrap();

    // Disconnect
    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_protocol_compliance_v5_full_handshake_and_burst_publish() {
    let broker = MockMqttBroker::start(true).await;
    let stream = TcpStream::connect(("127.0.0.1", broker.port()))
        .await
        .unwrap();
    let transport = TokioTcpTransport::new(stream);

    let options = MqttOptions::new("client-v5", "127.0.0.1", broker.port())
        .with_version(MqttVersion::V5)
        .with_clean_session(true);

    let mut client: MqttClient<_, 8, 2048> = MqttClient::new(transport, options);
    client.connect().await.expect("v5.0 handshake failed");

    // Burst batch publish
    let messages = [
        PublishMessage::new("v5/temp", b"23.5", QoS::AtMostOnce),
        PublishMessage::new("v5/humidity", b"60.2", QoS::AtMostOnce),
        PublishMessage::new("v5/pressure", b"1013.25", QoS::AtMostOnce),
    ];
    let sent = client.publish_batch(&messages).await.unwrap();
    assert_eq!(sent, 3);

    // Disconnect
    client.disconnect().await.unwrap();
}
