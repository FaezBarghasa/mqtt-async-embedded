//! Async Integration Tests for MqttClient with MockTransport

use embassy_time::Duration;
use heapless::Vec;
use mqtt_async_embedded::client::{
    ConnectionState, MqttClient, MqttEvent, MqttOptions, MqttVersion, PublishMessage,
};
use mqtt_async_embedded::error::{ConnectReasonCode, MqttError, ProtocolError};
use mqtt_async_embedded::packet::{self, EncodePacket, QoS};
use mqtt_async_embedded::transport::{MqttTransport, TransportError};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq, Eq)]
struct MockTransportError;
impl TransportError for MockTransportError {}

/// An in-memory mock transport for testing `MqttClient`.
#[derive(Clone)]
struct MockTransport {
    /// Bytes sent by the client to the transport.
    outgoing: Arc<Mutex<VecDeque<u8>>>,
    /// Bytes queued to be read by the client.
    incoming: Arc<Mutex<VecDeque<u8>>>,
}

impl MockTransport {
    fn new() -> Self {
        Self {
            outgoing: Arc::new(Mutex::new(VecDeque::new())),
            incoming: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Feeds bytes into the transport as if received from the broker.
    fn feed_incoming(&self, data: &[u8]) {
        let mut inc = self.incoming.lock().unwrap();
        inc.extend(data.iter().copied());
    }

    /// Reads and clears all outgoing bytes sent by the client.
    fn drain_outgoing(&self) -> std::vec::Vec<u8> {
        let mut out = self.outgoing.lock().unwrap();
        out.drain(..).collect()
    }
}

impl MqttTransport for MockTransport {
    type Error = MockTransportError;

    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        let mut out = self.outgoing.lock().unwrap();
        out.extend(buf.iter().copied());
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

#[tokio::test]
async fn test_client_connect_success() {
    let transport = MockTransport::new();
    // Pre-queue a successful CONNACK (0x20, remaining length 2, session_present 0, reason_code 0)
    transport.feed_incoming(&[0x20, 0x02, 0x00, 0x00]);

    let options = MqttOptions::new("client-1", "127.0.0.1", 1883)
        .with_version(MqttVersion::V3)
        .with_keep_alive(Duration::from_secs(60));
    let mut client: MqttClient<_, 8, 512> = MqttClient::new(transport.clone(), options);

    assert_eq!(client.state(), ConnectionState::Disconnected);
    client.connect().await.expect("Connect should succeed");
    assert_eq!(client.state(), ConnectionState::Connected);

    // Verify CONNECT packet was sent
    let sent = transport.drain_outgoing();
    assert!(!sent.is_empty());
    assert_eq!(sent[0] >> 4, 1); // Packet type 1 = CONNECT
}

#[tokio::test]
async fn test_client_connect_refused() {
    let transport = MockTransport::new();
    // Pre-queue a refused CONNACK (reason code 0x04: BadUserNameOrPassword)
    transport.feed_incoming(&[0x20, 0x02, 0x00, 0x04]);

    let options = MqttOptions::new("client-1", "127.0.0.1", 1883);
    let mut client: MqttClient<_, 8, 512> = MqttClient::new(transport.clone(), options);

    let err = client.connect().await.unwrap_err();
    match err {
        MqttError::ConnectionRefused(ConnectReasonCode::BadUserNameOrPassword) => {}
        other => panic!("Expected BadUserNameOrPassword, got {:?}", other),
    }
    assert_eq!(client.state(), ConnectionState::Disconnected);
}

#[tokio::test]
async fn test_client_publish_qos0_and_qos1() {
    let transport = MockTransport::new();
    transport.feed_incoming(&[0x20, 0x02, 0x00, 0x00]); // CONNACK

    let options = MqttOptions::new("client-1", "127.0.0.1", 1883);
    let mut client: MqttClient<_, 8, 512> = MqttClient::new(transport.clone(), options);
    client.connect().await.unwrap();
    transport.drain_outgoing(); // clear CONNECT

    // 1. Publish QoS 0
    client
        .publish("telemetry/temp", b"23.5", QoS::AtMostOnce)
        .await
        .unwrap();
    let sent_qos0 = transport.drain_outgoing();
    assert!(!sent_qos0.is_empty());
    assert_eq!(sent_qos0[0] >> 4, 3); // PUBLISH
    assert_eq!((sent_qos0[0] >> 1) & 0x03, 0); // QoS 0

    // 2. Publish QoS 1
    client
        .publish("telemetry/alert", b"HIGH", QoS::AtLeastOnce)
        .await
        .unwrap();
    let sent_qos1 = transport.drain_outgoing();
    assert!(!sent_qos1.is_empty());
    assert_eq!(sent_qos1[0] >> 4, 3); // PUBLISH
    assert_eq!((sent_qos1[0] >> 1) & 0x03, 1); // QoS 1

    // 3. Publish QoS 2 should be rejected
    let res = client
        .publish("telemetry/q2", b"data", QoS::ExactlyOnce)
        .await;
    assert_eq!(res, Err(MqttError::Protocol(ProtocolError::UnsupportedQoS)));
}

#[tokio::test]
async fn test_client_publish_batch_burst() {
    let transport = MockTransport::new();
    transport.feed_incoming(&[0x20, 0x02, 0x00, 0x00]); // CONNACK

    let options = MqttOptions::new("client-1", "127.0.0.1", 1883);
    let mut client: MqttClient<_, 8, 512> = MqttClient::new(transport.clone(), options);
    client.connect().await.unwrap();
    transport.drain_outgoing();

    let batch = [
        PublishMessage::new("sensors/temp", b"22.1", QoS::AtMostOnce),
        PublishMessage::new("sensors/hum", b"55.0", QoS::AtMostOnce),
        PublishMessage::new("sensors/press", b"1013.2", QoS::AtMostOnce),
    ];

    let count = client.publish_batch(&batch).await.unwrap();
    assert_eq!(count, 3);

    let sent = transport.drain_outgoing();
    // Parse all frames from the outgoing buffer using RawPacketFrameIter
    let iter = mqtt_async_embedded::util::RawPacketFrameIter::new(&sent);
    let frames: std::vec::Vec<&[u8]> = iter.map(|r| r.unwrap()).collect();
    assert_eq!(frames.len(), 3);
}

#[tokio::test]
async fn test_client_subscribe_and_unsubscribe() {
    let transport = MockTransport::new();
    transport.feed_incoming(&[0x20, 0x02, 0x00, 0x00]); // CONNACK

    let options = MqttOptions::new("client-1", "127.0.0.1", 1883);
    let mut client: MqttClient<_, 8, 512> = MqttClient::new(transport.clone(), options);
    client.connect().await.unwrap();
    transport.drain_outgoing();

    // 1. Subscribe
    let sub_pid = client
        .subscribe(&[("sensors/+", QoS::AtLeastOnce)])
        .await
        .unwrap();
    assert_eq!(sub_pid, 1);
    let sent_sub = transport.drain_outgoing();
    assert_eq!(sent_sub[0] >> 4, 8); // SUBSCRIBE

    // 2. Unsubscribe
    let unsub_pid = client.unsubscribe(&["sensors/+"]).await.unwrap();
    assert_eq!(unsub_pid, 2);
    let sent_unsub = transport.drain_outgoing();
    assert_eq!(sent_unsub[0] >> 4, 10); // UNSUBSCRIBE
}

#[tokio::test]
async fn test_client_poll_incoming_publish_and_auto_puback() {
    let transport = MockTransport::new();
    transport.feed_incoming(&[0x20, 0x02, 0x00, 0x00]); // CONNACK

    let options = MqttOptions::new("client-1", "127.0.0.1", 1883);
    let mut client: MqttClient<_, 8, 512> = MqttClient::new(transport.clone(), options);
    client.connect().await.unwrap();
    transport.drain_outgoing();

    // Broker sends a QoS 1 Publish message to client (packet ID = 42)
    let incoming_pub = packet::Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: false,
        topic: "commands/reboot",
        packet_id: Some(42),
        payload: b"now",
        properties: Vec::new(),
    };
    let mut pub_buf = [0u8; 128];
    let len = incoming_pub.encode(&mut pub_buf, MqttVersion::V3).unwrap();
    transport.feed_incoming(&pub_buf[..len]);

    // Poll the client
    let event = client.poll().await.unwrap().expect("Expected event");
    if let MqttEvent::Publish(pub_msg) = event {
        assert_eq!(pub_msg.topic, "commands/reboot");
        assert_eq!(pub_msg.payload, b"now");
        assert_eq!(pub_msg.packet_id, Some(42));
    } else {
        panic!("Expected MqttEvent::Publish");
    }

    // Check that client automatically transmitted a PUBACK with packet_id = 42
    let sent_ack = transport.drain_outgoing();
    assert!(!sent_ack.is_empty());
    assert_eq!(sent_ack[0] >> 4, 4); // PUBACK
    assert_eq!(u16::from_be_bytes([sent_ack[2], sent_ack[3]]), 42);
}

#[tokio::test]
async fn test_client_poll_batch_multi_event() {
    let transport = MockTransport::new();
    transport.feed_incoming(&[0x20, 0x02, 0x00, 0x00]); // CONNACK

    let options = MqttOptions::new("client-1", "127.0.0.1", 1883);
    let mut client: MqttClient<_, 8, 1024> = MqttClient::new(transport.clone(), options);
    client.connect().await.unwrap();
    transport.drain_outgoing();

    // Broker sends 2 Publish messages and 1 PingResp in the same receive buffer
    let p1 = packet::Publish::new("a/1", b"val1", QoS::AtMostOnce);
    let p2 = packet::Publish::new("a/2", b"val2", QoS::AtMostOnce);

    let mut stream = [0u8; 256];
    let mut cursor = 0;
    let l1 = p1.encode(&mut stream[cursor..], MqttVersion::V3).unwrap();
    cursor += l1;
    let l2 = p2.encode(&mut stream[cursor..], MqttVersion::V3).unwrap();
    cursor += l2;
    let ping_len = packet::PingResp
        .encode(&mut stream[cursor..], MqttVersion::V3)
        .unwrap();
    cursor += ping_len;

    transport.feed_incoming(&stream[..cursor]);

    let events: heapless::Vec<MqttEvent<'_>, 8> = client.poll_batch().await.unwrap();
    assert_eq!(events.len(), 3);
    match &events[0] {
        MqttEvent::Publish(p) => assert_eq!(p.topic, "a/1"),
        _ => panic!("Expected publish a/1"),
    }
    match &events[1] {
        MqttEvent::Publish(p) => assert_eq!(p.topic, "a/2"),
        _ => panic!("Expected publish a/2"),
    }
    match &events[2] {
        MqttEvent::PingResp => {}
        _ => panic!("Expected PingResp"),
    }
}

#[tokio::test]
async fn test_client_disconnect_lifecycle() {
    let transport = MockTransport::new();
    transport.feed_incoming(&[0x20, 0x02, 0x00, 0x00]); // CONNACK

    let options = MqttOptions::new("client-1", "127.0.0.1", 1883);
    let mut client: MqttClient<_, 8, 512> = MqttClient::new(transport.clone(), options);
    client.connect().await.unwrap();
    transport.drain_outgoing();

    client.disconnect().await.unwrap();
    assert_eq!(client.state(), ConnectionState::Disconnected);

    let sent_disc = transport.drain_outgoing();
    assert_eq!(sent_disc[0] >> 4, 14); // DISCONNECT

    // Calling publish when disconnected should fail immediately
    let res = client.publish("test", b"data", QoS::AtMostOnce).await;
    assert_eq!(res, Err(MqttError::NotConnected));
}
