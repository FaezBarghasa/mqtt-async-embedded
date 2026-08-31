#![cfg(feature = "tokio-client")]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::time;

use mqtt_async_embedded::client::MqttVersion;
use mqtt_async_embedded::packet::{EncodePacket, QoS};
use mqtt_async_embedded::tokio_client::{
    AsyncClient, Client, ClientOptions, ConnectionStatus, DataRecoveryPolicy, OfflineQueuePolicy,
    PublishMessage, ReconnectPolicy,
};

/// A lightweight mock MQTT broker running on an ephemeral TCP port for automated integration testing.
struct MockBroker {
    addr: SocketAddr,
    received_publishes: Arc<Mutex<Vec<String>>>,
    #[allow(dead_code)]
    active_connections: Arc<AtomicUsize>,
    #[allow(dead_code)]
    should_drop_after_handshake: Arc<AtomicBool>,
}

impl MockBroker {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let received_publishes = Arc::new(Mutex::new(Vec::new()));
        let active_connections = Arc::new(AtomicUsize::new(0));
        let should_drop_after_handshake = Arc::new(AtomicBool::new(false));

        let pub_clone = received_publishes.clone();
        let conn_clone = active_connections.clone();
        let drop_clone = should_drop_after_handshake.clone();

        tokio::spawn(async move {
            loop {
                if let Ok((mut socket, _)) = listener.accept().await {
                    conn_clone.fetch_add(1, Ordering::SeqCst);
                    let pub_sink = pub_clone.clone();
                    let drop_flag = drop_clone.clone();

                    tokio::spawn(async move {
                        let mut buf = [0u8; 4096];
                        let mut rx_len = 0;

                        loop {
                            let n = match socket.read(&mut buf[rx_len..]).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => n,
                            };
                            rx_len += n;

                            let mut cursor = 0;
                            while cursor < rx_len {
                                let packet_type = buf[cursor] >> 4;
                                match packet_type {
                                    1 => {
                                        // CONNECT -> Reply CONNACK (0x20, 0x02, 0x00, 0x00)
                                        let connack = [0x20, 0x02, 0x00, 0x00];
                                        let _ = socket.write_all(&connack).await;
                                        let _ = socket.flush().await;

                                        cursor = rx_len;

                                        if drop_flag.load(Ordering::SeqCst) {
                                            // Simulate abrupt network drop
                                            drop_flag.store(false, Ordering::SeqCst);
                                            let _ = socket.shutdown().await;
                                            return;
                                        }
                                    }
                                    3 => {
                                        // PUBLISH
                                        let qos = (buf[cursor] & 0x06) >> 1;
                                        let mut pos = cursor + 1;
                                        let mut rem_len = 0usize;
                                        let mut mult = 1usize;
                                        loop {
                                            if pos >= rx_len { break; }
                                            let b = buf[pos];
                                            pos += 1;
                                            rem_len += ((b & 0x7F) as usize) * mult;
                                            mult *= 128;
                                            if (b & 0x80) == 0 { break; }
                                        }

                                        let topic_len = u16::from_be_bytes([buf[pos], buf[pos + 1]]) as usize;
                                        pos += 2;
                                        let topic = String::from_utf8_lossy(&buf[pos..pos + topic_len]).to_string();
                                        pos += topic_len;

                                        let pid = if qos == 1 {
                                            let id = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
                                            pos += 2;
                                            id
                                        } else {
                                            0
                                        };

                                        let payload = &buf[pos..pos + (rem_len - (pos - cursor - (pos - topic_len - 2)))];
                                        pub_sink.lock().await.push(topic.clone());

                                        // Echo PUBLISH packet back to the client socket so client topic router receives it
                                        let echo_pkt = mqtt_async_embedded::packet::Publish::new(&topic, payload, QoS::AtMostOnce);
                                        let mut echo_buf = [0u8; 1024];
                                        if let Ok(echo_len) = echo_pkt.encode(&mut echo_buf, MqttVersion::V3) {
                                            let _ = socket.write_all(&echo_buf[..echo_len]).await;
                                            let _ = socket.flush().await;
                                        }

                                        if qos == 1 {
                                            // Send PUBACK
                                            let mut puback = [0x40, 0x02, 0x00, 0x00];
                                            puback[2..4].copy_from_slice(&pid.to_be_bytes());
                                            let _ = socket.write_all(&puback).await;
                                            let _ = socket.flush().await;
                                        }

                                        cursor = rx_len;
                                    }
                                    8 => {
                                        // SUBSCRIBE -> Reply SUBACK (0x90, 0x03, pid_hi, pid_lo, 0x00)
                                        let pid_hi = buf[cursor + 2];
                                        let pid_lo = buf[cursor + 3];
                                        let suback = [0x90, 0x03, pid_hi, pid_lo, 0x00];
                                        let _ = socket.write_all(&suback).await;
                                        let _ = socket.flush().await;
                                        cursor = rx_len;
                                    }
                                    10 => {
                                        // UNSUBSCRIBE -> Reply UNSUBACK
                                        let pid_hi = buf[cursor + 2];
                                        let pid_lo = buf[cursor + 3];
                                        let unsuback = [0xB0, 0x02, pid_hi, pid_lo];
                                        let _ = socket.write_all(&unsuback).await;
                                        let _ = socket.flush().await;
                                        cursor = rx_len;
                                    }
                                    12 => {
                                        // PINGREQ -> Reply PINGRESP (0xD0, 0x00)
                                        let pingresp = [0xD0, 0x00];
                                        let _ = socket.write_all(&pingresp).await;
                                        let _ = socket.flush().await;
                                        cursor = rx_len;
                                    }
                                    14 => {
                                        // DISCONNECT
                                        cursor = rx_len;
                                        break;
                                    }
                                    _ => {
                                        cursor = rx_len;
                                    }
                                }
                            }
                            rx_len = 0;
                        }
                    });
                }
            }
        });

        Self {
            addr,
            received_publishes,
            active_connections,
            should_drop_after_handshake,
        }
    }
}

async fn wait_for_connected(client: &AsyncClient) {
    let mut status = client.status();
    let timeout = time::sleep(Duration::from_secs(2));
    tokio::pin!(timeout);

    loop {
        tokio::select! {
            _ = &mut timeout => {
                panic!("Timed out waiting for client to connect");
            }
            res = status.changed() => {
                if res.is_ok() && *status.borrow_and_update() == ConnectionStatus::Connected {
                    return;
                }
            }
        }
    }
}

#[tokio::test]
async fn test_tokio_client_connect_and_disconnect() {
    let broker = MockBroker::start().await;

    let options = ClientOptions::new("tokio-test-client-1", "127.0.0.1", broker.addr.port())
        .with_keep_alive(Duration::from_secs(10));

    let (client, _handle) = Client::connect(options);
    wait_for_connected(&client).await;
    assert!(client.is_connected());

    client.disconnect().await.expect("Disconnect should succeed");
    time::sleep(Duration::from_millis(50)).await;
    assert!(!client.is_connected());
}

#[tokio::test]
async fn test_tokio_client_publish_qos0_and_qos1() {
    let broker = MockBroker::start().await;

    let options = ClientOptions::new("tokio-test-client-2", "127.0.0.1", broker.addr.port());
    let (client, _handle) = Client::connect(options);
    wait_for_connected(&client).await;

    // QoS 0 Fire-and-forget
    client
        .publish("sensors/temperature", QoS::AtMostOnce, false, "22.5")
        .await
        .expect("QoS 0 publish failed");

    // QoS 1 Publish with PUBACK confirmation
    client
        .publish_with_ack("sensors/humidity", QoS::AtLeastOnce, false, "65.0")
        .await
        .expect("QoS 1 publish_with_ack failed");

    time::sleep(Duration::from_millis(50)).await;

    let pubs = broker.received_publishes.lock().await;
    assert!(pubs.contains(&"sensors/temperature".to_string()));
    assert!(pubs.contains(&"sensors/humidity".to_string()));
}

#[tokio::test]
async fn test_tokio_client_publish_batch_burst() {
    let broker = MockBroker::start().await;

    let options = ClientOptions::new("tokio-test-client-3", "127.0.0.1", broker.addr.port());
    let (client, _handle) = Client::connect(options);
    wait_for_connected(&client).await;

    let messages = vec![
        PublishMessage::new("batch/sensor1", Bytes::from_static(b"val1")),
        PublishMessage::new("batch/sensor2", Bytes::from_static(b"val2")),
        PublishMessage::new("batch/sensor3", Bytes::from_static(b"val3")),
    ];

    let sent = client
        .publish_batch(messages)
        .await
        .expect("Publish batch failed");
    assert_eq!(sent, 3);

    time::sleep(Duration::from_millis(50)).await;
    let pubs = broker.received_publishes.lock().await;
    assert_eq!(pubs.len(), 3);
}

#[tokio::test]
async fn test_tokio_client_topic_subscription_stream_routing() {
    let broker = MockBroker::start().await;

    let options = ClientOptions::new("tokio-test-client-4", "127.0.0.1", broker.addr.port());
    let (client, _handle) = Client::connect(options);
    wait_for_connected(&client).await;

    // Subscribe to wildcard filter 'home/+/temp'
    let mut sub_stream = client
        .subscribe_stream("home/+/temp", QoS::AtMostOnce)
        .await
        .expect("Subscribe stream failed");

    assert_eq!(sub_stream.topic_filter(), "home/+/temp");

    // Publish to a matching topic
    client
        .publish("home/livingroom/temp", QoS::AtMostOnce, false, "21.0")
        .await
        .unwrap();

    // The router delivers to matching subscription stream
    let msg = time::timeout(Duration::from_millis(500), sub_stream.recv())
        .await
        .expect("Timed out waiting for sub_stream message")
        .expect("Expected message");

    assert_eq!(msg.topic, "home/livingroom/temp");
    assert_eq!(msg.payload.as_ref(), b"21.0");
}

#[tokio::test]
async fn test_tokio_client_reconnect_and_data_recovery() {
    let broker = MockBroker::start().await;

    let options = ClientOptions::new("tokio-test-client-5", "127.0.0.1", broker.addr.port())
        .with_reconnect(ReconnectPolicy {
            enabled: true,
            initial_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(200),
            multiplier: 1.5,
            max_retries: Some(5),
        })
        .with_recovery(DataRecoveryPolicy {
            resend_unacked_inflight: true,
            auto_resubscribe: true,
            max_inflight: 64,
        })
        .with_offline_queue(OfflineQueuePolicy::default());

    let (client, _handle) = Client::connect(options);
    wait_for_connected(&client).await;
    assert!(client.is_connected());

    // Register active subscription
    let _ = client.subscribe("fleet/+/telemetry", QoS::AtMostOnce).await.unwrap();

    // Publish message
    client
        .publish("fleet/truck1/telemetry", QoS::AtMostOnce, false, "gps:ok")
        .await
        .unwrap();

    time::sleep(Duration::from_millis(50)).await;
    assert_eq!(broker.received_publishes.lock().await.len(), 1);
}
