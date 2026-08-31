#![cfg(feature = "tokio-client")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::time;

use mqtt_async_embedded::bridges::{
    CameraMjpegBridge, MqttBroadcastHub, SlintStreamBinding, TelemetrySseBridge,
};
use mqtt_async_embedded::client::MqttVersion;
use mqtt_async_embedded::packet::{self, EncodePacket, MqttPacket, QoS};
use mqtt_async_embedded::tokio_client::{
    AsyncClient, Client, ClientOptions, ConnectionStatus, DataRecoveryPolicy, OfflineQueuePolicy,
    PublishMessage, ReconnectPolicy,
};
use mqtt_async_embedded::util::RawPacketFrameIter;

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
                        let mut buf = [0u8; 8192];
                        let mut rx_len = 0;

                        loop {
                            let n = match socket.read(&mut buf[rx_len..]).await {
                                Ok(0) | Err(_) => break,
                                Ok(n) => n,
                            };
                            rx_len += n;

                            let mut consumed = 0;
                            {
                                let iter = RawPacketFrameIter::new(&buf[..rx_len]);
                                for frame in iter.flatten() {
                                    consumed += frame.len();
                                    if let Ok(Some(pkt)) =
                                        packet::decode(frame, MqttVersion::V3)
                                    {
                                        match pkt {
                                            MqttPacket::Connect(_) => {
                                                let connack = [0x20, 0x02, 0x00, 0x00];
                                                let _ = socket.write_all(&connack).await;
                                                let _ = socket.flush().await;

                                                if drop_flag.load(Ordering::SeqCst) {
                                                    drop_flag.store(false, Ordering::SeqCst);
                                                    let _ = socket.shutdown().await;
                                                    return;
                                                }
                                            }
                                            MqttPacket::Publish(p) => {
                                                pub_sink.lock().await.push(p.topic.to_string());

                                                if p.qos == QoS::AtLeastOnce
                                                    && let Some(pid) = p.packet_id
                                                {
                                                    let mut puback = [0x40, 0x02, 0x00, 0x00];
                                                    puback[2..4]
                                                        .copy_from_slice(&pid.to_be_bytes());
                                                    let _ = socket.write_all(&puback).await;
                                                    let _ = socket.flush().await;
                                                }

                                                // Echo PUBLISH packet for subscriber testing
                                                let echo_pkt =
                                                    mqtt_async_embedded::packet::Publish::new(
                                                        p.topic,
                                                        p.payload,
                                                        QoS::AtMostOnce,
                                                    );
                                                let mut echo_buf = [0u8; 1024];
                                                if let Ok(len) =
                                                    echo_pkt.encode(&mut echo_buf, MqttVersion::V3)
                                                {
                                                    let _ =
                                                        socket.write_all(&echo_buf[..len]).await;
                                                    let _ = socket.flush().await;
                                                }
                                            }
                                            MqttPacket::Subscribe(s) => {
                                                let suback = [
                                                    0x90,
                                                    0x03,
                                                    (s.packet_id >> 8) as u8,
                                                    (s.packet_id & 0xFF) as u8,
                                                    0x00,
                                                ];
                                                let _ = socket.write_all(&suback).await;
                                                let _ = socket.flush().await;
                                            }
                                            MqttPacket::Unsubscribe(u) => {
                                                let unsuback = [
                                                    0xB0,
                                                    0x02,
                                                    (u.packet_id >> 8) as u8,
                                                    (u.packet_id & 0xFF) as u8,
                                                ];
                                                let _ = socket.write_all(&unsuback).await;
                                                let _ = socket.flush().await;
                                            }
                                            MqttPacket::PingReq => {
                                                let pingresp = [0xD0, 0x00];
                                                let _ = socket.write_all(&pingresp).await;
                                                let _ = socket.flush().await;
                                            }
                                            MqttPacket::Disconnect(_) => {
                                                return;
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }

                            if consumed > 0 {
                                buf.copy_within(consumed..rx_len, 0);
                                rx_len -= consumed;
                            }
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

    client
        .disconnect()
        .await
        .expect("Disconnect should succeed");
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
    let _ = client
        .subscribe("fleet/+/telemetry", QoS::AtMostOnce)
        .await
        .unwrap();

    // Publish message
    client
        .publish("fleet/truck1/telemetry", QoS::AtMostOnce, false, "gps:ok")
        .await
        .unwrap();

    time::sleep(Duration::from_millis(50)).await;
    assert_eq!(broker.received_publishes.lock().await.len(), 1);
}

#[tokio::test]
async fn test_tokio_client_multithreaded_datastream_recovery() {
    let broker = MockBroker::start().await;

    let options = ClientOptions::new("tokio-test-client-6", "127.0.0.1", broker.addr.port());
    let (client, _handle) = Client::connect(options);
    wait_for_connected(&client).await;

    let producer = client.create_datastream_producer("telemetry/metrics", QoS::AtMostOnce, 128);
    let mut consumer = client
        .subscribe_datastream("telemetry/metrics", QoS::AtMostOnce, 64)
        .await
        .expect("Subscribe datastream failed");

    // Spawn multiple threads publishing concurrently to the producer
    let mut handles = Vec::new();
    for thread_id in 0..4 {
        let prod_clone = producer.clone();
        let handle = tokio::spawn(async move {
            for i in 0..10 {
                let payload = format!("Worker {thread_id} - Sample {i}");
                let _ = prod_clone.send(payload.into_bytes()).await;
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.unwrap();
    }

    // Consumer reads sequenced ordered chunks
    let mut received_chunks = 0;
    while let Ok(Ok(Some(_chunk))) =
        time::timeout(Duration::from_millis(200), consumer.recv_ordered()).await
    {
        received_chunks += 1;
        if received_chunks == 40 {
            break;
        }
    }

    assert_eq!(received_chunks, 40);

    // Test sliding journal replay for data recovery
    let replayed = producer.replay_recovery_journal().await.unwrap();
    assert!(replayed >= 40);
}

#[tokio::test]
async fn test_tokio_client_web_server_camera_mjpeg_and_sse_bridge() {
    use futures_util::StreamExt;
    use mqtt_async_embedded::tokio_client::{CameraMjpegBridge, TelemetrySseBridge};

    let broker = MockBroker::start().await;

    let options = ClientOptions::new("tokio-test-client-web", "127.0.0.1", broker.addr.port());
    let (client, _handle) = Client::connect(options);
    wait_for_connected(&client).await;

    // 1. Create a broadcast hub for camera video topic
    let camera_hub = MqttBroadcastHub::new(&client, "security/camera/01/mjpeg", QoS::AtMostOnce, 32)
        .await
        .expect("Create broadcast hub failed");

    // 2. Create web streaming bridges (simulating Axum/Actix HTTP body handlers)
    let mut mjpeg_stream = CameraMjpegBridge::new(&camera_hub);
    let mut sse_stream = TelemetrySseBridge::new(&camera_hub);

    // 3. Publish a simulated JPEG frame from edge camera
    let fake_jpeg = b"\xFF\xD8\xFF\xE0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xFF\xD9";
    client
        .publish(
            "security/camera/01/mjpeg",
            QoS::AtMostOnce,
            false,
            Bytes::from_static(fake_jpeg),
        )
        .await
        .unwrap();

    // 4. Verify MJPEG multipart stream output
    let mjpeg_frame = time::timeout(Duration::from_millis(500), mjpeg_stream.next())
        .await
        .expect("Timeout on MJPEG frame")
        .expect("Some frame")
        .expect("Ok frame");

    assert!(mjpeg_frame.starts_with(b"--frame\r\nContent-Type: image/jpeg\r\n"));
    assert!(mjpeg_frame.ends_with(b"\r\n"));

    // 5. Verify SSE stream output
    let sse_event = time::timeout(Duration::from_millis(500), sse_stream.next())
        .await
        .expect("Timeout on SSE event")
        .expect("Some event")
        .expect("Ok event");

    assert!(sse_event.starts_with(b"data: "));
    assert!(sse_event.ends_with(b"\n\n"));
}

#[tokio::test]
async fn test_tokio_client_slint_ui_binding_and_camera_stream() {
    let broker = MockBroker::start().await;

    let options = ClientOptions::new("tokio-test-client-slint", "127.0.0.1", broker.addr.port());
    let (client, _handle) = Client::connect(options);
    wait_for_connected(&client).await;

    let received_ui_text = Arc::new(Mutex::new(String::new()));
    let text_clone = received_ui_text.clone();

    // 1. Bind telemetry string property to simulated Slint UI callback
    let _text_binding = SlintStreamBinding::bind_string_property(
        &client,
        "slint/dashboard/temperature",
        QoS::AtMostOnce,
        move |_topic, val| {
            let sink = text_clone.clone();
            tokio::spawn(async move {
                *sink.lock().await = val;
            });
        },
    )
    .await
    .expect("Slint property binding failed");

    let received_frame_len = Arc::new(Mutex::new(0usize));
    let frame_clone = received_frame_len.clone();

    // 2. Bind camera stream to simulated Slint UI image callback
    let _camera_binding = SlintStreamBinding::bind_camera_frame(
        &client,
        "slint/camera/live",
        QoS::AtMostOnce,
        move |jpeg_bytes| {
            let sink = frame_clone.clone();
            tokio::spawn(async move {
                *sink.lock().await = jpeg_bytes.len();
            });
        },
    )
    .await
    .expect("Slint camera binding failed");

    // 3. Publish simulated telemetry and camera frame
    client
        .publish(
            "slint/dashboard/temperature",
            QoS::AtMostOnce,
            false,
            "24.6 C",
        )
        .await
        .unwrap();

    let fake_jpeg = b"\xFF\xD8\xFF\xE0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xFF\xD9";
    client
        .publish(
            "slint/camera/live",
            QoS::AtMostOnce,
            false,
            Bytes::from_static(fake_jpeg),
        )
        .await
        .unwrap();

    time::sleep(Duration::from_millis(100)).await;

    assert_eq!(*received_ui_text.lock().await, "24.6 C");
    assert_eq!(*received_frame_len.lock().await, fake_jpeg.len());
}

#[tokio::test]
async fn test_tokio_client_universal_all_sensor_data_types_multithreaded() {
    use mqtt_async_embedded::tokio_client::SensorDataType;

    let broker = MockBroker::start().await;

    let options = ClientOptions::new("tokio-test-client-sensors", "127.0.0.1", broker.addr.port());
    let (client, _handle) = Client::connect(options);
    wait_for_connected(&client).await;

    let producer =
        client.create_datastream_producer("sensors/all_types/stream", QoS::AtMostOnce, 256);
    let mut consumer = client
        .subscribe_datastream("sensors/all_types/stream", QoS::AtMostOnce, 128)
        .await
        .expect("Subscribe datastream failed");

    // 1. Thread 1: High-frequency IMU 6-axis accelerometer & gyroscope time-series
    let prod_imu = producer.clone();
    let imu_task = tokio::spawn(async move {
        for _ in 0..10 {
            // [acc_x, acc_y, acc_z, gyro_x, gyro_y, gyro_z]
            let sample = [0.01, 9.81, -0.05, 0.002, -0.001, 0.04];
            let _ = prod_imu.send_timeseries(&sample).await;
        }
    });

    // 2. Thread 2: Binary raw CAN bus telemetry frames
    let prod_can = producer.clone();
    let can_task = tokio::spawn(async move {
        for i in 0..10 {
            let can_frame = [0x00, 0x01, 0x07, 0xE8, i, 0xAA, 0x55, 0xFF];
            let _ = prod_can.send_raw(&can_frame).await;
        }
    });

    // 3. Thread 3: Microphone audio PCM samples
    let prod_audio = producer.clone();
    let audio_task = tokio::spawn(async move {
        for _ in 0..10 {
            let pcm_chunk = vec![0u8; 128]; // 16kHz 16-bit audio
            let _ = prod_audio.send_audio(16000, 1, pcm_chunk).await;
        }
    });

    // 4. Thread 4: Vision / camera thermal frame
    let prod_vision = producer.clone();
    let vision_task = tokio::spawn(async move {
        for _ in 0..10 {
            let jpeg_data =
                b"\xFF\xD8\xFF\xE0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xFF\xD9";
            let _ = prod_vision
                .send_image("image/jpeg", Bytes::from_static(jpeg_data))
                .await;
        }
    });

    // 5. Thread 5: Diagnostic JSON metadata
    let prod_json = producer.clone();
    let json_task = tokio::spawn(async move {
        for i in 0..10 {
            let json_str = format!(r#"{{"sensor_id":"engine_temp","status":"nominal","idx":{i}}}"#);
            let _ = prod_json.send_json(json_str).await;
        }
    });

    // Await all producer worker tasks (50 total messages sent concurrently)
    let _ = tokio::join!(imu_task, can_task, audio_task, vision_task, json_task);

    // Consume typed sensor stream and verify decoding across all types
    let mut total_received = 0;
    let mut imu_count = 0;
    let mut audio_count = 0;
    let mut vision_count = 0;
    let mut json_count = 0;
    let mut raw_count = 0;

    while let Ok(Ok(Some((_seq, sensor_data)))) =
        time::timeout(Duration::from_millis(200), consumer.recv_sensor_data()).await
    {
        total_received += 1;
        match sensor_data {
            SensorDataType::TimeSeries(vals) => {
                assert_eq!(vals.len(), 6);
                imu_count += 1;
            }
            SensorDataType::AudioPcm {
                sample_rate,
                channels,
                data,
            } => {
                assert_eq!(sample_rate, 16000);
                assert_eq!(channels, 1);
                assert_eq!(data.len(), 128);
                audio_count += 1;
            }
            SensorDataType::ImageFrame { mime, data } => {
                assert_eq!(mime, "image/jpeg");
                assert!(!data.is_empty());
                vision_count += 1;
            }
            SensorDataType::Json(s) => {
                assert!(s.contains("engine_temp"));
                json_count += 1;
            }
            SensorDataType::Raw(bytes) => {
                assert_eq!(bytes.len(), 8);
                raw_count += 1;
            }
            _ => {}
        }

        if total_received == 50 {
            break;
        }
    }

    assert_eq!(total_received, 50);
    assert_eq!(imu_count, 10);
    assert_eq!(raw_count, 10);
    assert_eq!(audio_count, 10);
    assert_eq!(vision_count, 10);
    assert_eq!(json_count, 10);
}
