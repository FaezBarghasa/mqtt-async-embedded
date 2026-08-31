# High-Performance Async Embedded & Standard MQTT Client (`mqtt-async-embedded`)

[![Crates.io](https://img.shields.io/crates/v/mqtt-async-embedded.svg)](https://crates.io/crates/mqtt-async-embedded)
[![Documentation](https://docs.rs/mqtt-async-embedded/badge.svg)](https://docs.rs/mqtt-async-embedded)
[![License: GPL-3.0-or-later](https://img.shields.io/badge/License-GPL_v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![CI](https://github.com/FaezBarghasa/mqtt-async-embedded/actions/workflows/ci.yml/badge.svg)](https://github.com/FaezBarghasa/mqtt-async-embedded/actions/workflows/ci.yml)
[![no_std](https://img.shields.io/badge/no__std-compatible-success.svg)](https://docs.rust-embedded.org/book/intro/no-std.html)

A next-generation, asynchronous MQTT client in Rust (2024 edition) offering dual operating modes:
1. **Embedded Engine (`no_std`, `no_alloc`)**: Zero heap allocations, fixed static buffers for bare-metal MCUs (ESP32, STM32, Cortex-M, RISC-V).
2. **Standard Tokio Client (`tokio-client`)**: Ultra-high throughput, cross-platform (Linux, Windows, Android), multi-threaded data streams, sliding journal data recovery, and MQTT over QUIC.

---

## Key Highlights

- **Embedded `no_std` Mode**:
  - Zero heap allocation (`no_alloc`), static compile-time array buffers (`heapless`).
  - Compatible with `esp-hal`, `esp-wifi`, `embassy-net`, and any `embedded-io-async` transport.
  - Zero-RAM chunked streaming (`begin_stream_publish`) for camera/audio sensors on MCUs with only 512B-2KB RAM.
- **Tokio / `std` Client Mode (`--features tokio-client`)**:
  - **Outperforms `rumqttc`**: Zero-copy packet pipeline via `bytes::Bytes`, multi-packet batch bursting (`publish_batch`), and direct topic-filtered stream routing (`subscribe_stream`).
  - **Cross-Platform OS Drivers**:
    - **Linux**: TCP (`TCP_NODELAY`), Pure-Rust TLS (`tokio-rustls`), QUIC (`quinn`), Unix Domain Sockets (`tokio::net::UnixStream`).
    - **Windows**: TCP, TLS, QUIC, and **Windows Named Pipes** (`pipe://\\.\pipe\mqtt_ipc`).
    - **Android**: TCP, TLS, QUIC, and **Android Abstract Namespace Sockets** (`unix://@android_mqtt_ipc`).
  - **Multi-Threaded Data Streams (`DataStreamProducer` / `DataStreamConsumer`)**:
    - Concurrent multi-worker ingestion with lock-free atomic sequence numbering and microsecond timestamps.
    - Out-of-order chunk reassembly window, duplicate suppression, and gap detection.
  - **Session Data Recovery Engine**:
    - In-flight QoS 1/2 message retransmission with `DUP = true`.
    - Automated topic subscription restoration on reconnect.
    - Offline queueing (`DropOldest`, `ErrorOnFull`, `Block`) to buffer telemetry while disconnected.
    - Sliding journal replay (`replay_recovery_journal`) for zero data loss.
  - **MQTT over QUIC & HTTP/3**:
    - Multiplexed streams eliminating TCP Head-of-Line blocking.
    - Sub-millisecond unreliable datagram telemetry (`client.publish_datagram()`).

---

## Quickstart

Add to `Cargo.toml`:
```toml
[dependencies]
# For embedded bare-metal MCUs (no_std)
mqtt-async-embedded = "1.3.0"

# OR for high-performance Tokio / Standard desktop, server, edge & mobile
mqtt-async-embedded = { version = "1.3.0", features = ["tokio-client", "tokio-tls", "tokio-quic"] }
```

---

## 1. High-Performance Tokio Client Example

```rust,no_run
use std::time::Duration;
use bytes::Bytes;
use mqtt_async_embedded::packet::QoS;
use mqtt_async_embedded::tokio_client::{
    Client, ClientOptions, DataRecoveryPolicy, DropStrategy, OfflineQueuePolicy,
    PublishMessage, ReconnectPolicy,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure client options with resilience & recovery policies
    let options = ClientOptions::from_uri("edge-node-01", "mqtt://127.0.0.1:1883")?
        .with_keep_alive(Duration::from_secs(30))
        .with_reconnect(ReconnectPolicy {
            enabled: true,
            initial_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(10),
            multiplier: 1.8,
            max_retries: None, // retry indefinitely
        })
        .with_offline_queue(OfflineQueuePolicy {
            capacity: 1000,
            drop_strategy: DropStrategy::DropOldest,
        })
        .with_recovery(DataRecoveryPolicy {
            resend_unacked_inflight: true,
            auto_resubscribe: true,
            max_inflight: 128,
        });

    // 2. Connect with auto-managed background Tokio driver task
    let (client, _handle) = Client::connect(options);

    // 3. Direct topic stream subscription with wildcard matching
    let mut temp_stream = client
        .subscribe_stream("sensors/+/temperature", QoS::AtLeastOnce)
        .await?;

    tokio::spawn(async move {
        while let Some(msg) = temp_stream.recv().await {
            println!("Received: {} -> {}", msg.topic, msg.payload_as_str().unwrap_or(""));
        }
    });

    // 4. High-throughput multi-packet batch publish (single syscall)
    let batch = vec![
        PublishMessage::new("sensors/bedroom/temperature", Bytes::from_static(b"21.5")),
        PublishMessage::new("sensors/kitchen/temperature", Bytes::from_static(b"23.8")),
    ];
    client.publish_batch(batch).await?;

    // 5. Multi-threaded data stream with automatic sliding journal recovery
    let producer = client.create_datastream_producer("telemetry/metrics", QoS::AtLeastOnce, 256);
    
    // Spawn across multiple worker threads concurrently
    for worker_id in 0..4 {
        let prod = producer.clone();
        tokio::spawn(async move {
            for i in 0..100 {
                let _ = prod.send(format!("worker-{worker_id}-metric-{i}").into_bytes()).await;
            }
        });
    }

    Ok(())
}
```

---

## 2. Embedded `no_std` Bare-Metal Example (ESP32, STM32, Embassy)

```rust,no_run
use embassy_time::Duration;
use mqtt_async_embedded::{
    MqttClient, MqttEvent, MqttOptions, MqttVersion, PublishMessage, QoS,
};

async fn run_mqtt<T: mqtt_async_embedded::MqttTransport>(transport: T) {
    let options = MqttOptions::new("embedded-sensor-node", "192.168.1.100", 1883)
        .with_version(MqttVersion::V5)
        .with_keep_alive(Duration::from_secs(30))
        .with_clean_session(true);

    // Fixed static buffers: MAX_TOPICS = 8, BUF_SIZE = 2048 bytes (0 heap allocs)
    let mut client: MqttClient<_, 8, 2048> = MqttClient::new(transport, options);

    client.connect().await.expect("Connect failed");
    client.subscribe(&[("sensors/commands/+", QoS::AtLeastOnce)]).await.expect("Sub failed");

    // Zero-RAM Chunk Stream Publish (e.g. DMA camera/audio stream)
    let mut stream = client
        .begin_stream_publish("sensors/camera/raw", 4096, QoS::AtMostOnce)
        .await
        .expect("Stream init failed");
    for chunk in camera_dma_chunks {
        stream.write_chunk(chunk).await.expect("Chunk write failed");
    }
    stream.finish().expect("Stream finish failed");

    // Event Loop
    loop {
        match client.poll().await {
            Ok(Some(MqttEvent::Publish(msg))) => defmt::info!("Topic: {}", msg.topic),
            Ok(Some(MqttEvent::PingResp)) => defmt::trace!("Heartbeat OK"),
            Ok(Some(MqttEvent::Disconnect(d))) => break,
            Ok(None) => {}
            Err(e) => break,
        }
    }
}
```

---

## Feature Flags Matrix

| Feature Flag | Description | Target Platforms |
| :--- | :--- | :--- |
| `std` | Standard library support | Linux, Windows, macOS, Android |
| `tokio-client` | Async standard client, multi-threaded data streams, recovery engine, topic router | Linux, Windows, Android |
| `tokio-tls` | Pure-Rust TLS support via `tokio-rustls` and WebPKI roots | Linux, Windows, Android |
| `tokio-quic` / `transport-quic` | MQTT over QUIC / H3 transport with multiplexed streams & datagrams (`quinn`) | Linux, Windows, Android |
| `transport-smoltcp` | Native bare-metal Ethernet/IP stack via `embassy-net` | Bare-Metal MCUs |
| `v5` | MQTT v5.0 User Properties and Enhanced Reason Codes | All Platforms |
| `defmt` | Zero-overhead binary logging for microcontrollers | Bare-Metal MCUs |

---

## Running Tests & Examples

```bash
# Run all unit and integration tests across embedded & tokio suites
cargo test --all-features

# Run Tokio basic pub/sub example
cargo run --example tokio_basic_pubsub --features tokio-client

# Run Tokio reconnection & recovery resilience example
cargo run --example tokio_reconnect_resilience --features tokio-client
```

---

## License

Licensed under GPL-3.0-or-later.