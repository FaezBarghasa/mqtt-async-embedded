# High-Performance Async Embedded & Standard MQTT Client (`mqtt-async-embedded`)

[![Crates.io](https://img.shields.io/crates/v/mqtt-async-embedded.svg)](https://crates.io/crates/mqtt-async-embedded)
[![Documentation](https://docs.rs/mqtt-async-embedded/badge.svg)](https://docs.rs/mqtt-async-embedded)
[![License: GPL-3.0-or-later](https://img.shields.io/badge/License-GPL_v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![CI](https://github.com/FaezBarghasa/mqtt-async-embedded/actions/workflows/ci.yml/badge.svg)](https://github.com/FaezBarghasa/mqtt-async-embedded/actions/workflows/ci.yml)
[![no_std](https://img.shields.io/badge/no__std-compatible-success.svg)](https://docs.rust-embedded.org/book/intro/no-std.html)

Dual-mode, asynchronous MQTT client in Rust (2024 edition):
1. **Embedded Mode (`no_std`, `no_alloc`)**: Zero heap allocations, fixed static buffers for bare-metal MCUs (ESP32, STM32, Cortex-M, RISC-V).
2. **Standard Tokio Mode (`tokio-client`)**: High-throughput, multi-threaded data streams, sliding journal session recovery, and MQTT over QUIC for Linux, Windows, and Android.

---

## At a Glance

| Feature | Embedded (`no_std`) | Standard (`tokio-client`) |
| :--- | :--- | :--- |
| **Heap Allocation** | Zero (`no_alloc`) | `bytes::Bytes` zero-copy pipeline |
| **Buffer Management** | Compile-time fixed arrays (`heapless`) | Dynamic channel-driven queues |
| **Transports** | `embedded-io-async`, TCP, UART, QUIC | TCP (`TCP_NODELAY`), TLS, QUIC, Named Pipes, Unix Sockets |
| **High Throughput** | Batch polling, chunked stream publishing | Multi-packet burst publishing (`publish_batch`) |
| **Recovery** | Manual reconnect loop | Automatic reconnect, in-flight retransmission, subscription restore |
| **Routing** | Single event loop poll | Trie-based topic stream router (`subscribe_stream`) |

---

## Quickstart

Add to `Cargo.toml`:

```toml
[dependencies]
# Bare-metal MCUs (no_std, zero heap)
mqtt-async-embedded = "1.3.0"

# Standard host / edge / mobile (Tokio, TLS, QUIC)
mqtt-async-embedded = { version = "1.3.0", features = ["tokio-client", "tokio-tls", "tokio-quic"] }
```

---

## 1. Tokio Client Quickstart

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
    // 1. Configure client with reconnection and offline recovery
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

    // 2. Connect (spawns background EventLoop driver)
    let (client, _handle) = Client::connect(options);

    // 3. Subscribe to topic stream (wildcard matching via Trie)
    let mut temp_stream = client
        .subscribe_stream("sensors/+/temperature", QoS::AtLeastOnce)
        .await?;

    tokio::spawn(async move {
        while let Some(msg) = temp_stream.recv().await {
            println!("{}: {}", msg.topic, msg.payload_as_str().unwrap_or(""));
        }
    });

    // 4. Batch publish multiple messages in a single burst
    client.publish_batch(vec![
        PublishMessage::new("sensors/bedroom/temperature", Bytes::from_static(b"21.5")),
        PublishMessage::new("sensors/kitchen/temperature", Bytes::from_static(b"23.8")),
    ]).await?;

    // 5. Multi-threaded data stream with atomic sequence IDs and sliding recovery journal
    let producer = client.create_datastream_producer("telemetry/metrics", QoS::AtLeastOnce, 256);
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

## 2. Embedded `no_std` Quickstart

```rust,no_run
use embassy_time::Duration;
use mqtt_async_embedded::{
    MqttClient, MqttEvent, MqttOptions, MqttVersion, QoS,
};

async fn run_mqtt<T: mqtt_async_embedded::MqttTransport>(transport: T) {
    let options = MqttOptions::new("sensor-node", "192.168.1.100", 1883)
        .with_version(MqttVersion::V5)
        .with_keep_alive(Duration::from_secs(30))
        .with_clean_session(true);

    // Static buffer allocation: MAX_TOPICS = 8, BUF_SIZE = 2048 bytes (0 heap allocs)
    let mut client: MqttClient<_, 8, 2048> = MqttClient::new(transport, options);

    client.connect().await.expect("Connect failed");
    client.subscribe(&[("sensors/commands/+", QoS::AtLeastOnce)]).await.expect("Sub failed");

    // Zero-RAM chunk stream publish (direct DMA / sensor piping)
    let mut stream = client
        .begin_stream_publish("sensors/camera/raw", 4096, QoS::AtMostOnce)
        .await
        .expect("Stream init failed");
    
    for chunk in camera_chunks {
        stream.write_chunk(chunk).await.expect("Chunk write failed");
    }
    stream.finish().expect("Stream finish failed");

    // Polling event loop
    loop {
        match client.poll().await {
            Ok(Some(MqttEvent::Publish(msg))) => defmt::info!("Topic: {}", msg.topic),
            Ok(Some(MqttEvent::PingResp)) => defmt::trace!("Ping OK"),
            Ok(Some(MqttEvent::Disconnect(_))) | Err(_) => break,
            Ok(None) => {}
        }
    }
}
```

---

## Feature Flags

| Flag | Purpose | Target |
| :--- | :--- | :--- |
| `std` | Standard library support | Linux, Windows, macOS, Android |
| `tokio-client` | Standard async client, topic router, recovery engine, data streams | Linux, Windows, Android |
| `tokio-tls` | TLS support via `tokio-rustls` and WebPKI roots | Linux, Windows, Android |
| `tokio-quic` / `transport-quic` | QUIC transport with multiplexed streams & datagrams (`quinn`) | Linux, Windows, Android |
| `transport-smoltcp` | Bare-metal Ethernet/IP stack via `embassy-net` | Embedded MCUs |
| `v5` | MQTT v5.0 User Properties and Reason Codes | All Platforms |
| `defmt` | Zero-overhead binary logging | Embedded MCUs |

---

## Commands

```bash
# Run full test suite across embedded and Tokio targets
cargo test --all-features

# Run Tokio basic pub/sub example
cargo run --example tokio_basic_pubsub --features tokio-client

# Run Tokio reconnect & recovery resilience example
cargo run --example tokio_reconnect_resilience --features tokio-client
```

---

## License

GPL-3.0-or-later.