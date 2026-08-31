# High-Performance Async Embedded & Standard MQTT Client (`mqtt-async-embedded`)

[![Crates.io](https://img.shields.io/crates/v/mqtt-async-embedded.svg)](https://crates.io/crates/mqtt-async-embedded)
[![Documentation](https://docs.rs/mqtt-async-embedded/badge.svg)](https://docs.rs/mqtt-async-embedded)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)
[![CI](https://github.com/FaezBarghasa/mqtt-async-embedded/actions/workflows/ci.yml/badge.svg)](https://github.com/FaezBarghasa/mqtt-async-embedded/actions/workflows/ci.yml)
[![no_std](https://img.shields.io/badge/no__std-compatible-success.svg)](https://docs.rust-embedded.org/book/intro/no-std.html)

**`mqtt-async-embedded`** is a modular, zero-allocation, asynchronous MQTT client written in pure Rust (2024 edition).

Designed to be the fastest, safest, and most flexible MQTT ecosystem in Rust, it spans from **bare-metal microcontrollers** (`no_std`, `no_alloc`) to **high-throughput cloud services, Web servers (Axum/Actix), and Slint UI applications**.

---

## Workspace Subcrates

| Crate | Target / Environment | Description |
| :--- | :--- | :--- |
| **[`mqtt-packet`](crates/mqtt-packet)** | `no_std`, `no_alloc` | Zero-allocation MQTT v3.1.1 & v5 encoder/decoder with proptest validation. |
| **[`mqtt-embedded`](crates/mqtt-embedded)** | `no_std`, `no_alloc` (Embassy) | Bare-metal MCU async client, bounded in-flight QoS 1 & 2, direct DMA streaming. |
| **[`mqtt-tokio`](crates/mqtt-tokio)** | `std` (Tokio Runtime) | High-throughput host client with offline queueing, topic routing & smart QUIC fallback. |
| **[`mqtt-bridges`](crates/mqtt-bridges)** | `std` (Tokio) | Web server MJPEG multipart & SSE stream formatters, and Slint UI bindings. |
| **[`mqtt-async-embedded`](crates/mqtt-async-embedded)** | Facade / Umbrella | Root facade crate providing 100% backward compatibility and feature toggles. |

---

## Comparison Matrix

| Feature | `mqtt-async-embedded` | `rumqttc` | `minimq` |
| :--- | :---: | :---: | :---: |
| **License** | **MIT / Apache-2.0** | Apache-2.0 | MIT / Apache-2.0 |
| **`no_std` / `no_alloc` Bare Metal** | ✅ Full Support | ❌ Requires heap | ✅ `no_alloc` only |
| **Embedded Async Runtime** | ✅ **Embassy Native** | ❌ (Tokio only) | ❌ Non-async poll |
| **MQTT v5 & v3.1.1** | ✅ Both | ✅ Both | ⚠️ v5 Only |
| **QoS 0, 1, and 2** | ✅ Full State Machine | ✅ Full | ⚠️ QoS 0 & 1 only |
| **Zero-RAM DMA Streaming** | ✅ `MqttStreamWriter` | ❌ | ❌ |
| **Multi-Packet Batching** | ✅ `publish_batch` / `poll_batch` | ❌ | ❌ |
| **MQTT over QUIC (HTTP/3)** | ✅ QUIC + Smart Fallback | ❌ | ❌ |
| **Web Bridges (Axum/Actix)** | ✅ Native MJPEG & SSE | ❌ | ❌ |
| **Slint UI Direct Binding** | ✅ Auto UI Binders | ❌ | ❌ |
| **Fuzz & Proptest Suite** | ✅ Built-in | ⚠️ Partial | ⚠️ Partial |

---

## Quickstart

### 1. Bare-Metal Microcontrollers (`no_std`, `no_alloc`)

Add to `Cargo.toml`:
```toml
[dependencies]
mqtt-embedded = "1.6.0"
# Or using the facade crate:
mqtt-async-embedded = { version = "1.6.0", default-features = false }
```

```rust,no_run
use embassy_time::Duration;
use mqtt_embedded::client::{MqttClient, MqttEvent, MqttOptions};
use mqtt_packet::QoS;
use mqtt_embedded::EmbeddedIoTransport;

#[embassy_executor::task]
async fn mqtt_task(socket: MyEspWifiSocket) {
    let options = MqttOptions::new("esp32-sensor", "192.168.1.1", 1883)
        .with_keep_alive(Duration::from_secs(30));

    let transport = EmbeddedIoTransport::new(socket);
    let mut client: MqttClient<_, 8, 1024> = MqttClient::new(transport, options);

    client.connect().await.expect("MQTT connection failed");

    // Publish telemetry with zero heap allocations
    client.publish("sensors/temperature", b"24.5", QoS::AtLeastOnce).await.unwrap();

    // Event loop poll
    loop {
        if let Some(event) = client.poll().await.unwrap() {
            match event {
                MqttEvent::Publish(pub_msg) => {
                    // Process message
                }
                _ => {}
            }
        }
    }
}
```

### 2. High-Performance Host Client (Tokio, Web Bridges, Slint UI)

Add to `Cargo.toml`:
```toml
[dependencies]
mqtt-tokio = "1.6.0"
mqtt-bridges = "1.6.0"
# Or using the facade:
mqtt-async-embedded = { version = "1.6.0", features = ["tokio-client"] }
```

```rust,no_run
use bytes::Bytes;
use std::time::Duration;
use mqtt_tokio::{Client, ClientOptions, ReconnectPolicy, OfflineQueuePolicy, DropStrategy};
use mqtt_packet::QoS;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = ClientOptions::new("host-gateway", "127.0.0.1", 1883)
        .with_keep_alive(Duration::from_secs(30))
        .with_reconnect(ReconnectPolicy {
            enabled: true,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            multiplier: 1.5,
            max_retries: None,
        })
        .with_offline_queue(OfflineQueuePolicy {
            capacity: 10_000,
            drop_strategy: DropStrategy::DropOldest,
        });

    let (client, _handle) = Client::connect(options);

    // Topic subscription stream router with wildcard matching
    let mut sub_stream = client.subscribe_stream("sensors/+/temperature", QoS::AtLeastOnce).await?;
    tokio::spawn(async move {
        while let Some(msg) = sub_stream.recv().await {
            println!("{}: {}", msg.topic, msg.payload_as_str().unwrap_or(""));
        }
    });

    // High-speed batch publishing
    client.publish(
        "sensors/livingroom/temperature",
        QoS::AtLeastOnce,
        false,
        "22.4",
    ).await?;

    Ok(())
}
```

---

## License

Dual-licensed under either of:
- **MIT License** ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
- **Apache License, Version 2.0** ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

at your option.