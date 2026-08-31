# High-Performance Async Embedded & Standard MQTT Client (`mqtt-async-embedded`)

[![Crates.io](https://img.shields.io/crates/v/mqtt-async-embedded.svg)](https://crates.io/crates/mqtt-async-embedded)
[![Documentation](https://docs.rs/mqtt-async-embedded/badge.svg)](https://docs.rs/mqtt-async-embedded)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)
[![CI](https://github.com/FaezBarghasa/mqtt-async-embedded/actions/workflows/ci.yml/badge.svg)](https://github.com/FaezBarghasa/mqtt-async-embedded/actions/workflows/ci.yml)
[![no_std](https://img.shields.io/badge/no__std-compatible-success.svg)](https://docs.rust-embedded.org/book/intro/no-std.html)
[![Safety: forbid(unsafe_code)](https://img.shields.io/badge/unsafe_code-forbidden-brightgreen.svg)](#security--correctness)

**`mqtt-async-embedded`** is a modular, zero-allocation, asynchronous MQTT client ecosystem written in pure Rust (2024 edition).

Designed to be the fastest, safest, and most versatile MQTT client in the Rust ecosystem, it spans from **bare-metal microcontrollers** (`no_std`, `no_alloc` on STM32, ESP32, RISC-V) to **edge gateways on alternative microkernels (Redox OS)** and **high-throughput cloud services, web streaming servers (Axum/Actix), and Slint UI applications**.

---

## Workspace Architecture

```
mqtt-async-embedded (Facade / Umbrella Crate)
├── mqtt-packet   : Pure no_std, no_alloc MQTT 3.1.1 & 5.0 codec engine
├── mqtt-embedded : no_std, no_alloc async state machine (Embassy / embedded-io-async)
├── mqtt-tokio    : High-throughput async client for Tokio, QUIC, and session recovery
└── mqtt-bridges  : Web server bridges (Axum SSE/MJPEG) and Slint UI binders
```

| Crate | Environment | Description |
| :--- | :--- | :--- |
| **[`mqtt-packet`](crates/mqtt-packet)** | `no_std`, `no_alloc` | Zero-allocation MQTT v3.1.1 & v5 encoder/decoder with proptest and fuzz validation. |
| **[`mqtt-embedded`](crates/mqtt-embedded)** | `no_std`, `no_alloc` (Embassy) | Bare-metal MCU async client, bounded in-flight QoS 1 & 2, direct DMA streaming. |
| **[`mqtt-tokio`](crates/mqtt-tokio)** | `std` (Tokio Runtime) | High-throughput host client with offline queueing, topic routing & smart QUIC fallback. |
| **[`mqtt-bridges`](crates/mqtt-bridges)** | `std` (Tokio) | Web server MJPEG multipart & SSE stream formatters, and Slint UI bindings. |
| **[`mqtt-async-embedded`](crates/mqtt-async-embedded)** | Facade / Umbrella | Root facade crate providing 100% backward compatibility and feature toggles. |

---

## Comprehensive Comparison Matrix

| Feature | `mqtt-async-embedded` | `rumqttc` | `minimq` | `paho-mqtt` (C FFI) |
| :--- | :---: | :---: | :---: | :---: |
| **License** | **MIT OR Apache-2.0** | Apache-2.0 | MIT / Apache-2.0 | EPL-2.0 / EDLv1 |
| **Safety Invariant** | **`#![forbid(unsafe_code)]`** | Uses `unsafe` deps | `#![deny(unsafe_code)]` | Heavy C FFI / Unsafe |
| **`no_std` / `no_alloc` Bare Metal** | ✅ Full Support | ❌ Requires heap | ✅ `no_alloc` only | ❌ Requires OS |
| **Embedded Async Runtime** | ✅ **Embassy Native** | ❌ (Tokio only) | ❌ Non-async poll | ❌ Non-async |
| **MQTT v5 & v3.1.1** | ✅ Both | ✅ Both | ⚠️ v5 Only | ✅ Both |
| **QoS 0, 1, and 2** | ✅ Full State Machine | ✅ Full | ⚠️ QoS 0 & 1 only | ✅ Full |
| **Zero-Copy DMA Streaming** | ✅ `begin_stream_publish` | ❌ | ❌ | ❌ |
| **Multi-Packet Batching** | ✅ `publish_batch` / `poll_batch` | ❌ | ❌ | ❌ |
| **MQTT over QUIC (HTTP/3)** | ✅ QUIC + Smart TCP Fallback | ❌ | ❌ | ❌ |
| **Redox OS Compatibility** | ✅ Verified (`x86_64-unknown-redox`) | ⚠️ Unverified | ⚠️ Unverified | ❌ |
| **Web Bridges (Axum/Actix)** | ✅ Native MJPEG & SSE | ❌ | ❌ | ❌ |
| **Slint UI Direct Binding** | ✅ Auto UI Binders | ❌ | ❌ | ❌ |
| **Fuzz & Proptest Suite** | ✅ Built-in (`libfuzzer-sys`) | ⚠️ Partial | ⚠️ Partial | ⚠️ External |

---

## Quickstart

### 1. Bare-Metal Microcontrollers (`no_std`, `no_alloc` for STM32 / ESP32 / RISC-V)

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
    // Bounded inflight queue = 4, Static Packet Buffer = 1024 bytes
    let mut client: MqttClient<_, 4, 1024> = MqttClient::new(transport, options);

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

### 2. Zero-Copy DMA Streaming on STM32

```rust,no_run
// Stream raw ADC circular buffer directly to network without intermediate memory copies
let adc_dma_buffer = [0xAAu8; 512];
let mut writer = client
    .begin_stream_publish("telemetry/stm32/adc", adc_dma_buffer.len(), QoS::AtLeastOnce)
    .await?;

writer.write_dma_slice(&adc_dma_buffer).await?;
writer.finish()?;
```

### 3. High-Performance Host Client (Tokio, Redox OS, QUIC, Slint UI)

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

    // High-speed publishing
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

## Example Gallery

Run any of the curated examples:

| Example | Command | Description |
| :--- | :--- | :--- |
| **STM32H7 DMA Streaming** | `cargo run --example stm32h7_embassy_mqtt` | Bare-metal ADC DMA stream publishing with Embassy. |
| **ESP32-C3 UART Modem** | `cargo run --example esp32c3_uart_mqtt` | RISC-V UART modem serial transport integration. |
| **Redox OS Daemon** | `cargo run --example redox_daemon --features tokio-client` | Background gateway daemon for Redox OS microkernel. |
| **Slint HMI Dashboard** | `cargo run --example slint_dashboard --features tokio-client` | Slint UI declarative property & camera stream binding. |
| **QUIC with Smart Fallback** | `cargo run --example quic_client --features transport-quic` | QUIC transport with automatic TCP/TLS fallback. |
| **Axum SSE & MJPEG Bridge**| `cargo run --example server_camera_web_bridge --features tokio-client` | Live browser camera stream via multipart MJPEG & SSE. |
| **Multi-Packet Burst** | `cargo run --example multipacket_burst --features std` | Multi-packet batched publish and event loop iteration. |

---

## Security & Correctness

1. **`#![forbid(unsafe_code)]`**: Strictly enforced across all crates in the workspace. Zero unsafe code.
2. **Parser Fuzzing**: Continuous fuzzing harness in [`fuzz/`](fuzz) using `libfuzzer-sys` across all MQTT 3.1.1 and 5.0 decoders.
3. **Property-Based Testing**: Exhaustive proptest suites verifying packet roundtrips, variable-byte integer encoding, and QoS control packets.

---

## Architecture Decision Records (ADRs)

Key architectural decisions are documented in [`docs/adr/`](docs/adr):
- **[ADR 0001](docs/adr/0001-workspace-modularization.md)**: Modular Cargo Workspace Architecture
- **[ADR 0002](docs/adr/0002-dual-licensing.md)**: MIT OR Apache-2.0 Dual Licensing
- **[ADR 0003](docs/adr/0003-zero-allocation-embedded-architecture.md)**: Zero-Allocation Embedded Architecture
- **[ADR 0004](docs/adr/0004-smart-transport-quic-fallback.md)**: Smart QUIC-to-TCP/TLS Fallback Strategy
- **[ADR 0005](docs/adr/0005-embedded-dma-streaming-and-tls.md)**: Zero-Copy DMA Streaming and Pluggable MCU TLS

---

## License

Dual-licensed under either of:
- **MIT License** ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
- **Apache License, Version 2.0** ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

at your option.