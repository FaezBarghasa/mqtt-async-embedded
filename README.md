# High-Performance Async Embedded MQTT Client (`mqtt-async-embedded`)

[![Crates.io](https://img.shields.io/crates/v/mqtt-async-embedded.svg)](https://crates.io/crates/mqtt-async-embedded)
[![Documentation](https://docs.rs/mqtt-async-embedded/badge.svg)](https://docs.rs/mqtt-async-embedded)
[![License: GPL-3.0-or-later](https://img.shields.io/badge/License-GPL_v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![CI](https://github.com/FaezBarghasa/mqtt-async-embedded/actions/workflows/ci.yml/badge.svg)](https://github.com/FaezBarghasa/mqtt-async-embedded/actions/workflows/ci.yml)
[![no_std](https://img.shields.io/badge/no__std-compatible-success.svg)](https://docs.rust-embedded.org/book/intro/no-std.html)

An `async`, `no_std`-compatible MQTT client library in Rust (2024 edition), designed for embedded microcontrollers, edge gateways, and low-latency IoT systems with **zero dynamic heap allocations**, **multi-packet burst batching**, **Last Will and Testament (LWT)**, and **MQTT over QUIC / HTTP/3** support.

---

## Core Highlights

- **`no_std` & `no_alloc` by Default**: Zero dynamic heap allocations across all communication cycles using compile-time const generics and static `heapless` buffers.
- **High-Throughput Multi-Packet Burst**:
  - `publish_batch(&[PublishMessage])`: Packs multiple telemetry messages into a single network frame burst to minimize socket and hardware write overhead.
  - `poll_batch()`: Parses and yields all available incoming events in a single receive buffer using `RawPacketFrameIter` without dropping packets.
- **MQTT over QUIC / HTTP/3 (`MqttQuicTransport`)**:
  - Eliminates Head-of-Line (HoL) blocking via stream multiplexing.
  - Ultra-fast real-time sensor streaming via unreliable QUIC datagrams (`QuicMqttClient`).
  - Native 0-RTT connection resumption.
- **Protocol Completeness**:
  - Full wire codec support for MQTT **v3.1.1** and **v5.0** (User Properties, Reason Codes, Property lengths).
  - Explicit Quality of Service: QoS 0 (`AtMostOnce`) and QoS 1 (`AtLeastOnce`) with pipelined auto-ACKs (`PUBACK`). Runtime validation explicitly rejects unsupported QoS (`UnsupportedQoS`).
  - **Last Will and Testament (LWT)** with custom topics, payloads, QoS, and retain flags.
  - **`UNSUBSCRIBE` & `UNSUBACK`** support for dynamic topic unsubscriptions.
  - Robust broker packet recognition including `PUBREC`, `PUBREL`, and `PUBCOMP`.
- **Hardware & Transport Agnostic**: Decoupled via `MqttTransport` (TCP, UART modems, SPI) and `MqttQuicTransport` (cellular QUIC modems, `quinn`).
- **Fast-Path Vectored I/O**: `send_vectored` allows zero-copy header + payload transmission.
- **Memory & Panics Resilient**: Defensive bounds checking across all frame decoders and encoders with detailed `MqttError` reporting.

---

## Getting Started

Add `mqtt-async-embedded` to your `Cargo.toml`:

```toml
[dependencies]
mqtt-async-embedded = "1.2.0"
```

### Standard TCP / UART Embedded Client

```rust,no_run
use mqtt_async_embedded::{
    MqttClient, MqttOptions, MqttVersion, QoS, PublishMessage, MqttEvent,
};
use embassy_time::Duration;

async fn run_mqtt<T: mqtt_async_embedded::MqttTransport>(transport: T) {
    // 1. Configure options with client ID, broker endpoint, keep-alive, and optional LWT
    let options = MqttOptions::new("embedded-sensor-node", "192.168.1.100", 1883)
        .with_version(MqttVersion::V5)
        .with_keep_alive(Duration::from_secs(30))
        .with_clean_session(true)
        .with_will("devices/sensor-node/status", b"offline", QoS::AtLeastOnce, true);

    // 2. Instantiate client with const generics: MAX_TOPICS = 8, BUF_SIZE = 2048 bytes
    let mut client: MqttClient<_, 8, 2048> = MqttClient::new(transport, options);

    // 3. Connect to broker
    client.connect().await.expect("Failed to connect");

    // 4. Subscribe to topic filter
    client.subscribe(&[("sensors/commands/+", QoS::AtLeastOnce)]).await.expect("Subscribe failed");

    // 5. Single message publish (QoS 0 or 1)
    client.publish("sensors/temp", b"24.5", QoS::AtLeastOnce).await.expect("Publish failed");

    // 6. Multi-packet burst publish (highest throughput)
    let batch = [
        PublishMessage::new("sensors/temp", b"24.5", QoS::AtMostOnce),
        PublishMessage::new("sensors/humidity", b"60.2", QoS::AtMostOnce),
        PublishMessage::new("sensors/pressure", b"1013.25", QoS::AtMostOnce),
    ];
    let sent = client.publish_batch(&batch).await.expect("Batch publish failed");
    defmt::info!("Published {} burst messages in single frame", sent);

    // 7. Polling event loop
    loop {
        match client.poll().await {
            Ok(Some(MqttEvent::Publish(msg))) => {
                defmt::info!("Received message on topic: {}", msg.topic);
            }
            Ok(Some(MqttEvent::PubAck(ack))) => {
                defmt::info!("Received PubAck for packet ID: {}", ack.packet_id);
            }
            Ok(Some(MqttEvent::SubAck(suback))) => {
                defmt::info!("Received SubAck for packet ID: {}", suback.packet_id);
            }
            Ok(Some(MqttEvent::UnsubAck(unsuback))) => {
                defmt::info!("Received UnsubAck for packet ID: {}", unsuback.packet_id);
            }
            Ok(Some(MqttEvent::PingResp)) => {
                defmt::trace!("Heartbeat PINGRESP received");
            }
            Ok(Some(MqttEvent::Disconnect(disc))) => {
                defmt::warn!("Broker disconnected, reason: {}", disc.reason_code);
                break;
            }
            Ok(None) => {}
            Err(e) => {
                defmt::error!("MQTT Error: {:?}", e);
                break;
            }
        }
    }
}
```

---

## Feature Flags

| Feature | Description | Default |
| :--- | :--- | :---: |
| `default` | Zero-allocation, `no_std` pure embedded build. | **Yes** |
| `std` | Standard library support for desktop development, Tokio, and test mocks. | No |
| `v5` | Full MQTT v5.0 extended properties and user properties support. | No |
| `defmt` | Zero-overhead logging implementations for microcontrollers. | No |
| `transport-smoltcp` | Native integration with `embassy-net` TCP sockets. | No |
| `transport-quic` | High-throughput MQTT over QUIC / H3 transport for host and Linux edge systems via `quinn`. | No |

---

## Running Examples

### 1. Multi-Packet Burst Batching
Demonstrates packing multiple sensor messages into a single frame to minimize socket overhead:
```bash
cargo run --example multipacket_burst --features std
```

### 2. Desktop Mock Client
Demonstrates complete connect, subscribe, publish, unsubscribe, and disconnect lifecycle over TCP:
```bash
cargo run --example desktop_mock --features std
```

### 3. MQTT over QUIC Client
Demonstrates QUIC stream multiplexing and sub-millisecond datagram telemetry:
```bash
cargo run --example quic_client --features transport-quic
```

---

## Architecture & Documentation

For in-depth architectural overviews, binary wire formats, state machines, and design specifications, check the [`docs/`](./docs) directory:

- [**Application Flow & State Machines**](docs/appflow.md): Execution flow, keep-alive loops, and connection lifecycles.
- [**Wire Protocol & Binary Schemas**](docs/backend_schema.md): Packet structures, variable-byte integer encoding, and property tables.
- [**Design Brief**](docs/design_brief.md): Architectural philosophy, zero-copy safety models, and memory constraints.
- [**Product Requirements (PRD)**](docs/prd.md): Functional and non-functional requirements and ecosystem targets.
- [**Technical Design (TDD)**](docs/tdd.md): Subsystem designs, traits, and concurrency guarantees.

---

## License

This project is licensed under the **GNU General Public License v3.0 or later** ([GPL-3.0-or-later](LICENSE)).