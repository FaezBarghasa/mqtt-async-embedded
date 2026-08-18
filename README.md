# High-Performance Async Embedded MQTT Client

An `async`, `no_std`-compatible MQTT client library in Rust (2024 edition), designed for embedded microcontrollers and edge gateways with **zero heap allocations**, **multi-packet burst batching**, and **MQTT over QUIC / HTTP/3** support.

---

## Core Features

- **`no_std` & `no_alloc` by Default**: Zero dynamic heap allocations across all communication cycles using `heapless` and compile-time const generics.
- **High-Throughput Multi-Packet Burst**:
  - `publish_batch(&[PublishMessage])`: Packs multiple telemetry messages into a single frame burst to minimize socket/hardware write overhead.
  - `poll_batch()`: Parses and yields all available incoming events in a single receive buffer without drops.
- **MQTT over QUIC / HTTP/3 (`MqttQuicTransport`)**:
  - Eliminates Head-of-Line (HoL) blocking via stream multiplexing.
  - Ultra-fast real-time sensor streaming via unreliable QUIC datagrams (`QuicMqttClient`).
  - Native 0-RTT connection resumption.
- **Hardware & Transport Agnostic**: Decoupled via `MqttTransport` (TCP, UART modems, SPI) and `MqttQuicTransport` (cellular QUIC modems, `quinn`).
- **Fast-Path Vectored I/O**: `send_vectored` allows zero-copy header + payload transmission.
- **MQTT v3.1.1 & v5.0**: Dynamic selection and full wire codec support for extended properties, reason codes, and user properties.
- **QoS 0 & 1 with Pipelined Auto-ACKs**: Automatic `PUBACK` generation during polling cycles.

---

## Getting Started

### Standard TCP / UART Client

```rust,no_run
use mqtt_async_embedded::{MqttClient, MqttOptions, QoS, PublishMessage};
use embassy_time::Duration;

async fn run_mqtt<T: mqtt_async_embedded::MqttTransport>(transport: T) {
    let options = MqttOptions::new("my-embedded-device", "192.168.1.100", 1883)
        .with_keep_alive(Duration::from_secs(30));

    // Const generics: MAX_TOPICS = 8, BUF_SIZE = 2048 bytes
    let mut client: MqttClient<_, 8, 2048> = MqttClient::new(transport, options);

    // Connect to broker
    client.connect().await.unwrap();

    // Subscribe to topic filter
    client.subscribe(&[("sensors/+", QoS::AtLeastOnce)]).await.unwrap();

    // Single message publish
    client.publish("sensors/temp", b"24.5", QoS::AtLeastOnce).await.unwrap();

    // Multi-packet burst publish (highest throughput)
    let batch = [
        PublishMessage::new("sensors/temp", b"24.5", QoS::AtMostOnce),
        PublishMessage::new("sensors/humidity", b"60.2", QoS::AtMostOnce),
        PublishMessage::new("sensors/pressure", b"1013.25", QoS::AtMostOnce),
    ];
    client.publish_batch(&batch).await.unwrap();

    // Poll event loop
    loop {
        if let Some(event) = client.poll().await.unwrap() {
            // Process incoming events
        }
    }
}
```

---

## Feature Flags

| Feature | Description |
| :--- | :--- |
| `default` | Standard `no_std` zero-allocation build. |
| `std` | Standard library support for desktop host testing and mocks. |
| `v5` | Full MQTT v5.0 extended properties and user properties support. |
| `defmt` | Zero-overhead logging via the `defmt` framework for microcontrollers. |
| `transport-smoltcp` | Direct integration with `embassy-net` TCP sockets. |
| `transport-quic` | MQTT over QUIC / H3 transport for host and Linux edge devices via `quinn`. |

---

## Running Examples

### 1. Multi-Packet Burst Batching
Demonstrates packing 5+ sensor readings into a single hardware frame:
```bash
cargo run --example multipacket_burst --features std
```

### 2. Desktop Mock Client
Connects and performs full pub/sub/disconnect lifecycle over TCP:
```bash
cargo run --example desktop_mock --features std
```

### 3. MQTT over QUIC Client
Demonstrates QUIC stream multiplexing and real-time datagram telemetry:
```bash
cargo run --example quic_client --features transport-quic
```