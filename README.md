# High-Performance Async Embedded MQTT Client (`mqtt-async-embedded`)

[![Crates.io](https://img.shields.io/crates/v/mqtt-async-embedded.svg)](https://crates.io/crates/mqtt-async-embedded)
[![Documentation](https://docs.rs/mqtt-async-embedded/badge.svg)](https://docs.rs/mqtt-async-embedded)
[![License: GPL-3.0-or-later](https://img.shields.io/badge/License-GPL_v3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![CI](https://github.com/FaezBarghasa/mqtt-async-embedded/actions/workflows/ci.yml/badge.svg)](https://github.com/FaezBarghasa/mqtt-async-embedded/actions/workflows/ci.yml)
[![no_std](https://img.shields.io/badge/no__std-compatible-success.svg)](https://docs.rust-embedded.org/book/intro/no-std.html)

Async, `no_std` MQTT client in Rust (2024 edition) for MCUs, edge gateways, and low-latency IoT.

## Key Facts
- **Memory**: Zero heap allocations (`no_std`, `no_alloc`). Uses static `heapless` buffers and const generics.
- **Protocols**: MQTT v3.1.1, MQTT v5.0, and MQTT over QUIC / HTTP/3.
- **QoS**: QoS 0 (`AtMostOnce`), QoS 1 (`AtLeastOnce`) with auto-`PUBACK`. Rejects QoS 2 with `UnsupportedQoS`.
- **Transports**: Abstracted via `MqttTransport` (TCP, UART modems, SPI) and `MqttQuicTransport`. Universal `embedded-io-async` adapters included.
- **High-Throughput Features**:
  - **Burst Publish (`publish_batch`)**: Packs multiple messages into one frame.
  - **Burst Poll (`poll_batch`)**: Drains all available packets from RX buffer in one pass.
  - **Chunk Streaming (`begin_stream_publish`)**: Streams large payloads (audio, camera stills) chunk-by-chunk with zero intermediate RAM buffers.
  - **QUIC Datagrams**: Unreliable sub-millisecond sensor streaming without Head-of-Line blocking.

---

## Quickstart

Add to `Cargo.toml`:
```toml
[dependencies]
mqtt-async-embedded = "1.2.0"
```

### Standard Client Example

```rust,no_run
use embassy_time::Duration;
use mqtt_async_embedded::{
    MqttClient, MqttEvent, MqttOptions, MqttVersion, PublishMessage, QoS,
};

async fn run_mqtt<T: mqtt_async_embedded::MqttTransport>(transport: T) {
    // 1. Config
    let options = MqttOptions::new("embedded-sensor-node", "192.168.1.100", 1883)
        .with_version(MqttVersion::V5)
        .with_keep_alive(Duration::from_secs(30))
        .with_clean_session(true)
        .with_will("devices/sensor-node/status", b"offline", QoS::AtLeastOnce, true);

    // 2. Init (MAX_TOPICS = 8, BUF_SIZE = 2048 bytes)
    let mut client: MqttClient<_, 8, 2048> = MqttClient::new(transport, options);

    // 3. Connect & Subscribe
    client.connect().await.expect("Connect failed");
    client.subscribe(&[("sensors/commands/+", QoS::AtLeastOnce)]).await.expect("Sub failed");

    // 4. Single Publish
    client.publish("sensors/temp", b"24.5", QoS::AtLeastOnce).await.expect("Pub failed");

    // 5. Batch Burst Publish
    let batch = [
        PublishMessage::new("sensors/temp", b"24.5", QoS::AtMostOnce),
        PublishMessage::new("sensors/humidity", b"60.2", QoS::AtMostOnce),
    ];
    client.publish_batch(&batch).await.expect("Batch failed");

    // 6. Zero-RAM Chunk Stream Publish
    let mut stream = client
        .begin_stream_publish("sensors/audio/pcm", 4096, QoS::AtMostOnce)
        .await
        .expect("Stream init failed");
    for chunk in audio_dma_chunks {
        stream.write_chunk(chunk).await.expect("Chunk write failed");
    }
    stream.finish().expect("Stream finish failed");

    // 7. Event Loop
    loop {
        match client.poll().await {
            Ok(Some(MqttEvent::Publish(msg))) => defmt::info!("Topic: {}", msg.topic),
            Ok(Some(MqttEvent::PubAck(ack))) => defmt::info!("PubAck: {}", ack.packet_id),
            Ok(Some(MqttEvent::SubAck(sub))) => defmt::info!("SubAck: {}", sub.packet_id),
            Ok(Some(MqttEvent::UnsubAck(unsub))) => defmt::info!("UnsubAck: {}", unsub.packet_id),
            Ok(Some(MqttEvent::PingResp)) => defmt::trace!("Heartbeat OK"),
            Ok(Some(MqttEvent::Disconnect(d))) => break defmt::warn!("Disconnect: {}", d.reason_code),
            Ok(None) => {}
            Err(e) => break defmt::error!("MQTT Error: {:?}", e),
        }
    }
}
```

---

## ESP32 Native Setup (`esp-hal` + `esp-wifi` / `embassy-net`)

Supports ESP32-S series (S2, S3) and ESP32-C series (C2, C3, C6, H2).

```rust,no_run
use embassy_time::Duration;
use mqtt_async_embedded::{
    EmbeddedIoTransport, MqttClient, MqttOptions, MqttVersion, PublishMessage, QoS,
};

async fn mqtt_task(stack: embassy_net::Stack<'static>) {
    let mut rx_buf = [0u8; 1536];
    let mut tx_buf = [0u8; 1536];
    let mut socket = embassy_net::tcp::TcpSocket::new(stack, &mut rx_buf, &mut tx_buf);
    
    socket.connect((embassy_net::Ipv4Address::new(192, 168, 1, 10), 1883)).await.unwrap();

    // Wrap socket in zero-alloc transport adapter
    let transport = EmbeddedIoTransport::new(socket);

    let options = MqttOptions::new("esp32-node", "192.168.1.10", 1883)
        .with_version(MqttVersion::V5)
        .with_keep_alive(Duration::from_secs(30))
        .with_will("devices/esp32/status", b"offline", QoS::AtLeastOnce, true);

    let mut client: MqttClient<_, 8, 2048> = MqttClient::new(transport, options);
    client.connect().await.unwrap();
    client.subscribe(&[("esp32/commands/+", QoS::AtLeastOnce)]).await.unwrap();

    let batch = [
        PublishMessage::new("esp32/temp", b"24.8", QoS::AtMostOnce),
        PublishMessage::new("esp32/humidity", b"52.1", QoS::AtMostOnce),
    ];
    client.publish_batch(&batch).await.unwrap();

    loop {
        if let Some(event) = client.poll().await.unwrap() {
            // Process event
        }
    }
}
```

---

## Feature Flags

| Feature | Function | Default |
| :--- | :--- | :---: |
| `default` | Zero-allocation `no_std` embedded build | **Yes** |
| `std` | Standard library support for desktop/testing | No |
| `v5` | MQTT v5.0 properties and reason codes | No |
| `defmt` | Zero-overhead logging for microcontrollers | No |
| `transport-smoltcp` | Direct `embassy-net` TCP socket integration | No |
| `transport-quic` | QUIC / HTTP/3 transport via `quinn` | No |

---

## Run Examples

```bash
# ESP32 Wi-Fi & Embassy
cargo run --example esp32_wifi_embassy --features std

# Multi-Packet Burst Batching
cargo run --example multipacket_burst --features std

# Desktop Mock Client
cargo run --example desktop_mock --features std

# Real-Time Chunk Streaming
cargo run --example realtime_stream --features std

# MQTT over QUIC
cargo run --example quic_client --features transport-quic
```

---

## Documentation Links

- [**Application Flow & State Machines**](docs/appflow.md)
- [**Wire Protocol & Binary Schemas**](docs/backend_schema.md)
- [**Design Brief**](docs/design_brief.md)
- [**Product Requirements (PRD)**](docs/prd.md)
- [**Technical Design (TDD)**](docs/tdd.md)

---

## License

GNU General Public License v3.0 or later ([GPL-3.0-or-later](LICENSE)).