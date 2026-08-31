# Technical Design Document (TDD)

Subsystem architecture, transport contracts, web bridges, and test strategy for `mqtt-async-embedded`.

---

## 1. System Architecture

```
+------------------------------------------------------------------+
|                    Application (Task / Worker)                   |
+------------------------------------------------------------------+
|  MqttClient<'a, T, MAX_TOPICS, BUF_SIZE> / Tokio AsyncClient    |
|   +-------------------+  +-------------------+  +---------------+|
|   |  ConnectionState  |  |  tx_buf / rx_buf  |  |  MqttOptions  ||
|   +-------------------+  +-------------------+  +---------------+|
+------------------------------------------------------------------+
                                  |
            +---------------------+---------------------+
            |                                           |
            v                                           v
+---------------------------+               +---------------------------+
|  MqttTransport Trait      |               | Web & UI Bridges          |
|  MqttQuicTransport Trait  |               | - CameraMjpegBridge (Axum)|
|  TlsTransport Trait       |               | - TelemetrySseBridge (SSE)|
+---------------------------+               | - Slint Stream Binding    |
  /           |            \                +---------------------------+
 v            v             v
TcpSocket UART Modem  QUIC Stream
```

---

## 2. Core Modules & Workspace Crates

### 2.1. `mqtt-packet`
- **`DecodePacket` & `EncodePacket` traits**: Zero-allocation encoding and decoding within user-provided slices.
- **`RawPacketFrameIter`**: Zero-copy packet streaming iterator over continuous byte streams.
- **`properties`**: MQTT 5.0 properties parsing with safety bounds guards.
- **Fuzzing & Proptest**: `fuzz/fuzz_targets/fuzz_packet_decode.rs` and `tests/proptest_codec.rs`.

### 2.2. `mqtt-embedded`
- **`MqttOptions<'a>`**: Broker endpoint, keep-alive, clean session, LWT, credentials.
- **`MqttClient<'a, T, MAX_TOPICS, BUF_SIZE, MAX_INFLIGHT>` (`no_std`)**:
  - `publish(topic, payload, qos)`: Single packet write.
  - `publish_batch(&[PublishMessage])`: Packs multiple messages into one network write.
  - `subscribe(&[(&str, QoS)])` / `unsubscribe(&[&str])`: Sends subscription requests.
  - `poll()` / `poll_batch()`: Parses RX buffer, returns zero-copy `MqttEvent<'p>`.
  - `begin_stream_publish(topic, total_len, qos)`: Direct-to-wire chunked streaming.
  - `MqttStreamWriter::write_dma_slice(slice)`: Zero-copy DMA buffer streaming.
- **`InflightQueue`**: Compile-time bounded queue for QoS 1 and QoS 2 in-flight tracking.
- **`TlsTransport`**: Pluggable MCU TLS abstraction for `embedded-tls` / `mbedtls-sys`.

### 2.3. `mqtt-tokio`
- **`Client` / `AsyncClient` / `EventLoop`**:
  - `Client::connect(options)`: Spawns background driver task.
  - `publish(topic, qos, retain, payload)`: Zero-copy publish via `bytes::Bytes`.
  - `subscribe_stream(topic, qos)`: Topic-filtered stream backed by a prefix trie.
  - `SmartTransport`: Automatic QUIC to TCP/TLS fallback.
  - Session Data Recovery: Offline queueing (`DropOldest`, `ErrorOnFull`, `Block`) and in-flight retransmission.
- **Target OS Compatibility**: Linux, Windows, Android, macOS, and Redox OS (`x86_64-unknown-redox`).

### 2.4. `mqtt-bridges`
- **`MqttBroadcastHub`**: Subscribes once to MQTT topic and broadcasts to unbounded HTTP/SSE connections via `tokio::sync::broadcast`.
- **`CameraMjpegBridge`**: Prepares `multipart/x-mixed-replace; boundary=frame` chunks for streaming directly into HTML `<img>` elements.
- **`TelemetrySseBridge`**: Formats MQTT payloads into standard SSE lines (`data: <payload>\n\n`).
- **`SlintStreamBinding`**: Dispatches incoming MQTT payloads to Slint UI event loops safely across thread boundaries.

---

## 3. Transport Abstraction

```rust
pub trait MqttTransport {
    type Error: TransportError;
    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error>;
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;
    async fn send_vectored(&mut self, bufs: &[&[u8]]) -> Result<(), Self::Error>;
}

pub trait TlsTransport: MqttTransport {
    fn is_handshake_complete(&self) -> bool;
}

pub trait MqttQuicTransport {
    type Error: TransportError;
    type SendStream: MqttQuicSendStream<Error = Self::Error>;
    type RecvStream: MqttQuicRecvStream<Error = Self::Error>;
    async fn open_bi_stream(&mut self) -> Result<(Self::SendStream, Self::RecvStream), Self::Error>;
    async fn open_uni_stream(&mut self) -> Result<Self::SendStream, Self::Error>;
    async fn accept_bi_stream(&mut self) -> Result<(Self::SendStream, Self::RecvStream), Self::Error>;
    async fn send_datagram(&mut self, data: &[u8]) -> Result<(), Self::Error>;
    async fn recv_datagram(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;
}
```

---

## 4. Verification Suite

| Test Target | Files | Coverage |
| :--- | :--- | :--- |
| **Packet Codec & Bounds** | `tests/engine_tests.rs`, `crates/mqtt-packet/tests/proptest_codec.rs` | Varint encoding, packet roundtrips, malformed bounds safety, random bytes fuzzing |
| **Fuzzing Harness** | `fuzz/fuzz_targets/fuzz_packet_decode.rs` | `libfuzzer-sys` continuous decoder fuzzing for v3.1.1 & v5 |
| **Embedded Client Logic** | `tests/client_tests.rs` | Mock transport, handshake, burst publish, auto-`PUBACK`, zero-copy DMA streaming |
| **Tokio Host & Driver** | `tests/tokio_client_tests.rs` | Batch publish, stream routing, reconnect recovery, offline queue, Slint binding |
| **Performance Benchmarks**| `benches/benches/codec_benchmarks.rs` | Criterion throughput benchmarks for encoding, decoding, and varints |

### Reference Examples

- `examples/stm32h7_embassy_mqtt.rs`: Bare-metal STM32H7 DMA ADC stream publishing with Embassy.
- `examples/esp32c3_uart_mqtt.rs`: ESP32-C3 / RISC-V UART modem serial transport.
- `examples/redox_daemon.rs`: Redox OS microkernel gateway background daemon.
- `examples/slint_dashboard.rs`: Slint UI property & live camera stream binding.
- `examples/esp32_wifi_embassy.rs`: Bare-metal ESP32 Wi-Fi task.
- `examples/realtime_stream.rs`: Zero-RAM sensor chunk publishing.
- `examples/quic_client.rs`: Unreliable QUIC telemetry datagrams.
- `examples/tokio_basic_pubsub.rs`: Tokio pub/sub with topic router.
- `examples/tokio_reconnect_resilience.rs`: Tokio connection loss & journal replay.
- `examples/server_camera_web_bridge.rs`: Axum / Actix MJPEG video and SSE telemetry bridge.
- `examples/slint_dashboard_app.rs`: Slint desktop/embedded dashboard with live MQTT bindings.
