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
+---------------------------+               | - TelemetrySseBridge (SSE)|
  /           |            \                | - Slint Stream Binding    |
 v            v             v               +---------------------------+
TcpSocket UART Modem  QUIC Stream
```

---

## 2. Core Modules

### 2.1. Client Module (`src/client.rs` & `src/tokio_client/*`)

- **`MqttOptions<'a>`**: Broker endpoint, keep-alive, clean session, LWT, credentials.
- **`MqttClient<'a, T, MAX_TOPICS, BUF_SIZE>` (`no_std`)**:
  - `publish(topic, payload, qos)`: Single packet write.
  - `publish_batch(&[PublishMessage])`: Packs multiple messages into one network write.
  - `subscribe(&[(&str, QoS)])`: Sends subscription array, returns `packet_id`.
  - `unsubscribe(&[&str])`: Sends unsubscription array, returns `packet_id`.
  - `poll()` / `poll_batch()`: Parses RX buffer, returns zero-copy `MqttEvent<'p>`.
  - `begin_stream_publish(topic, total_len, qos)`: Direct-to-wire streaming without intermediate buffer allocation.
- **`Client` / `AsyncClient` (`tokio-client`)**:
  - `Client::connect(options)`: Spawns background `EventLoop` driver task.
  - `publish_batch(messages)`: Zero-copy burst publishing via `bytes::Bytes`.
  - `subscribe_stream(topic, qos)`: Topic-filtered stream backed by a prefix trie.
  - `create_datastream_producer(topic, qos, window)`: Multi-worker producer with atomic ordering and sliding recovery journal.
  - `create_broadcast_hub(topic, qos, capacity)`: 1-to-N fanout hub for web servers.
  - `bind_slint_property()` / `bind_slint_camera()`: Cross-thread UI property update binders.

---

### 2.2. Web Server Bridges & UI Integrations (`src/tokio_client/web.rs`, `src/tokio_client/slint_support.rs`)

- **`MqttBroadcastHub`**: Subscribes once to MQTT topic and broadcasts to unbounded HTTP/SSE connections via `tokio::sync::broadcast`.
- **`CameraMjpegBridge`**: Prepares `multipart/x-mixed-replace; boundary=frame` chunks for streaming directly into standard HTML `<img>` elements.
- **`TelemetrySseBridge`**: Formats MQTT payloads into standard SSE lines (`data: <payload>\n\n`).
- **`SlintStreamBinding`**: Dispatches incoming MQTT payloads to Slint UI event loops safely across thread boundaries.

---

### 2.3. Transport Abstraction (`src/transport.rs`)

```rust
pub trait MqttTransport {
    type Error: TransportError;
    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error>;
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;
    async fn send_vectored(&mut self, bufs: &[&[u8]]) -> Result<(), Self::Error>;
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

## 3. Concurrency & Memory Safety

- **Cooperative Polling**: Tailored for `embassy-executor` and `tokio`. Automatically handles incoming `PUBACK`.
- **Zero-Allocation Lifetimes**: `MqttEvent<'p>` borrows directly from `rx_buffer`. Compiler ensures events never outlive the client.
- **Cancel Safety**: Future drops maintain consistent state; partial packet buffers reset cleanly on reconnect.

---

## 4. Verification Suite

| Test Target | Files | Coverage |
| :--- | :--- | :--- |
| **Packet Codec & Bounds** | `tests/engine_tests.rs` | Varint encoding, packet roundtrips, malformed bounds safety |
| **Embedded Client Logic** | `tests/client_tests.rs` | Mock transport, handshake, burst publish, auto-`PUBACK` |
| **Tokio Host & Driver** | `tests/tokio_client_tests.rs` | Batch publish, stream routing, reconnect recovery, offline queue |

### Example Reference Implementations

- `examples/esp32_wifi_embassy.rs`: Bare-metal ESP32 Wi-Fi task.
- `examples/realtime_stream.rs`: Zero-RAM sensor chunk publishing.
- `examples/quic_client.rs`: Unreliable QUIC telemetry datagrams.
- `examples/tokio_basic_pubsub.rs`: Tokio pub/sub with topic router.
- `examples/tokio_reconnect_resilience.rs`: Tokio connection loss & journal replay.
- `examples/server_camera_web_bridge.rs`: Axum / Actix MJPEG video and SSE telemetry bridge.
- `examples/slint_dashboard_app.rs`: Slint desktop/embedded dashboard with live MQTT bindings.
