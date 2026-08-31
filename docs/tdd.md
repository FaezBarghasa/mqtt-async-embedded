# Technical Design Document (TDD)

Subsystem architecture, transport contracts, and test strategy for `mqtt-async-embedded`.

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
                                  v
                    +---------------------------+
                    |  MqttTransport Trait      |
                    |  MqttQuicTransport Trait  |
                    +---------------------------+
                      /           |            \
                     v            v             v
             TcpSocket        UART Modem      QUIC Stream
            (embassy-net)   (ESP8266/AT)    (Quinn / Cellular)
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

---

### 2.2. Transport Abstraction (`src/transport.rs`)

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

- **`EmbeddedIoTransport<S>`**: Wraps unified `embedded_io_async::Read + Write` streams.
- **`EmbeddedIoSplitTransport<R, W>`**: Wraps split reader/writer streams (e.g. split UART RX/TX).

---

## 3. Concurrency & Memory Safety

- **Cooperative Polling**: Designed for `embassy-executor` and `tokio`. Polling handles automated `PUBACK` responses internally.
- **Zero-Allocation Lifetimes**: `MqttEvent<'p>` borrows directly from `rx_buffer`. Compiler prevents events from outliving the client borrow.
- **Cancel Safety**: Future drops leave internal state machines consistent; partial packet state resets cleanly on reconnect.

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
