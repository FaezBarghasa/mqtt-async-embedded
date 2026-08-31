# Technical Design Document (TDD)

Subsystem architectures, transport contracts, and testing specifications for `mqtt-async-embedded`.

---

## 1. System Architecture

```
+------------------------------------------------------------------+
|                    Application (Embassy Task)                    |
+------------------------------------------------------------------+
|  MqttClient<'a, T, MAX_TOPICS, BUF_SIZE>                         |
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
                     /            |            \
                    v             v             v
             TcpSocket        UART Modem      QUIC Stream
            (embassy-net)   (ESP8266/AT)    (Quinn / Cellular)
```

---

## 2. Core Modules

### 2.1. Client Module (`src/client.rs`)
- **`MqttOptions<'a>`**: Holds broker parameters, keep-alive interval, clean session, credentials, and LWT configuration.
- **`MqttClient<'a, T, MAX_TOPICS, BUF_SIZE>`**:
  - `publish(topic, payload, qos)`: Publishes single message.
  - `publish_batch(&[PublishMessage])`: Packs multiple messages into a single network frame.
  - `subscribe(&[(&str, QoS)])`: Sends subscription request and returns `packet_id`.
  - `unsubscribe(&[&str])`: Sends unsubscription request and returns `packet_id`.
  - `poll()` / `poll_batch()`: Parses RX buffer and yields zero-copy `MqttEvent<'p>`.
- **`QuicMqttClient<'a, Q, BUF_SIZE>`**: Transmits real-time telemetry over unreliable QUIC datagrams.

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

- **`EmbeddedIoTransport<S>`**: Wraps any combined async stream (`Read + Write`).
- **`EmbeddedIoSplitTransport<R, W>`**: Wraps separate reader and writer streams (e.g. split UART RX/TX).

---

## 3. Concurrency & Lifetimes

- **Async Polling**: Cooperative execution tailored for `embassy-executor`. Polling automatically responds to QoS 1 incoming packets with `PUBACK`.
- **Zero-Allocation Lifetime**: `MqttEvent<'p>` borrows from the internal `rx_buffer`. Compiler ensures events do not outlive the client borrow.

---

## 4. Testing & Verification

1. **Unit Tests (`tests/engine_tests.rs`)**:
   - Variable-byte integer encoding/decoding.
   - Control packet roundtrips (`Publish`, `Subscribe`, `Unsubscribe`, `Connect`, `PubAck`, `ConnAck`, `Disconnect`).
   - Frame bounds safety and zero-length buffer assertions.
   - Multi-packet frame iteration via `RawPacketFrameIter`.
2. **Integration Tests (`tests/client_tests.rs`)**:
   - Mock transport verifying connection handshakes, QoS 0/1 bursts, auto-`PUBACK`, and stream adapters (20 tests).
3. **Hardware Examples**:
   - `examples/esp32_wifi_embassy.rs` (ESP32 Wi-Fi task)
   - `examples/multipacket_burst.rs` (Batch burst)
   - `examples/realtime_stream.rs` (Chunk streaming)
   - `examples/quic_client.rs` (QUIC datagrams)
   - `examples/esp8266_uart.rs` (UART AT modem)
   - `examples/smoltcp_ethernet.rs` (`embassy-net` TCP)
   - `examples/desktop_mock.rs` (Desktop TCP lifecycle)
