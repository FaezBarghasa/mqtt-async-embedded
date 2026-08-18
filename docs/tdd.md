# Technical Design Document (TDD)

## 1. System Architecture & Component Design

The `mqtt-async-embedded` crate provides an asynchronous, zero-allocation (`no_std`) MQTT protocol client for embedded microcontrollers and edge gateways.

```
+------------------------------------------------------------------+
|                    Application Code (Embassy Task)                |
+------------------------------------------------------------------+
|  MqttClient<'a, T, MAX_TOPICS, BUF_SIZE>                         |
|   +-------------------+  +-------------------+  +---------------+|
|   |  ConnectionState  |  | tx_buffer / rx_...|  | MqttOptions   ||
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

## 2. Key Modules & Subsystems

### 2.1. Client Module (`src/client.rs`)
- **`MqttOptions<'a>`**: Holds `client_id`, `broker_addr`, `broker_port`, `keep_alive`, `clean_session`, `username`, `password`, and `will` (`Will<'a>`).
- **`MqttClient<'a, T, MAX_TOPICS, BUF_SIZE>`**:
  - `transport`: Generic parameter `T` implementing `MqttTransport`.
  - `publish(topic, payload, qos)`: Publishes single message with QoS validation.
  - `publish_batch(&[PublishMessage])`: Packs multiple messages into a single network frame.
  - `subscribe(&[(&str, QoS)])`: Subscribes to topic filters returning packet ID.
  - `unsubscribe(&[&str])`: Unsubscribes from topic filters returning packet ID.
  - `poll()` / `poll_batch()`: Parses and yields all available incoming events in `rx_buffer`.
- **`QuicMqttClient<'a, Q, BUF_SIZE>`**:
  - Specialized real-time client transmitting telemetry directly over unreliable QUIC datagrams.

### 2.2. Transport Abstraction Layer (`src/transport.rs`)
Decouples protocol execution from physical socket or radio modems:
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

#### Universal `embedded-io-async` Adapters
- **`EmbeddedIoTransport<S>`**: Wraps any single stream `S: embedded_io_async::Read + embedded_io_async::Write` (`esp-hal`, `esp-wifi`, `esp-idf-svc`, `embassy-net`).
- **`EmbeddedIoSplitTransport<R, W>`**: Wraps separate reader `R` and writer `W` streams (split UART RX/TX or split TCP channels).

---

## 3. Asynchronous Flow & Concurrency Considerations

### 3.1. Single-Threaded Async Polling & Multi-Packet Burst
Designed specifically for cooperative async executors like `embassy-executor`. Polling processes all frames available in `rx_buffer`, automatically generating QoS 1 `PUBACK` responses inline.

### 3.2. Lifetime Annotations & Zero-Allocation Safety
`MqttEvent<'p>` borrows from the client's internal `rx_buffer` for duration `'p'`. Because Rust enforces exclusive borrow semantics, events are processed without dynamic allocation or heap fragmentation.

---

## 4. Feature Flags & Target Matrix

- **`default = []`**: Standard `no_std` compilation for embedded microcontrollers.
- **`std`**: Includes standard library support for host testing and mocks.
- **`v5`**: Enables MQTT v5 extended properties and user properties.
- **`defmt`**: Implements `defmt::Format` across all packet and client types for zero-overhead microcontroller logging.
- **`transport-smoltcp`**: Integrates directly with `embassy-net` TCP stack.
- **`transport-quic`**: Enables MQTT over QUIC / H3 via `quinn`.

### Supported Target Architectures
- **ESP32-S Series**: ESP32-S2, ESP32-S3 (`xtensa-esp32s3-none-elf`)
- **ESP32-C Series**: ESP32-C2, ESP32-C3, ESP32-C6, ESP32-H2 (`riscv32imc-unknown-none-elf`, `riscv32imac-unknown-none-elf`)
- **ARM Cortex-M**: Cortex-M0/M3/M4/M7/M33 (`thumbv7em-none-eabihf`)

---

## 5. Verification & Testing Strategy

1. **Unit Testing (`tests/engine_tests.rs`)**:
   - Variable-byte integer encoding/peeking tests.
   - Packet codec roundtrip tests (`Publish`, `Subscribe`, `Unsubscribe`, `Connect` with LWT, `PubAck`, `ConnAck`, `Disconnect`).
   - Bounds safety and malformed packet tests (`read_properties` truncation, 0-byte buffer encoding).
   - Multi-packet streaming frame iteration tests (`RawPacketFrameIter`).
2. **Client-Level Integration Testing (`tests/client_tests.rs`)**:
   - In-memory async mock transport verifying connection lifecycle, refusal handling, QoS 0/1 burst sending, automatic QoS 1 `PUBACK`, dynamic unsubscription, `EmbeddedIoTransport` stream binding, and `EmbeddedIoSplitTransport` binding (20 tests total).
3. **Hardware Examples**:
   - `examples/esp32_wifi_embassy.rs`: ESP32 Wi-Fi & Embassy async task loop.
   - `examples/multipacket_burst.rs`: Multi-packet batch sending.
   - `examples/quic_client.rs`: Real-time QUIC datagrams.
   - `examples/esp8266_uart.rs`: AT UART hardware driver bridge.
   - `examples/smoltcp_ethernet.rs`: Native `embassy-net` TCP socket.
   - `examples/desktop_mock.rs`: TCP lifecycle connection, subscription, and disconnection tests.

