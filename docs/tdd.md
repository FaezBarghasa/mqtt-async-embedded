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
- **`MqttOptions<'a>`**: Holds `client_id`, `broker_addr`, `broker_port`, `keep_alive`, `clean_session`, and optional authentication credentials.
- **`MqttClient<'a, T, MAX_TOPICS, BUF_SIZE>`**:
  - `transport`: Generic parameter `T` implementing `MqttTransport`.
  - `publish_batch(&[PublishMessage])`: Packs multiple messages into a single network frame.
  - `poll_batch()`: Parses and yields all available incoming events in `rx_buffer`.
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

### 2.3. Control Packet Encoding & Decoding (`src/packet.rs` & `src/util.rs`)
- **Trait `EncodePacket` / `DecodePacket`**: Zero-copy packet serialization and deserialization.
- **`RawPacketFrameIter`**: Zero-copy streaming iterator that slices and parses multiple MQTT packets from a single continuous receive buffer.

---

## 3. Asynchronous Flow & Concurrency Considerations

### 3.1. Single-Threaded Async Polling & Multi-Packet Burst
Designed specifically for cooperative async executors like `embassy-executor`. Polling processes all frames available in `rx_buffer`, automatically generating QoS 1 `PUBACK` responses inline.

### 3.2. Lifetime Annotations & Zero-Allocation Safety
`MqttEvent<'p>` borrows from the client's internal `rx_buffer` for duration `'p`. Because Rust enforces exclusive borrow semantics, events are processed without dynamic allocation or heap fragmentation.

---

## 4. Feature Flags & Compilation Matrix

- **`default = []`**: Standard `no_std` compilation for embedded targets.
- **`std`**: Includes standard library support for host testing and mocks.
- **`v5`**: Enables MQTT v5 extended properties and user properties.
- **`defmt`**: Implements `defmt::Format` for high-efficiency microcontroller logging.
- **`transport-smoltcp`**: Integrates directly with `embassy-net` TCP stack.
- **`transport-quic`**: Enables MQTT over QUIC / H3 via `quinn`.

---

## 5. Verification & Testing Strategy

1. **Unit Testing (`cargo test --features std`)**:
   - Variable-byte integer encoding/peeking tests.
   - Packet codec roundtrip tests (`Publish`, `Subscribe`, `PubAck`, `ConnAck`, `Disconnect`).
   - Multi-packet streaming frame iteration tests (`RawPacketFrameIter`).
2. **Integration Testing (`examples/desktop_mock.rs` & `examples/multipacket_burst.rs`)**:
   - Batch packet burst transmission verification.
   - TCP lifecycle connection tests.

