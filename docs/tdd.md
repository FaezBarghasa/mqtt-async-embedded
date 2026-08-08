# Technical Design Document (TDD)

## 1. System Architecture & Component Design

The `mqtt-async-embedded` crate provides an asynchronous, zero-allocation (`no_std`) MQTT protocol client for embedded microcontrollers.

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
                    +---------------------------+
                     /            |            \
                    v             v             v
            TcpSocket        UART Modem      Mock Socket
            (embassy-net)   (ESP8266/AT)    (std::net)
```

---

## 2. Key Modules & Subsystems

### 2.1. Client Module (`src/client.rs`)
- **`MqttOptions<'a>`**: Holds `client_id`, `broker_addr`, `broker_port`, `keep_alive` duration, and `version` (`MqttVersion::V3` or `MqttVersion::V5`).
- **`MqttClient<'a, T, MAX_TOPICS, BUF_SIZE>`**:
  - `transport`: Generic parameter `T` implementing `MqttTransport`.
  - `tx_buffer: [u8; BUF_SIZE]`: Stack / inline array for outgoing packet formatting.
  - `rx_buffer: [u8; BUF_SIZE]`: Buffer for receiving incoming transport stream bytes.
  - `state`: Enum managing `Disconnected`, `Connecting`, `Connected`.
  - `last_tx_time`: Embassy `Instant` timestamp tracking keep-alive state.
  - `next_packet_id`: Wrapping `u16` counter for packet identification.

### 2.2. Transport Abstraction Layer (`src/transport.rs`)
Decouples protocol execution from socket or physical peripheral drivers:
```rust
pub trait MqttTransport {
    type Error: TransportError;
    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error>;
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;
}
```

### 2.3. Control Packet Encoding & Decoding (`src/packet.rs`)
- **Trait `EncodePacket`**: Encodes Rust data structures into wire format bytes.
- **Function `decode<E>(buf: &[u8], version: MqttVersion)`**: Parses raw bytes from transport into `MqttPacket` enums. Optional support for `nom` parser via feature flag `nom`.

---

## 3. Asynchronous Flow & Concurrency Considerations

### 3.1. Single-Threaded Async Polling
Designed specifically for cooperative async executors like `embassy-executor`. Polling is non-blocking and handles both incoming network traffic and heartbeat keep-alives within a single unified loop.

### 3.2. Lifetime Annotations & Zero-Allocation Safety
`MqttEvent<'p>` borrows from the client's internal `rx_buffer` for duration `'p`. Because Rust enforces exclusive borrow semantics, the event must be processed or copied by the caller before calling `poll()` again, preventing race conditions or slice invalidation without using heap memory.

---

## 4. Feature Flags & Compilation Matrix

- **`default = []`**: Standard `no_std` compilation for embedded targets.
- **`std`**: Includes standard library support, enabling testing on host computers via `tokio` or `std::net`.
- **`v5`**: Enables MQTT v5 extended properties and reason code handling.
- **`defmt`**: Implements `defmt::Format` for high-efficiency logging over SWD/RTT.
- **`transport-smoltcp`**: Integrates directly with `embassy-net` TCP stack.
- **`nom`**: Enables `nom` parser combinators for packet validation.

---

## 5. Verification & Testing Strategy

1. **Unit Testing (`cargo test --features std`)**:
   - Fixed-header parsing tests.
   - Variable-byte integer encoding/decoding validation.
   - Malformed packet handling tests.
2. **Integration Testing (`examples/desktop_mock.rs`)**:
   - Local Mosquitto broker integration testing over TCP.
3. **Embedded Target Verification (`examples/esp8266_uart.rs` & `smoltcp_ethernet.rs`)**:
   - Hardware validation on target microcontrollers under memory-constrained conditions.
