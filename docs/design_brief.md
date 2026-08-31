# Design Brief & Architectural Principles

Design philosophy, zero-allocation memory model, and interface contracts of `mqtt-async-embedded`.

---

## 1. Core Principles

- **Zero Allocations (`no_std`, `no_alloc`)**: Static compile-time buffers (`const BUF_SIZE: usize`) and `heapless` collections. No heap, no runtime fragmentation.
- **Hardware Decoupling**: All I/O goes through `MqttTransport` (TCP, UART modems, SPI) or `MqttQuicTransport`.
- **Native Async (Rust 2024)**: Non-blocking timers via `embassy-time`. No boxed futures.
- **Protocol Completeness**: Full wire codec for MQTT v3.1.1 and v5.0 (User Properties, Reason Codes), LWT, dynamic unsubscriptions, and QoS 2 broker handshakes.
- **Zero-RAM Chunk Streaming**: `MqttStreamWriter` streams continuous data (ADC, camera, audio) chunk-by-chunk directly across the wire on MCUs with only 512B-2KB RAM.

---

## 2. Transport Trait Contract

```rust
pub trait MqttTransport {
    type Error: TransportError;
    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error>;
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;
    async fn send_vectored(&mut self, bufs: &[&[u8]]) -> Result<(), Self::Error>;
}
```

Universal adapters (`EmbeddedIoTransport` and `EmbeddedIoSplitTransport`) wrap any `embedded-io-async` socket automatically.

---

## 3. Zero-Copy Lifetime Model

Incoming packets borrow directly from the internal `rx_buffer` for lifetime `'p`:

```rust
pub enum MqttEvent<'p> {
    Publish(Publish<'p>),
    PubAck(PubAck<'p>),
    SubAck(packet::SubAck<'p>),
    UnsubAck(packet::UnsubAck<'p>),
    PingResp,
    Disconnect(Disconnect<'p>),
}
```

- **Bounds Safety**: Decoders use checked slice indexing (`get()`, `get_mut()`). Truncated or malformed frames return `ProtocolError::MalformedPacket` instead of panicking.
- **Compile-Time Buffer Sizing**:
  - `BUF_SIZE`: Fixed byte capacity for TX/RX buffers (typically 512B to 2048B).
  - `MAX_TOPICS`: Maximum static subscription filters in `heapless::Vec`.

---

## 4. Hardware & Ecosystem Support Matrix

| Layer | Targets |
| :--- | :--- |
| **ESP32 MCUs** | ESP32-S series (S2, S3), ESP32-C series (C2, C3, C6, H2), ESP32 classic |
| **HALs** | `esp-hal` (`no_std`), `esp-wifi`, `esp-idf-svc`, `embassy-stm32`, `embassy-nrf`, `rp-hal` |
| **Runtimes** | `embassy-executor` (bare-metal), `tokio` (desktop/edge) |
| **Network Stacks** | `embassy-net` (`smoltcp`), `esp-wifi`, BSD sockets, UART AT modems |
| **Host / Edge** | `tokio`, `std::net::TcpStream`, `quinn` (QUIC / H3) |
