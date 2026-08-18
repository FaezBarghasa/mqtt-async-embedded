# Design Brief & Architectural Principles

This document presents the core design philosophy, architectural goals, memory model, and interface contracts of `mqtt-async-embedded`.

---

## 1. Executive Summary & Vision

`mqtt-async-embedded` is a light-weight, asynchronous, zero-allocation (`no_std` / `no_alloc`) MQTT client library written in Rust for resource-constrained microcontrollers (e.g. ARM Cortex-M, ESP32, RISC-V). Designed natively around the **Embassy** async ecosystem and Rust 2024 edition standard async features, it decouples networking protocols from raw hardware peripherals.

---

## 2. Core Architectural Pillars

### 2.1. Zero Dynamic Allocation (`no_std` & `no_alloc`)
- **Fixed-Capacity Buffers**: Uses compile-time constant generics `const BUF_SIZE: usize` for static transmit (`tx_buffer`) and receive (`rx_buffer`) buffers.
- **Bounded Data Structures**: Employs `heapless` collections for topic management and packet queueing without heap fragmentations or runtime memory panics.

### 2.2. Hardware-Agnostic Abstraction
- Transport capabilities are abstracted using the custom `MqttTransport` trait.
- Works seamlessly over standard TCP (`embassy-net`, `std::net`), UART (AT-command modems like ESP8266/SIM800), SPI, or custom wireless radio stacks.

```rust
pub trait MqttTransport {
    type Error: TransportError;
    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error>;
    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;
}
```

### 2.3. Asynchronous Native Design
- Leverages Embassy `embassy-time` for non-blocking timers (`Instant`, `Duration`).
- Implements Rust 2024 edition native `async fn` traits without allocating boxed futures.

### 2.4. Protocol Flexibility & Feature Modularization
- Supports both MQTT **v3.1.1** and **v5.0** protocol specifications with rich properties, reason codes, and user property pairs.
- Full support for **Last Will and Testament (LWT)**, **`UNSUBSCRIBE` / `UNSUBACK`**, and broker QoS 2 handshakes (`PUBREC`, `PUBREL`, `PUBCOMP`).
- Modular logging with zero-overhead `defmt` support for microcontrollers or standard `log`/`env_logger` on desktop host environments.

---

## 3. Memory & Lifetime Safety Model

### 3.1. Zero-Copy Event Yielding
Received MQTT packets (such as `Publish<'p>`) borrow slice references directly from the internal `rx_buffer` for lifetime `'p`.
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
This guarantees zero heap copies while ensuring safety: the event reference cannot outlive the lifetime of the client's mutable `poll()` borrow.

### 3.2. Defensive Bounds Checking & Resilient Codecs
All serialization and deserialization functions use checked slice indexing (`get()`, `get_mut()`) rather than unchecked direct index offsets, ensuring that malformed, truncated, or malicious broker packets cleanly bubble up as `MqttError::Protocol(ProtocolError::MalformedPacket)` or `MqttError::BufferTooSmall` rather than triggering panic aborts.

### 3.3. Buffer Allocation Strategy
- `BUF_SIZE`: Fixed array size for transmit/receive buffers (default standard recommendation: 512B - 2048B).
- `MAX_TOPICS`: Maximum allowed concurrent subscriptions or filters stored statically in `heapless::Vec`.

---

## 4. Hardware and Network Integration Targets

| Ecosystem Layer | Implementation / Integration Target |
| :--- | :--- |
| **ESP32 Microcontrollers** | **ESP32-S series** (ESP32-S2, ESP32-S3) [Xtensa] & **ESP32-C series** (ESP32-C2, ESP32-C3, ESP32-C6) [RISC-V], ESP32 classic, ESP32-H2 |
| **Microcontroller HALs** | **`esp-hal`** (bare-metal `no_std`), `esp-wifi`, **`esp-idf-svc` / `esp-idf-hal`**, `embassy-stm32`, `embassy-nrf`, `rp-hal` |
| **Async Runtime** | **`embassy-executor`** (bare-metal non-allocating tasks) & `tokio` (desktop/edge) |
| **Universal Adapters** | `EmbeddedIoTransport<S>` and `EmbeddedIoSplitTransport<R, W>` for any `embedded-io-async` socket |
| **Network Stacks** | `embassy-net` (`smoltcp`), `esp-wifi`, BSD / ESP-IDF sockets, UART AT modems |
| **Host / Edge System** | `tokio`, `std::net::TcpStream`, `quinn` (QUIC / H3) |
