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
- Supports both MQTT **v3.1.1** and **v5.0** protocol specifications controlled via conditional feature compilation (`cfg(feature = "v5")`).
- Modular logging with zero-overhead `defmt` support for microcontrollers or standard `log`/`env_logger` on desktop host environments.

---

## 3. Memory & Lifetime Safety Model

### 3.1. Zero-Copy Event Yielding
Received MQTT packets (such as `Publish<'p>`) borrow slice references directly from the internal `rx_buffer` for lifetime `'p`.
```rust
pub enum MqttEvent<'p> {
    Publish(Publish<'p>),
}
```
This guarantees zero heap copies while ensuring safety: the event reference cannot outlive the lifetime of the client's mutable `poll()` borrow.

### 3.2. Buffer Allocation Strategy
- `BUF_SIZE`: Fixed array size for transmit/receive buffers (default standard recommendation: 512B - 2048B).
- `MAX_TOPICS`: Maximum allowed concurrent subscriptions or filters stored statically.

---

## 4. Hardware and Network Integration Targets

| Ecosystem Layer | Implementation / Integration Target |
| :--- | :--- |
| **Microcontroller HALs** | `embassy-stm32`, `embassy-nrf`, `esp-hal`, `rp-hal` |
| **Async Runtime** | `embassy-executor` |
| **Network Stack** | `embassy-net` (`smoltcp`), UART AT drivers |
| **Host System** | `tokio`, `std::net::TcpStream` (Desktop testing / mocks) |
