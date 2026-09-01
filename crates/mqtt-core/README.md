# `mqtt-core`

[![Crates.io](https://img.shields.io/crates/v/mqtt-core.svg)](https://crates.io/crates/mqtt-core)
[![Documentation](https://docs.rs/mqtt-core/badge.svg)](https://docs.rs/mqtt-core)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](../../LICENSE-MIT)
[![no_std](https://img.shields.io/badge/no__std-compatible-success.svg)](https://docs.rust-embedded.org/book/intro/no-std.html)
[![Safety: forbid(unsafe_code)](https://img.shields.io/badge/unsafe_code-forbidden-brightgreen.svg)](#)

**`mqtt-core`** is the pure `no_std`, `no_alloc` foundational layer of the `mqtt-async-embedded` Universal MQTT Engine. It defines the runtime-independent protocol state machine, asynchronous transport traits, error types, and in-flight collision tracking.

---

## Features

- **Pure `no_std` & `no_alloc`:** Zero dynamic heap allocations, compile-time bounded types (`heapless`).
- **Foundational Traits:**
  - `Transport`: Core asynchronous full-duplex byte stream contract.
  - `VectoredTransport`: Scatter/gather vectored writing for syscall reduction (`writev`).
  - `ZeroCopyTransport`: Direct DMA ring buffer packet referencing.
  - `Clock`: Abstract monotonic clock for keep-alive timeouts and backoff jitter.
  - `Storage`: Key-value persistence abstraction for persistent sessions.
- **Pure State Machine:** Deterministic `transition(state, event) -> Result<(ConnState, StateAction), ProtocolError>` pure function for complete testability without physical I/O.
- **In-flight Collision Tracker:** Bounded `InflightQueue<N>` with $O(1)$ packet-ID index and out-of-order collision detection.
- **Unified Error Model:** Hierarchical `MqttError<E>` cleanly mapping transport, protocol, codec, storage, and crypto failure modes.

---

## Usage

```toml
[dependencies]
mqtt-core = "1.6.0"
```

```rust
use mqtt_core::state::{ConnState, StateAction, StateEvent, transition};
use mqtt_core::inflight::InflightQueue;
use mqtt_packet::QoS;

// 1. Evaluate pure protocol transitions without I/O
let (next_state, action) = transition(
    ConnState::WaitingForConnAck,
    StateEvent::ConnAckReceived { session_present: false }
).unwrap();
assert_eq!(next_state, ConnState::Connected);

// 2. Track in-flight QoS 1 & QoS 2 packet IDs with collision guards
let mut inflight: InflightQueue<8> = InflightQueue::new();
inflight.push(1, QoS::AtLeastOnce).unwrap();
assert!(inflight.has_collision(1));
inflight.acknowledge(1);
```

---

## License

Dual-licensed under either of:
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
