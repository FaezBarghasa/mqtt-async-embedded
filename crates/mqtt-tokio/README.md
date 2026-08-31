# `mqtt-tokio`

[![Crates.io](https://img.shields.io/crates/v/mqtt-tokio.svg)](https://crates.io/crates/mqtt-tokio)
[![Documentation](https://docs.rs/mqtt-tokio/badge.svg)](https://docs.rs/mqtt-tokio)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](../../LICENSE-MIT)
[![Safety: forbid(unsafe_code)](https://img.shields.io/badge/unsafe_code-forbidden-brightgreen.svg)](src/lib.rs)

High-performance, asynchronous MQTT host client for Tokio, with QUIC support, zero-copy batching, offline queues, and automatic session recovery.

---

## Features

- **Cloneable Handle**: Lightweight `AsyncClient` and background driver `EventLoop` via `Client::connect(options)`.
- **Zero-Copy Batch Publishing**: Optimized for throughput with `bytes::Bytes`.
- **Topic Subscription Streams**: Trie-based wildcard filtering (`subscribe_stream`) yielding async streams.
- **Session Resilience**: Offline queueing (`DropOldest`, `ErrorOnFull`, `Block`) and in-flight QoS 1/2 recovery.
- **MQTT over QUIC & Smart Fallback**: Low-latency HTTP/3 QUIC connection with seamless fallback to TCP/TLS.
- **Redox OS Verified**: Fully compatible with microkernel architectures (`x86_64-unknown-redox`).

---

## Quick Example

```rust,no_run
use bytes::Bytes;
use mqtt_tokio::{Client, ClientOptions, ReconnectPolicy};
use mqtt_packet::QoS;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = ClientOptions::new("tokio-client", "127.0.0.1", 1883)
        .with_keep_alive(Duration::from_secs(30))
        .with_reconnect(ReconnectPolicy::default());

    let (client, _handle) = Client::connect(options);

    client.publish("sensors/telemetry", QoS::AtLeastOnce, false, Bytes::from_static(b"OK")).await?;
    Ok(())
}
```

---

## License

Licensed under either of:
- **MIT License** ([LICENSE-MIT](../../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
- **Apache License, Version 2.0** ([LICENSE-APACHE](../../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

at your option.
