# `mqtt-embedded`

[![Crates.io](https://img.shields.io/crates/v/mqtt-embedded.svg)](https://crates.io/crates/mqtt-embedded)
[![Documentation](https://docs.rs/mqtt-embedded/badge.svg)](https://docs.rs/mqtt-embedded)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](../../LICENSE-MIT)
[![no_std](https://img.shields.io/badge/no__std-compatible-success.svg)](https://docs.rust-embedded.org/book/intro/no-std.html)
[![Safety: forbid(unsafe_code)](https://img.shields.io/badge/unsafe_code-forbidden-brightgreen.svg)](src/lib.rs)

A zero-allocation, `no_std`, `no_alloc` asynchronous MQTT client for microcontrollers (STM32, ESP32, RISC-V) and Embassy.

---

## Features

- **Strictly `no_std` / `no_alloc`**: Operates in fixed static memory with zero heap fragmentation.
- **Embedded QoS 1 & QoS 2**: Bounded in-flight queue (`InflightQueue`) with const-generic limits (`MAX_INFLIGHT`).
- **Zero-Copy DMA Streaming**: Stream circular DMA ADC/sensor buffers directly over the wire via `begin_stream_publish` and `MqttStreamWriter`.
- **Multi-Packet Burst Batching**: Publish and poll multiple packets in a single network pass.
- **Pluggable Transports**: Adapters for `embedded-io-async`, `smoltcp` / `embassy-net` TCP, and `TlsTransport` for embedded TLS.
- **Microcontroller Logging**: Native `defmt` support.

---

## Quick Example

```rust,no_run
use embassy_time::Duration;
use mqtt_embedded::client::{MqttClient, MqttOptions};
use mqtt_embedded::transport::EmbeddedIoTransport;
use mqtt_packet::QoS;

// Initialize client with 4 in-flight slots and a 1KB static packet buffer
let options = MqttOptions::new("stm32-node", "192.168.1.10", 1883)
    .with_keep_alive(Duration::from_secs(30));

let transport = EmbeddedIoTransport::new(my_socket);
let mut client: MqttClient<_, 4, 1024> = MqttClient::new(transport, options);

client.connect().await.unwrap();
client.publish("telemetry/status", b"ONLINE", QoS::AtLeastOnce).await.unwrap();
```

---

## License

Licensed under either of:
- **MIT License** ([LICENSE-MIT](../../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
- **Apache License, Version 2.0** ([LICENSE-APACHE](../../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

at your option.
