# `mqtt-packet`

[![Crates.io](https://img.shields.io/crates/v/mqtt-packet.svg)](https://crates.io/crates/mqtt-packet)
[![Documentation](https://docs.rs/mqtt-packet/badge.svg)](https://docs.rs/mqtt-packet)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](../../LICENSE-MIT)
[![no_std](https://img.shields.io/badge/no__std-compatible-success.svg)](https://docs.rust-embedded.org/book/intro/no-std.html)
[![Safety: forbid(unsafe_code)](https://img.shields.io/badge/unsafe_code-forbidden-brightgreen.svg)](src/lib.rs)

A zero-allocation, `no_std`, `no_alloc` MQTT 3.1.1 and 5.0 packet encoder and decoder engine written in pure Rust (2024 edition).

---

## Features

- **Strictly `no_std` and `no_alloc`**: Operates entirely within caller-provided byte buffers without heap allocation.
- **Dual Protocol Support**: Complete encoding and decoding for both MQTT v3.1.1 and v5.0.
- **Zero Panic Invariant**: Guaranteed by continuous `cargo-fuzz` (`libfuzzer-sys`) and property-based testing (`proptest`).
- **Multi-Packet Framing**: Extract multiple frames in a single network pass using `RawPacketFrameIter`.
- **`defmt` & `nom` Integration**: First-class support for microcontroller logging.

---

## Quick Example

```rust
use mqtt_packet::{Publish, QoS, MqttVersion, EncodePacket, DecodePacket};

let payload = b"{\"sensor\": \"temp\", \"val\": 23.8}";
let mut publish = Publish::new("sensors/telemetry", payload, QoS::AtLeastOnce);
publish.packet_id = Some(42);

let mut buffer = [0u8; 256];
let encoded_len = publish.encode(&mut buffer, MqttVersion::V5).unwrap();

let decoded = Publish::decode(&buffer[..encoded_len], MqttVersion::V5).unwrap();
assert_eq!(decoded.topic, "sensors/telemetry");
assert_eq!(decoded.payload, payload);
```

---

## License

Licensed under either of:
- **MIT License** ([LICENSE-MIT](../../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
- **Apache License, Version 2.0** ([LICENSE-APACHE](../../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

at your option.
