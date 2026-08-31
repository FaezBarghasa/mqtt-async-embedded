# `mqtt-packet`

Zero-allocation, `no_std`, `no_alloc` MQTT 3.1.1 and 5.0 packet encoder and decoder engine in pure Rust.

## Features
- **Strictly `no_std` and `no_alloc`**: Operates entirely within caller-provided byte buffers.
- **Dual Protocol Support**: MQTT v3.1.1 and v5.0.
- **Zero Panics (`#![forbid(unsafe_code)]`)**: Fully fuzzed and verified with property-based testing.
- **Zero-Copy Frame Stream Iterator**: Extract multiple packets in a single network pass via `RawPacketFrameIter`.
- **Optional `defmt` & `nom` support**.

## License
Licensed under either of Apache License, Version 2.0 or MIT license at your option.
