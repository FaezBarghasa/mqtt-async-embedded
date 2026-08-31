# `mqtt-embedded`

Zero-allocation, `no_std`, asynchronous MQTT client for microcontrollers (STM32, ESP32, RISC-V) built for Embassy and `embedded-io-async`.

## Features
- **Strictly `no_std` / `no_alloc`**: Fixed static memory footprint, 0 dynamic allocations.
- **Embedded QoS 1 & QoS 2**: Bounded inflight tracking with compile-time limits.
- **DMA Streaming**: Direct hardware-to-wire streaming without intermediate buffers.
- **Multi-Packet Burst Batching**: High throughput publishing and polling.
- **Pluggable Transports**: TCP via `smoltcp`/`embassy-net`, UART, SPI, and pure-Rust TLS via `embedded-tls`.
- **Zero Panic Guarantee**: `#![forbid(unsafe_code)]`.

## License
Licensed under either of Apache License, Version 2.0 or MIT license at your option.
