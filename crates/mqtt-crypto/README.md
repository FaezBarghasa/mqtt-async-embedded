# `mqtt-crypto`

[![Crates.io](https://img.shields.io/crates/v/mqtt-crypto.svg)](https://crates.io/crates/mqtt-crypto)
[![Documentation](https://docs.rs/mqtt-crypto/badge.svg)](https://docs.rs/mqtt-crypto)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](../../LICENSE-MIT)
[![no_std](https://img.shields.io/badge/no__std-compatible-success.svg)](https://docs.rust-embedded.org/book/intro/no-std.html)
[![Safety: forbid(unsafe_code)](https://img.shields.io/badge/unsafe_code-forbidden-brightgreen.svg)](#)

**`mqtt-crypto`** provides zero-allocation, `no_std` cryptographic acceleration traits and TLS abstractions for the `mqtt-async-embedded` ecosystem.

---

## Features

- **Hardware Crypto Offloading:** `CryptoBackend` trait for delegating cryptographic primitives (SHA-256, AES-128/256-CBC/GCM) to MCU hardware peripherals (e.g. STM32 CRYP/HASH, ESP32 hardware accelerators).
- **TLS Session Abstractions:** `TlsSession` trait decoupling TLS providers (`embedded-tls`, `mbedtls-sys`, `rustls`) from client connection loops.
- **Strict Memory Safety:** `#![no_std]` and `#![forbid(unsafe_code)]`.

---

## License

Dual-licensed under either of:
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
