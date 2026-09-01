# `mqtt-storage`

[![Crates.io](https://img.shields.io/crates/v/mqtt-storage.svg)](https://crates.io/crates/mqtt-storage)
[![Documentation](https://docs.rs/mqtt-storage/badge.svg)](https://docs.rs/mqtt-storage)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](../../LICENSE-MIT)
[![no_std](https://img.shields.io/badge/no__std-compatible-success.svg)](https://docs.rust-embedded.org/book/intro/no-std.html)
[![Safety: forbid(unsafe_code)](https://img.shields.io/badge/unsafe_code-forbidden-brightgreen.svg)](#)

**`mqtt-storage`** provides persistent storage abstractions and static in-memory buffers for MQTT session state, subscriptions, and offline message queuing.

---

## Features

- **Static In-Memory Key-Value Store:** `StaticMemStore<MAX_ENTRIES, MAX_KEY_LEN, MAX_VAL_LEN>` with compile-time bounded heapless buffers.
- **Embedded & Host Persistence:** Ready to interface with embedded flash (`embedded-storage`) and disk-backed engines (`redb`, `sled`).
- **Strict Memory Safety:** `#![no_std]` and `#![forbid(unsafe_code)]`.

---

## License

Dual-licensed under either of:
- MIT License ([LICENSE-MIT](../../LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](../../LICENSE-APACHE))
