# ADR 0001: Layered Cargo Workspace Architecture for Universal MQTT Engine

## Status
Accepted (Updated for Universal Architecture)

## Context
Originally, `mqtt-async-embedded` was structured as a single monolithic crate trying to support bare-metal `no_std` microcontrollers, host-side Tokio runtimes, Web streaming bridges (Axum/Actix-web), and Slint UI bindings concurrently.

To achieve unmatched performance across both `no_std` microcontrollers and high-throughput `std` cloud/desktop/Redox environments without code duplication, a strict layered architecture is required:
`codec / crypto / storage ← core ← transport (embedded / std) ← client / facade`.

## Decision
Organize the project into a modular, strictly layered Cargo workspace:
- **`mqtt-core`**: Pure `no_std`, `no_alloc` protocol state machine, foundational traits (`Transport`, `VectoredTransport`, `ZeroCopyTransport`, `Clock`, `Storage`), collision detection, and unified `MqttError<E>`.
- **`mqtt-packet`**: Pure zero-allocation `no_std` & `no_alloc` MQTT v3.1.1 and v5 packet encoder/decoder and proptest suite.
- **`mqtt-crypto`**: Pure `no_std` hardware crypto offloading (`CryptoBackend`) and TLS session abstractions.
- **`mqtt-storage`**: Pure `no_std` static memory stores and durable session storage traits.
- **`mqtt-embedded`**: Bare-metal `no_std` Embassy async MQTT client with bounded heapless in-flight queues and zero-RAM direct DMA stream writers.
- **`mqtt-tokio`**: Production-grade async MQTT client for host systems (Linux, Windows, Android, Redox) with offline queueing, topic routing, session recovery, and QUIC/TCP smart fallback.
- **`mqtt-bridges`**: Web server integration (Axum, Actix-web, MJPEG multipart, SSE) and Slint UI bindings.
- **`mqtt-async-embedded`**: Facade umbrella crate maintaining 100% backward compatibility for existing users.

## Consequences
- **Positive**:
  - Embedded microcontrollers depend only on pure `no_std` sub-crates (`mqtt-core`, `mqtt-packet`, `mqtt-embedded`) with zero allocation overhead and zero bloat.
  - State machine transitions are 100% testable without physical network I/O.
  - Strict `#![forbid(unsafe_code)]` enforced across all crates.
  - Sub-crates can be consumed independently or through the unified umbrella facade.
- **Negative**:
  - Requires maintaining inter-crate dependency paths in the workspace during development.
