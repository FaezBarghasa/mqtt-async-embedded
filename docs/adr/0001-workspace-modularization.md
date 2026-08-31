# ADR 0001: Cargo Workspace Modularization

## Status
Accepted

## Context
Originally, `mqtt-async-embedded` was structured as a single monolithic crate trying to support bare-metal `no_std` microcontrollers, host-side Tokio runtimes, Web streaming bridges (Axum/Actix-web), and Slint UI bindings concurrently.

This monolithic layout had several drawbacks:
1. Bare-metal embedded developers had to clone and parse heavy dependencies (`tokio`, `quinn`, `slint`, `axum`) even if unused.
2. Codebase navigation and clean architectural separation of concerns were difficult.
3. Feature flag explosion led to brittle conditional compilation paths.

## Decision
Split the project into a modular Cargo workspace with focused sub-crates:
- **`mqtt-packet`**: Pure zero-allocation `no_std` & `no_alloc` MQTT v3.1.1 and v5 packet encoder/decoder and proptest suite.
- **`mqtt-embedded`**: Bare-metal `no_std` Embassy async MQTT client with bounded heapless in-flight queues and zero-RAM direct DMA stream writers.
- **`mqtt-tokio`**: Production-grade async MQTT client for host systems (Linux, macOS, Windows) with offline queueing, topic routing, session recovery, and QUIC/TCP smart fallback.
- **`mqtt-bridges`**: Web server integration (Axum, Actix-web, MJPEG multipart, SSE) and Slint UI bindings.
- **`mqtt-async-embedded`**: Facade umbrella crate maintaining 100% backward compatibility for existing users.

## Consequences
- **Positive**:
  - Embedded microcontrollers depend only on `mqtt-packet` and `mqtt-embedded` with zero bloat.
  - Clear separation between wire format, MCU client state machine, host client runtime, and high-level UI/web bridges.
  - Sub-crates can be released and versioned independently on crates.io.
- **Negative**:
  - Requires maintaining inter-crate dependency paths in the workspace during development.
