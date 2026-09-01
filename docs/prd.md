# Product Requirements Document (PRD): Universal MQTT Engine

Core requirements, functional specs, constraints, and architecture for `mqtt-async-embedded`.

---

## 1. Overview & Architectural Layers

- **Protocol Core (`mqtt-core`)**: Pure `no_std`, `no_alloc` state machine, foundational traits, and hierarchical error model.
- **Packet Codec (`mqtt-packet`)**: Zero-allocation MQTT v3.1.1 and v5.0 packet serializer and parser.
- **Crypto & Security (`mqtt-crypto`)**: Hardware cryptographic accelerator offloading and TLS session abstractions.
- **Storage & Persistence (`mqtt-storage`)**: Static in-memory buffers and durable storage abstractions.
- **Embedded Engine (`mqtt-embedded`)**: Embassy async tasks, zero-copy DMA ring buffers, and MCU streaming for STM32, ESP32, RISC-V.
- **Host Engine (`mqtt-tokio`)**: High-throughput, cross-platform client with session data recovery across Linux, Windows, Android, and Redox OS.
- **Bridges & Integration (`mqtt-bridges`)**: Web server streaming bridges (Axum, Actix, SSE, MJPEG) and Slint GUI event loops.
- **Facade (`mqtt-async-embedded`)**: Top-level umbrella crate with feature toggles and backward compatibility.

---

## 2. Functional Requirements Matrix

| ID | Feature | Scope | Status |
| :--- | :--- | :--- | :--- |
| **FR-01** | MQTT v3.1.1 packet codec | Codec | Complete |
| **FR-02** | MQTT v5.0 properties & reason codes (`v5` flag) | Codec | Complete |
| **FR-03** | Pure deterministic protocol state machine (`transition`) | Core | Complete |
| **FR-04** | In-flight packet collision detection & QoS 1/2 tracking | Core | Complete |
| **FR-05** | Foundational transport abstractions (`Transport`, `VectoredTransport`, `ZeroCopyTransport`) | Core | Complete |
| **FR-06** | Hardware crypto acceleration interface (`CryptoBackend`) | Crypto | Complete |
| **FR-07** | Static bounded in-memory session store (`StaticMemStore`) | Storage | Complete |
| **FR-08** | Multi-packet burst publish (`publish_batch`) & batch poll (`poll_batch`) | Embedded | Complete |
| **FR-09** | MQTT over QUIC transport (`MqttQuicTransport` & `QuicMqttClient`) | Tokio/Embedded | Complete |
| **FR-10** | Low-latency QUIC datagram telemetry (`publish_datagram`) | Tokio | Complete |
| **FR-11** | Zero-RAM chunk stream publish (`begin_stream_publish` / `MqttStreamWriter`) | Embedded | Complete |
| **FR-12** | Direct DMA slice streaming (`write_dma_slice`, `write_dma_vectored`) | Embedded | Complete |
| **FR-13** | Standard Tokio Client (`Client`, `AsyncClient`, `EventLoop`) | Tokio | Complete |
| **FR-14** | Native OS Drivers (Linux TCP/TLS/Unix, Windows Named Pipes, Android Abstract, Redox OS) | Tokio | Complete |
| **FR-15** | Multi-threaded stream ingestion (`DataStreamProducer`, `DataStreamConsumer`) | Tokio | Complete |
| **FR-16** | Session Data Recovery (In-flight DUP retransmit, auto-resubscribe, offline queue) | Tokio | Complete |
| **FR-17** | Trie-based topic-filtered stream router (`subscribe_stream`) | Tokio | Complete |
| **FR-18** | Web Server Streaming Bridge (Axum, Actix-web, MJPEG, SSE) | Bridges | Complete |
| **FR-19** | Slint GUI Client Application Integration (`std` & `no_std`) | Bridges | Complete |
| **FR-20** | Pluggable MCU TLS Backend Trait (`TlsTransport`) | Embedded | Complete |
| **FR-21** | In-process mock broker test harness & Protocol Compliance validation | Tests | Complete |

---

## 3. Non-Functional Requirements

- **Zero Allocations**: 0 heap allocations on embedded targets (`no_std` / `no_alloc`).
- **RAM Footprint**: Fixed array buffers (512B - 2KB configurable) for microcontrollers.
- **Safety Invariant**: Strictly enforced `#![forbid(unsafe_code)]` across all workspace crates with automated CI checks.
- **Host Throughput**: High-throughput zero-copy pipeline backed by `bytes::Bytes`.
- **Concurrency**: Lock-free atomic sequence generation; cancel-safe async futures.
- **Logging**: Zero-overhead `defmt` for MCUs; `tracing` and `log` for host environments.

---

## 4. Dependencies & Targets

- **Language Edition**: Rust (2024 edition).
- **Core Embedded**: `embedded-hal`, `embedded-io-async`, `heapless`, `embassy-time`.
- **Tokio & Host**: `tokio`, `tokio-rustls`, `quinn`, `rustls`, `bytes`, `tracing`, `defmt`.
- **Verified Cross-Targets**:
  - `thumbv7em-none-eabihf` (ARM Cortex-M4F / Cortex-M7)
  - `thumbv6m-none-eabi` (ARM Cortex-M0 / Cortex-M0+)
  - `riscv32imc-unknown-none-elf` (ESP32-C2 / ESP32-C3)
  - `riscv32imac-unknown-none-elf` (ESP32-C6 / ESP32-H2)
  - `x86_64-unknown-redox` (Redox OS Microkernel)
  - Host OS: Linux, Windows, macOS.
