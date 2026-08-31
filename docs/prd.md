# Product Requirements Document (PRD)

Core requirements, functional specs, constraints, and architecture for `mqtt-async-embedded`.

---

## 1. Overview & Dual Modes

- **Embedded Engine (`no_std`, `no_alloc`)**: Zero heap allocations, compile-time static buffers for MCUs (STM32, ESP32, RISC-V).
- **Tokio Host Client (`mqtt-tokio`)**: High-throughput, cross-platform client with multi-threaded data streams and session data recovery across Linux, Windows, Android, and Redox OS.

---

## 2. Functional Requirements Matrix

| ID | Feature | Scope | Status |
| :--- | :--- | :--- | :--- |
| **FR-01** | MQTT v3.1.1 packet codec | All | Complete |
| **FR-02** | MQTT v5.0 properties & reason codes (`v5` flag) | All | Complete |
| **FR-03** | `CONNECT` / `CONNACK` with Last Will & Testament (LWT) | All | Complete |
| **FR-04** | `PUBLISH` (QoS 0, 1; QoS 2 full codec & state machine) | All | Complete |
| **FR-05** | `SUBSCRIBE` / `SUBACK` & `UNSUBSCRIBE` / `UNSUBACK` | All | Complete |
| **FR-06** | Keep-alive heartbeat loop (`PINGREQ` / `PINGRESP`) | All | Complete |
| **FR-07** | Universal `embedded-io-async` adapters (`EmbeddedIoTransport`) | Embedded | Complete |
| **FR-08** | Multi-packet burst publish (`publish_batch`) & batch poll (`poll_batch`) | All | Complete |
| **FR-09** | MQTT over QUIC transport (`MqttQuicTransport` & `QuicMqttClient`) | All | Complete |
| **FR-10** | Low-latency QUIC datagram telemetry (`publish_datagram`) | All | Complete |
| **FR-11** | Zero-RAM chunk stream publish (`begin_stream_publish` / `MqttStreamWriter`) | Embedded | Complete |
| **FR-12** | Direct DMA slice streaming (`write_dma_slice`, `write_dma_vectored`) | Embedded | Complete |
| **FR-13** | Standard Tokio Client (`Client`, `AsyncClient`, `EventLoop`) | Tokio | Complete |
| **FR-14** | Native OS Drivers (Linux TCP/TLS/Unix, Windows Named Pipes, Android Abstract, Redox OS) | Tokio | Complete |
| **FR-15** | Multi-threaded stream ingestion (`DataStreamProducer`, `DataStreamConsumer`) | Tokio | Complete |
| **FR-16** | Session Data Recovery (In-flight DUP retransmit, auto-resubscribe, offline queue) | Tokio | Complete |
| **FR-17** | Trie-based topic-filtered stream router (`subscribe_stream`) | Tokio | Complete |
| **FR-18** | Web Server Streaming Bridge (Axum, Actix-web, MJPEG, SSE) | Tokio | Complete |
| **FR-19** | Slint GUI Client Application Integration (`std` & `no_std`) | All | Complete |
| **FR-20** | Pluggable MCU TLS Backend Trait (`TlsTransport`) | Embedded | Complete |
| **FR-21** | Continuous Fuzzing (`libfuzzer-sys`) & Proptest Codec Validation | All | Complete |

---

## 3. Non-Functional Requirements

- **Zero Allocations**: 0 heap allocations on embedded targets (`no_std` / `no_alloc`).
- **RAM Footprint**: Fixed array buffers (512B - 2KB configurable) for MCUs.
- **Safety Invariant**: Strictly enforced `#![forbid(unsafe_code)]` across all 5 workspace crates.
- **Host Throughput**: High-throughput zero-copy pipeline backed by `bytes::Bytes`.
- **Concurrency**: Lock-free atomic sequence generation; cancel-safe async futures.
- **Logging**: Zero-overhead `defmt` for MCUs; `tracing` for host environments.

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
