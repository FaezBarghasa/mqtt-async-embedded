# Product Requirements Document (PRD)

Core requirements, hardware/OS targets, and technical constraints for `mqtt-async-embedded`.

---

## 1. Overview & Operating Modes

- **Goal**: Dual-mode, high-performance MQTT client in Rust (2024 edition):
  1. **Embedded Bare-Metal Engine (`no_std`, `no_alloc`)**: Zero heap allocations for microcontrollers.
  2. **Standard Tokio Client (`tokio-client`)**: High-throughput, cross-platform client with multi-threaded data streams and session data recovery.
- **Target Hardware & Platforms**:
  - **Embedded MCUs**: ESP32-S / ESP32-C / ESP32 Classic, ARM Cortex-M (M0/M3/M4/M7/M33), RISC-V.
  - **Host & Edge Operating Systems**: Linux, Windows, macOS, Android.
- **Supported Frameworks**: `esp-hal`, `esp-wifi`, `esp-idf-svc`, `embassy-executor`, `embassy-net`, `tokio`.

---

## 2. Functional Requirements

| ID | Requirement | Status |
| :--- | :--- | :--- |
| **FR-01** | MQTT v3.1.1 packet codec | Complete |
| **FR-02** | MQTT v5.0 properties & reason codes (`v5` flag) | Complete |
| **FR-03** | `CONNECT` / `CONNACK` with Last Will & Testament (LWT) | Complete |
| **FR-04** | `PUBLISH` (QoS 0 & QoS 1; QoS 2 rejected with `UnsupportedQoS` on embedded) | Complete |
| **FR-05** | `SUBSCRIBE` / `SUBACK` & `UNSUBSCRIBE` / `UNSUBACK` | Complete |
| **FR-06** | Keep-alive heartbeat loop (`PINGREQ` / `PINGRESP`) | Complete |
| **FR-07** | Universal `embedded-io-async` adapters (`EmbeddedIoTransport`) | Complete |
| **FR-08** | Multi-packet burst publish (`publish_batch`) & batch poll (`poll_batch`) | Complete |
| **FR-09** | MQTT over QUIC transport (`MqttQuicTransport` & `QuicMqttClient`) | Complete |
| **FR-10** | Low-latency QUIC datagram telemetry (`publish_datagram`) | Complete |
| **FR-11** | Zero-RAM chunk stream publish (`begin_stream_publish` / `MqttStreamWriter`) | Complete |
| **FR-12** | Streaming mode config (`StreamMode::RealTimeStreaming`) | Complete |
| **FR-13** | Standard Tokio Client (`Client`, `AsyncClient`, `EventLoop`) | Complete |
| **FR-14** | Cross-platform OS Drivers (Linux, Windows Named Pipes, Android Abstract Sockets) | Complete |
| **FR-15** | Multi-threaded data stream producers & consumers (`DataStreamProducer`, `DataStreamConsumer`) | Complete |
| **FR-16** | Session Data Recovery Engine (QoS 1/2 resend with `DUP`, auto-resubscription, offline queue) | Complete |
| **FR-17** | Trie-based topic-filtered stream router (`subscribe_stream`) | Complete |

---

## 3. Non-Functional Requirements

- **Embedded Memory**: 0 dynamic heap allocations (`no_std` / `no_alloc`).
- **RAM Footprint**: Fixed array buffers (512B - 2KB configurable) for embedded targets.
- **Desktop / Edge Throughput**: Multi-million messages/sec zero-copy pipeline with `bytes::Bytes`.
- **Concurrency**: Lock-free sequence generation and cancel-safe async futures.
- **Logging**: Zero-overhead `defmt` for MCUs, `tracing` for Tokio host environments.
- **Safety**: Lifetime-bound zero-copy frames without panic on malformed input.

---

## 4. Dependencies

- **Language**: Rust (2024 edition).
- **Core Embedded Crates**: `embedded-hal`, `embedded-io-async`, `heapless`, `embassy-time`.
- **Optional Crates**: `defmt`, `embassy-net`, `tokio`, `tokio-rustls`, `quinn`, `rustls`, `bytes`, `tracing`.

---

## 5. Roadmap

1. Full broker-side link router implementation (`mqtt-async-broker`).
2. Native WebSockets transport driver (`tokio-tungstenite`).
3. Hardware-accelerated cryptographic offload for embedded TLS.
