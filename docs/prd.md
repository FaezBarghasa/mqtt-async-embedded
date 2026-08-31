# Product Requirements Document (PRD)

Core requirements, functional specs, constraints, and roadmap for `mqtt-async-embedded`.

---

## 1. Overview & Dual Modes

- **Embedded Engine (`no_std`, `no_alloc`)**: Zero heap allocations, compile-time static buffers for MCUs (ESP32, Cortex-M, RISC-V).
- **Tokio Host Client (`tokio-client`)**: High-throughput, cross-platform client with multi-threaded data streams and session data recovery.

---

## 2. Functional Requirements Matrix

| ID | Feature | Scope | Status |
| :--- | :--- | :--- | :--- |
| **FR-01** | MQTT v3.1.1 packet codec | All | Complete |
| **FR-02** | MQTT v5.0 properties & reason codes (`v5` flag) | All | Complete |
| **FR-03** | `CONNECT` / `CONNACK` with Last Will & Testament (LWT) | All | Complete |
| **FR-04** | `PUBLISH` (QoS 0, 1; QoS 2 rejected with `UnsupportedQoS` on embedded) | All | Complete |
| **FR-05** | `SUBSCRIBE` / `SUBACK` & `UNSUBSCRIBE` / `UNSUBACK` | All | Complete |
| **FR-06** | Keep-alive heartbeat loop (`PINGREQ` / `PINGRESP`) | All | Complete |
| **FR-07** | Universal `embedded-io-async` adapters (`EmbeddedIoTransport`) | Embedded | Complete |
| **FR-08** | Multi-packet burst publish (`publish_batch`) & batch poll (`poll_batch`) | All | Complete |
| **FR-09** | MQTT over QUIC transport (`MqttQuicTransport` & `QuicMqttClient`) | All | Complete |
| **FR-10** | Low-latency QUIC datagram telemetry (`publish_datagram`) | All | Complete |
| **FR-11** | Zero-RAM chunk stream publish (`begin_stream_publish` / `MqttStreamWriter`) | Embedded | Complete |
| **FR-12** | Streaming mode configuration (`StreamMode::RealTimeStreaming`) | Embedded | Complete |
| **FR-13** | Standard Tokio Client (`Client`, `AsyncClient`, `EventLoop`) | Tokio | Complete |
| **FR-14** | Native OS Drivers (Linux TCP/TLS/Unix, Windows Named Pipes, Android Abstract) | Tokio | Complete |
| **FR-15** | Multi-threaded stream ingestion (`DataStreamProducer`, `DataStreamConsumer`) | Tokio | Complete |
| **FR-16** | Session Data Recovery (In-flight DUP retransmit, auto-resubscribe, offline queue) | Tokio | Complete |
| **FR-17** | Trie-based topic-filtered stream router (`subscribe_stream`) | Tokio | Complete |

---

## 3. Non-Functional Requirements

- **Zero Allocations**: 0 heap allocations on embedded targets (`no_std` / `no_alloc`).
- **RAM Footprint**: Fixed array buffers (512B - 2KB configurable) for MCUs.
- **Host Throughput**: High-throughput zero-copy pipeline backed by `bytes::Bytes`.
- **Concurrency**: Lock-free atomic sequence generation; cancel-safe async futures.
- **Logging**: Zero-overhead `defmt` for MCUs; `tracing` for host environments.
- **Safety**: Panic-free parsing with lifetime-bound zero-copy frames.

---

## 4. Dependencies

- **Language Edition**: Rust (2024 edition).
- **Core Embedded**: `embedded-hal`, `embedded-io-async`, `heapless`, `embassy-time`.
- **Tokio & Host**: `tokio`, `tokio-rustls`, `quinn`, `rustls`, `bytes`, `tracing`, `defmt`.

---

## 5. Roadmap

1. Full broker-side link router implementation (`mqtt-async-broker`).
2. Native WebSockets transport driver (`tokio-tungstenite`).
3. Hardware-accelerated cryptographic offload for embedded TLS.
