# Product Requirements Document (PRD)

Core requirements, hardware targets, and technical constraints for `mqtt-async-embedded`.

---

## 1. Overview & Targets

- **Goal**: Async, zero-allocation (`no_std`) MQTT client for embedded MCUs and edge gateways.
- **Target Hardware**:
  - ESP32-S series (S2, S3 [Xtensa])
  - ESP32-C series (C2, C3, C6, H2 [RISC-V])
  - ARM Cortex-M (M0/M3/M4/M7/M33)
- **Supported Frameworks**: `esp-hal`, `esp-wifi`, `esp-idf-svc`, `embassy-executor`, `embassy-net`.

---

## 2. Functional Requirements

| ID | Requirement | Status |
| :--- | :--- | :--- |
| **FR-01** | MQTT v3.1.1 packet codec | Complete |
| **FR-02** | MQTT v5.0 properties & reason codes (`v5` flag) | Complete |
| **FR-03** | `CONNECT` / `CONNACK` with Last Will & Testament (LWT) | Complete |
| **FR-04** | `PUBLISH` (QoS 0 & QoS 1; QoS 2 rejected with `UnsupportedQoS`) | Complete |
| **FR-05** | `SUBSCRIBE` / `SUBACK` & `UNSUBSCRIBE` / `UNSUBACK` | Complete |
| **FR-06** | Keep-alive heartbeat loop (`PINGREQ` / `PINGRESP`) | Complete |
| **FR-07** | Universal `embedded-io-async` adapters (`EmbeddedIoTransport`) | Complete |
| **FR-08** | Multi-packet burst publish (`publish_batch`) & batch poll (`poll_batch`) | Complete |
| **FR-09** | MQTT over QUIC transport (`MqttQuicTransport` & `QuicMqttClient`) | Complete |
| **FR-10** | Low-latency QUIC datagram telemetry | Complete |
| **FR-11** | Zero-RAM chunk stream publish (`begin_stream_publish` / `MqttStreamWriter`) | Complete |
| **FR-12** | Streaming mode config (`StreamMode::RealTimeStreaming`) | Complete |

---

## 3. Non-Functional Requirements

- **Memory**: 0 dynamic heap allocations (`no_std` / `no_alloc`).
- **RAM Footprint**: Fixed array buffers (512B - 2KB configurable).
- **Execution**: Non-blocking asynchronous I/O compatible with Embassy.
- **Logging**: Zero-overhead `defmt` support.
- **Safety**: Lifetime-bound zero-copy frames without panic on malformed input.

---

## 4. Dependencies

- **Language**: Rust (2024 edition).
- **Core Crates**: `embedded-hal`, `embedded-io-async`, `heapless`, `embassy-time`.
- **Optional Crates**: `defmt`, `embassy-net`, `tokio`, `quinn`, `rustls`.

---

## 5. Roadmap

1. QoS 2 ("Exactly Once") client-side publish handshakes.
2. Native TLS support via `embedded-tls`.
3. Auto-reconnect with exponential backoff on transport drops.
