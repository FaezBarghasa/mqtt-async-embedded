# Product Requirements Document (PRD)

## 1. Product Overview & Purpose

`mqtt-async-embedded` is an asynchronous, zero-allocation (`no_std`) MQTT client library written in Rust. It enables resource-constrained microcontrollers running bare-metal software or micro-OS environments (such as Embassy) to communicate securely and reliably with MQTT message brokers.

---

## 2. Target Persona & Use Cases

- **Target Audience**: Embedded Rust Engineers, IoT Firmware Developers, Embedded Systems Architects.
- **Primary Technical Environments**: ARM Cortex-M, ESP32, RISC-V, Nordic nRF, STM32 microcontrollers.
- **Key Use Cases**:
  - Telemetry collection from industrial sensor nodes over cellular or Wi-Fi modems.
  - Smart home device control with low power consumption requirements.
  - Embedded edge devices requiring non-blocking network I/O with strict RAM limits.

---

## 3. High-Level Requirements & Features

### 3.1. Functional Requirements

| ID | Requirement Description | Priority | Status |
| :--- | :--- | :--- | :--- |
| **FR-01** | Support MQTT v3.1.1 control packet encoding/decoding | High | Implemented |
| **FR-02** | Support MQTT v5.0 specification via `v5` feature flag | High | Implemented |
| **FR-03** | Provide connection establishment (`CONNECT` / `CONNACK` handshake) with Last Will and Testament (LWT) | High | Implemented |
| **FR-04** | Support message publishing (`PUBLISH`) with QoS 0 & QoS 1 and defensive QoS 2 validation | High | Implemented |
| **FR-05** | Support topic subscriptions (`SUBSCRIBE` / `SUBACK`) and dynamic unsubscriptions (`UNSUBSCRIBE` / `UNSUBACK`) | High | Implemented |
| **FR-06** | Automatic heartbeat transmission (`PINGREQ` / `PINGRESP`) during idle polling | High | Implemented |
| **FR-07** | Hardware-agnostic transport interface (`MqttTransport` trait) | High | Implemented |
| **FR-08** | Multi-packet burst sending (`publish_batch`) and polling (`poll_batch`) | High | Implemented |
| **FR-09** | MQTT over QUIC / HTTP/3 transport interface (`MqttQuicTransport` & `QuicMqttClient`) | High | Implemented |
| **FR-10** | Real-time zero-overhead telemetry datagrams | High | Implemented |

### 3.2. Non-Functional Requirements

| ID | Category | Requirement Description | Target Metric |
| :--- | :--- | :--- | :--- |
| **NFR-01** | **Memory** | Completely `no_std` and `no_alloc` compliant | 0 dynamic allocations on heap |
| **NFR-02** | **Performance** | Asynchronous non-blocking I/O using native async traits | Compatible with Embassy executor |
| **NFR-03** | **Footprint** | Minimal RAM usage with static heapless buffers | Configurable array sizes (e.g. 512B - 2KB) |
| **NFR-04** | **Observability**| Support `defmt` logging framework for low-overhead micro-controller logging | Feature flag `defmt` |
| **NFR-05** | **Reliability** | Strict lifetime management for zero-copy event handling | Compiler-verified borrow semantics |
| **NFR-06** | **Throughput** | Zero-copy vectored I/O and multi-packet stream parsing | Multi-packet frame batching |

---

## 4. Dependencies & Technical Constraints

- **Language**: Rust (2024 edition).
- **Core Dependencies**:
  - `embedded-hal` / `embedded-hal-async` (v1.0)
  - `embedded-io-async` (v0.6)
  - `heapless` (v0.9)
  - `embassy-time` (v0.5)
- **Optional Dependencies**: `defmt`, `embassy-net`, `nom`, `tokio`, `quinn`, `rustls`.

---

## 5. Success Metrics & Future Roadmap

### Success Criteria
1. Zero dynamic allocations across all communication cycles.
2. Multi-packet burst batching significantly reduces transmission latency.
3. Clean compilation for `no_std` embedded targets, `std` host machines, and QUIC/H3 networks.

### Future Enhancements
- QoS 2 ("Exactly Once") message delivery support.
- Native TLS integration over `embedded-tls`.
- Automated retry mechanism with exponential backoff on transport failure.

