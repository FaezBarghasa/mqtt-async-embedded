# ADR 0005: Zero-Copy DMA Streaming and Pluggable Embedded TLS Architecture

## Status
Accepted

## Context
Resource-constrained microcontrollers (ARM Cortex-M4/M7, STM32, ESP32, RISC-V) have strict memory budgets (often < 64 KB RAM) and specialized peripheral hardware (e.g. DMA controllers for high-rate ADCs, cameras, and network MACs).

Traditional MQTT clients force payloads to be copied into intermediate heap-allocated buffers or fixed RAM buffers prior to packet framing. Furthermore, heavyweight TLS engines like standard `rustls` require significant memory allocation and x86_64-centric architectures unsuitable for bare-metal targets.

## Decision
1. **Zero-Copy DMA Streaming**:
   - Implement `begin_stream_publish` and `MqttStreamWriter` in `mqtt-embedded`.
   - Provide `write_dma_slice` and `write_dma_vectored` to transmit contiguous hardware memory slices (DMA circular buffers, sensor arrays) directly over `embedded-io-async` / `MqttTransport` without intermediate buffer allocation.
2. **Pluggable `TlsTransport` Abstraction**:
   - Define a minimal, zero-allocation `TlsTransport` trait in `mqtt-embedded`.
   - Enable bare-metal developers to seamlessly plug in lightweight embedded TLS engines (`embedded-tls`, `mbedtls-sys`) on MCUs while retaining `tokio-rustls` for `std` edge nodes.
3. **Compile-Time Strict Queue Bounds**:
   - Back inflight queues with `heapless::spsc::Queue<InflightEntry, MAX_INFLIGHT>` ensuring compile-time memory bounds and zero panic guarantees.

## Consequences
- **Positive**:
  - High-frequency sensor and waveform data streams without RAM fragmentation.
  - Full compatibility with STM32 DMA circular buffers and embedded Ethernet/Wi-Fi stacks (`smoltcp`, `embassy-net`).
  - Strict `#![forbid(unsafe_code)]` compliance preserved across all streaming operations.
- **Negative**:
  - Streaming payloads requires caller adherence to chunk size invariants bounded by the total specified length.
