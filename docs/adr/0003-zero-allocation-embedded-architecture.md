# ADR 0003: Zero-Allocation Embedded QoS State Machine and DMA Streaming

## Status
Accepted

## Context
Embedded microcontrollers (Cortex-M0+, Cortex-M4, ESP32, RISC-V) frequently operate without a dynamic heap allocator (`no_std`, `no_alloc`). Supporting MQTT Quality of Service (QoS 1 and QoS 2) normally requires allocating in-flight message state and buffering retransmissions. Furthermore, high-bandwidth sensors (cameras, microphones, IMUs) cannot fit multi-megabyte payloads in microcontrollers with 32KB RAM.

## Decision
1. **Bounded In-Flight Queue**: Implement `InflightQueue<MAX_INFLIGHT>` using bounded static `heapless::Vec` structures. QoS 1 and QoS 2 message state machines (tracking packet IDs, PUBREC, PUBREL, and PUBCOMP sequences) run entirely on static or stack-allocated memory.
2. **Chunked & DMA Direct Streaming**: Provide `MqttStreamWriter` allowing zero-copy streaming of arbitrarily large payloads directly from hardware DMA buffers or stream chunks over the transport wire without buffering entire messages in memory.

## Consequences
- **Positive**:
  - Full MQTT QoS 0, 1, and 2 protocol compliance in pure `no_std`, `no_alloc` environments.
  - Constant memory footprint known statically at compile time.
  - Zero memory fragmentation risk on long-running IoT edge nodes.
- **Negative**:
  - Maximum concurrent in-flight unacknowledged packets is constrained by the const generic parameter `MAX_INFLIGHT` (default 8).
