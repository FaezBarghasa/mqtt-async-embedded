# Technical Design Document (TDD): Universal MQTT Engine

System architecture, layered subsystem contracts, transport abstractions, protocol state machines, and verification strategy for `mqtt-async-embedded`.

---

## 1. System Architecture

```
+----------------------------------------------------------------------------------------------------+
|                                    Application (Embedded Task / OS Worker)                        |
+----------------------------------------------------------------------------------------------------+
|                      mqtt-async-embedded (Unified Root Facade)                                     |
|  +-------------------------------------+   +----------------------------------------------------+  |
|  |  MqttClient (no_std, Embassy)       |   |  Tokio AsyncClient / Client (rumqttc compatible)   |  |
|  |   - Zero-alloc bounded buffers      |   |   - bytes::Bytes zero-copy sharing                 |  |
|  |   - Direct DMA sensor streaming     |   |   - Topic prefix trie stream routing               |  |
|  |   - Multi-packet batching           |   |   - Dynamic offline queue & journal replay         |  |
|  +-------------------------------------+   +----------------------------------------------------+  |
+----------------------------------------------------------------------------------------------------+
                                                  |
                                                  v
+----------------------------------------------------------------------------------------------------+
|                                    mqtt-core (no_std, no_alloc)                                    |
|  +------------------------------------+  +----------------------------------+  +----------------+  |
|  |  Pure State Machine (transition)   |  |  InflightQueue (Collision Guard) |  |  MqttError<E>  |  |
|  |   - Disconnected / Connecting      |  |   - O(1) packet-ID index         |  |   - Transport  |  |
|  |   - WaitingForConnAck / Connected  |  |   - Stash & out-of-order ACK     |  |   - Protocol   |  |
|  |   - Disconnecting / Reconnecting   |  |   - QoS 1 & QoS 2 state flow     |  |   - Codec      |  |
|  +------------------------------------+  +----------------------------------+  +----------------+  |
|  +----------------------------------------------------------------------------------------------+  |
|  |  Foundational Traits: Transport | VectoredTransport | ZeroCopyTransport | Clock | Storage    |  |
|  +----------------------------------------------------------------------------------------------+  |
+----------------------------------------------------------------------------------------------------+
              /                                   |                                   \
             v                                    v                                    v
+---------------------------+       +---------------------------+       +---------------------------+
|  mqtt-packet (Codec)      |       |  mqtt-crypto              |       |  mqtt-storage             |
|  - Zero-copy PacketRef    |       |  - CryptoBackend (AES/SHA)|       |  - StaticMemStore         |
|  - Fast Varint Hot Path   |       |  - TlsSession Abstraction |       |  - Flash / Disk Engines   |
|  - MQTT 3.1.1 & 5.0 types |       +---------------------------+       +---------------------------+
+---------------------------+
```

---

## 2. Core Modules & Workspace Crates

### 2.1. `mqtt-core` (`no_std`, `no_alloc`)
- **Foundational Traits**:
  - `Transport`: Async byte stream abstraction (`send`, `recv`).
  - `VectoredTransport`: Scatter-gather vectored writes (`send_vectored`) for syscall reduction.
  - `ZeroCopyTransport`: Receive zero-copy buffers directly from DMA rings.
  - `Clock`: Abstract monotonic clock for timeout management and backoff jitter.
  - `Storage`: Key-value durability trait for session recovery.
- **Pure State Machine**: `transition(ConnState, StateEvent) -> Result<(ConnState, StateAction), ProtocolError>` without physical I/O dependencies.
- **In-flight Collision Tracker**: `InflightQueue<N>` with $O(1)$ packet-ID index, duplicate detection, and QoS 1/2 tracking.
- **Unified Error Model**: `MqttError<E>` providing standard hierarchical error mapping.

### 2.2. `mqtt-packet` (`no_std`, `no_alloc`)
- **`DecodePacket` & `EncodePacket` traits**: Zero-allocation encoding and decoding within user-provided slices.
- **`RawPacketFrameIter`**: Zero-copy packet streaming iterator over continuous byte streams.
- **`properties`**: MQTT 5.0 properties parsing with strict bounds checks.
- **Fuzzing & Proptest**: `fuzz/fuzz_targets/fuzz_packet_decode.rs` and `tests/proptest_codec.rs`.

### 2.3. `mqtt-crypto` (`no_std`)
- **`CryptoBackend` trait**: Hardware crypto accelerator offload (SHA-256, AES-128/256-CBC/GCM).
- **`TlsSession` trait**: Pluggable TLS session state abstraction.

### 2.4. `mqtt-storage` (`no_std`)
- **`StaticMemStore<MAX_ENTRIES, MAX_KEY_LEN, MAX_VAL_LEN>`**: Zero-alloc in-memory persistent buffer.
- **Storage Extensibility**: Flash wear-leveling ring storage and disk-backed persistence adapters.

### 2.5. `mqtt-embedded` (`no_std`, `no_alloc`)
- **`MqttOptions<'a>`**: Broker endpoint, keep-alive, clean session, LWT, credentials.
- **`MqttClient<'a, T, MAX_TOPICS, BUF_SIZE, MAX_INFLIGHT>`**:
  - `publish(topic, payload, qos)`: Single packet write.
  - `publish_batch(&[PublishMessage])`: Packs multiple messages into one network write.
  - `subscribe(&[(&str, QoS)])` / `unsubscribe(&[&str])`: Sends subscription requests.
  - `poll()` / `poll_batch()`: Parses RX buffer, returns zero-copy `MqttEvent<'p>`.
  - `begin_stream_publish(topic, total_len, qos)`: Direct-to-wire chunked streaming.
  - `MqttStreamWriter::write_dma_slice(slice)`: Zero-copy DMA buffer streaming.
- **`TlsTransport`**: Pluggable MCU TLS abstraction for `embedded-tls` / `mbedtls-sys`.

### 2.6. `mqtt-tokio` (`std`)
- **`Client` / `AsyncClient` / `EventLoop`**:
  - `Client::connect(options)`: Spawns background driver task.
  - `publish(topic, qos, retain, payload)`: Zero-copy publish via `bytes::Bytes`.
  - `subscribe_stream(topic, qos)`: Topic-filtered stream backed by a prefix trie.
  - `SmartTransport`: Automatic QUIC to TCP/TLS fallback.
  - Session Data Recovery: Offline queueing (`DropOldest`, `ErrorOnFull`, `Block`) and in-flight retransmission.
- **Target OS Compatibility**: Linux, Windows, Android, macOS, and Redox OS (`x86_64-unknown-redox`).

### 2.7. `mqtt-bridges` (`std`)
- **`MqttBroadcastHub`**: Subscribes once to MQTT topic and broadcasts to unbounded HTTP/SSE connections via `tokio::sync::broadcast`.
- **`CameraMjpegBridge`**: Prepares `multipart/x-mixed-replace; boundary=frame` chunks for streaming directly into HTML `<img>` elements.
- **`TelemetrySseBridge`**: Formats MQTT payloads into standard SSE lines (`data: <payload>\n\n`).
- **`SlintStreamBinding`**: Dispatches incoming MQTT payloads to Slint UI event loops safely across thread boundaries.

---

## 3. Verification Suite

| Test Target | Files | Coverage |
| :--- | :--- | :--- |
| **Core State Machine** | `crates/mqtt-core/tests/state_machine_tests.rs` | Deterministic state transitions, reconnect backoff counters, packet ID collision guards |
| **Protocol Compliance** | `crates/mqtt-async-embedded/tests/protocol_compliance_tests.rs` | In-process mock broker test harness for MQTT v3.1.1 & v5.0 handshakes, QoS 0/1 pub/sub, burst batching |
| **Packet Codec & Bounds** | `tests/engine_tests.rs`, `crates/mqtt-packet/tests/proptest_codec.rs` | Varint encoding, packet roundtrips, malformed bounds safety, random bytes fuzzing |
| **Fuzzing Harness** | `fuzz/fuzz_targets/fuzz_packet_decode.rs` | `libfuzzer-sys` continuous decoder fuzzing for v3.1.1 & v5 |
| **Embedded Client Logic** | `tests/client_tests.rs` | Mock transport, handshake, burst publish, auto-`PUBACK`, zero-copy DMA streaming |
| **Tokio Host & Driver** | `tests/tokio_client_tests.rs` | Batch publish, stream routing, reconnect recovery, offline queue, Slint binding |
| **Performance Benchmarks**| `benches/benches/codec_benchmarks.rs` | Criterion throughput benchmarks for encoding, decoding, and varints |

---

## 4. Reference Examples

- `examples/stm32h7_embassy_mqtt.rs`: Bare-metal STM32H7 DMA ADC stream publishing with Embassy.
- `examples/esp32c3_uart_mqtt.rs`: ESP32-C3 / RISC-V UART modem serial transport.
- `examples/redox_daemon.rs`: Redox OS microkernel gateway background daemon.
- `examples/slint_dashboard.rs`: Slint UI property & live camera stream binding.
- `examples/esp32_wifi_embassy.rs`: Bare-metal ESP32 Wi-Fi task.
- `examples/realtime_stream.rs`: Zero-RAM sensor chunk publishing.
- `examples/quic_client.rs`: Unreliable QUIC telemetry datagrams.
- `examples/tokio_basic_pubsub.rs`: Tokio pub/sub with topic router.
- `examples/tokio_reconnect_resilience.rs`: Tokio connection loss & journal replay.
- `examples/server_camera_web_bridge.rs`: Axum / Actix MJPEG video and SSE telemetry bridge.
- `examples/slint_dashboard_app.rs`: Slint desktop/embedded dashboard with live MQTT bindings.
