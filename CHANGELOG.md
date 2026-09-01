# Changelog

All notable changes to this project are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.6.0] - 2026-09-01

### Added
- **Universal MQTT Engine Layered Workspace**:
  - Introduced `mqtt-core` (`no_std`, `no_alloc`): Pure protocol state machine (`transition`), abstract asynchronous traits (`Transport`, `VectoredTransport`, `ZeroCopyTransport`, `Clock`, `Storage`), collision detection, and unified `MqttError<E>`.
  - Introduced `mqtt-crypto` (`no_std`): Hardware crypto offload interface (`CryptoBackend`) and `TlsSession` traits.
  - Introduced `mqtt-storage` (`no_std`): Compile-time bounded static in-memory store (`StaticMemStore`) and durability interfaces.
- **Protocol Compliance & Mock Broker Test Harness**:
  - In-process mock broker test harness simulating full MQTT v3.1.1 and MQTT v5.0 handshake, QoS 0/1 pub/sub, burst batching, and keep-alive ping cycles (`crates/mqtt-async-embedded/tests/protocol_compliance_tests.rs`).
  - Pure state machine unit test suite verifying deterministic transitions, reconnect backoff cycles, and packet ID collision detection (`crates/mqtt-core/tests/state_machine_tests.rs`).
- **Cargo Workspace Modularization**:
  - Strict layered dependency hierarchy: `codec / crypto / storage ← core ← transport ← client / facade`.

- **Dual Licensing**:
  - Re-licensed all crates and documentation under standard dual licensing: `MIT OR Apache-2.0`.
- **Security & Correctness**:
  - Enforced `#![forbid(unsafe_code)]` across all 5 workspace crates.
  - Implemented `libfuzzer-sys` fuzzing harness in `fuzz/` testing all MQTT 3.1.1 and 5.0 decoders.
  - Expanded `proptest` test suite covering QoS 1 (`PubAck`), QoS 2 (`PubRec`, `PubRel`, `PubComp`), variable-byte integers, and UTF-8 strings.
- **Embedded & Bare-Metal Enhancements**:
  - Zero-copy DMA stream publishing via `begin_stream_publish` and `MqttStreamWriter::write_dma_slice`.
  - Added `TlsTransport` trait for pluggable MCU TLS backends (`embedded-tls`, `mbedtls-sys`).
  - Strict compile-time bounded inflight queues via `heapless::spsc::Queue` and const generics.
- **Redox OS Support & CI Matrix**:
  - Added `x86_64-unknown-redox` to the automated CI target matrix and verified cross-compilation.
- **Curated Example Gallery**:
  - `examples/stm32h7_embassy_mqtt.rs`: Bare-metal STM32H7 DMA ADC stream publishing.
  - `examples/esp32c3_uart_mqtt.rs`: ESP32-C3 / RISC-V UART modem serial transport.
  - `examples/redox_daemon.rs`: Redox OS microkernel gateway background daemon.
  - `examples/slint_dashboard.rs`: Slint UI property and live camera stream bindings.
- **Architecture Decision Records (ADRs)**:
  - Documented ADRs 0001 through 0005 in `docs/adr/`.
- **Performance Benchmarks**:
  - Criterion benchmark suite for packet encoding, decoding, and variable-byte integer codec in `benches/`.

---

## [1.5.1] - 2026-08-31

### Added
- **Standard Tokio Client (`--features tokio-client`)**:
  - Cloneable `AsyncClient` handle and background `EventLoop` driver via `Client::connect(options)`.
  - Fast-path multi-packet batch bursting (`client.publish_batch`).
  - Zero-copy payload buffer integration using `bytes::Bytes`.
- **Cross-Platform OS Drivers**:
  - **Linux**: TCP (`TCP_NODELAY`), pure-Rust TLS (`tokio-rustls`), QUIC (`quinn`), and Unix Domain Sockets (`tokio::net::UnixStream`).
  - **Windows**: TCP, TLS, QUIC, and **Windows Named Pipes** (`pipe://\\.\pipe\mqtt_ipc`).
  - **Android**: TCP, TLS, QUIC, and **Android Abstract Namespace Sockets** (`unix://@android_mqtt_ipc`).
- **Multi-Threaded Data Streams**:
  - `DataStreamProducer`: Concurrent ingestion with atomic sequence IDs and microsecond timestamps.
  - `DataStreamConsumer`: In-order reassembly window (`BTreeMap`), duplicate suppression, and gap detection.
- **Session Data Recovery Engine**:
  - In-flight QoS 1 & 2 retransmission with `DUP = true` upon reconnection.
  - Automatic active subscription restoration on reconnect.
  - Offline queue buffering (`DropOldest`, `ErrorOnFull`, `Block`) during network loss.
  - Sliding recovery journal (`replay_recovery_journal`) for zero data loss.
- **Topic-Filtered Stream Routing**:
  - Trie-based routing engine with wildcard support (`+`, `#`).
  - `client.subscribe_stream(topic, qos)` returning dedicated async `TopicSubscription` streams.
- **MQTT over QUIC & Datagrams**:
  - Stream multiplexing eliminating TCP Head-of-Line blocking.
  - Unreliable zero-handshake datagram telemetry (`client.publish_datagram()`).
- **Web Server Streaming Bridge (Axum & Actix-web)**:
  - `MqttBroadcastHub`: Multi-client fanout hub distributing a single MQTT topic feed to thousands of concurrent HTTP / WebSocket / SSE connections.
  - `CameraMjpegBridge`: Standard `multipart/x-mixed-replace` formatter for direct browser video streaming (`<img>` tags).
  - `TelemetrySseBridge`: Server-Sent Events (SSE) stream bridge for web dashboards.
  - Example: `examples/server_camera_web_bridge.rs`.
- **Slint GUI Client Application Integration (`std` & `no_std`)**:
  - `SlintStreamBinding`: Thread-safe cross-thread UI stream bindings via `bind_slint_property` and `bind_slint_camera`.
  - Seamless zero-allocation `no_std` embedded MCU integration inside Slint display tick loops.
  - Example: `examples/slint_dashboard_app.rs`.
- **Examples & Test Suite**:
  - `examples/tokio_basic_pubsub.rs`, `examples/tokio_reconnect_resilience.rs`, `examples/server_camera_web_bridge.rs`, and `examples/slint_dashboard_app.rs`.
  - Integration test suite in `tests/tokio_client_tests.rs`.

---

## [1.2.0] - 2026-08-18

### Added
- **Real-Time Chunk Streaming**:
  - `StreamMode::RealTimeStreaming` and `MqttOptions::with_stream_mode()`.
  - `begin_stream_publish()` and `MqttStreamWriter` for zero-RAM chunked publishing.
  - Dedicated QUIC telemetry streams (`QuicMqttClient::open_telemetry_stream`).
- **Universal Adapters**: `EmbeddedIoTransport` and `EmbeddedIoSplitTransport` for `embedded-io-async`.
- **ESP32 Support**: ESP32-S series (S2, S3) and ESP32-C series (C2, C3, C6, H2).
- **Transport Accessors**: `transport()`, `transport_mut()`, and `into_transport()`.
- **Microcontroller Logging**: `defmt` derivations across all packets, events, and errors.
- **Protocol Features**:
  - Last Will and Testament (LWT) in `MqttOptions` and `Connect`.
  - `UNSUBSCRIBE` and `UNSUBACK` codecs and `MqttClient::unsubscribe()` API.
  - Packet decoders for `PUBREC`, `PUBREL`, `PUBCOMP`, and `PINGRESP`.

### Fixed
- Fixed bounds check panics on corrupted/truncated MQTT v5 User Properties (`0x26`).
- Fixed bounds check panics when encoding packets into undersized buffers.
- Return explicit `ProtocolError::UnsupportedQoS` on `QoS::ExactlyOnce` in embedded mode.
- Fixed `get_next_packet_id()` initialization sequence.
