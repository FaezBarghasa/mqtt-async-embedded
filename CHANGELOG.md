# Changelog

All notable changes to this project are documented here.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
