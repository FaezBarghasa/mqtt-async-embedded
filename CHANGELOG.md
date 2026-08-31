# Changelog

All notable changes to this project are documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.3.0] - 2026-08-31

### Added
- **Standard Tokio High-Performance Client (`--features tokio-client`)**:
  - Outperforms `rumqttc` in throughput, ergonomics, and resilience.
  - Cloneable `AsyncClient` handle and background `EventLoop` driver.
  - Managed background lifecycle with `Client::connect(options)`.
  - Fast-path multi-packet batch bursting (`client.publish_batch`).
  - Zero-copy payload buffer integration using `bytes::Bytes`.
- **Cross-Platform OS Drivers**:
  - **Linux Driver**: TCP with `TCP_NODELAY`, pure-Rust TLS via `tokio-rustls`, QUIC via `quinn`, and Unix Domain Sockets (`tokio::net::UnixStream`).
  - **Windows Driver**: TCP, TLS, QUIC, and **Windows Named Pipes** (`tokio::net::windows::named_pipe`) via `pipe://\\.\pipe\mqtt_ipc`.
  - **Android Driver**: TCP, TLS, QUIC, and **Android Abstract Namespace Domain Sockets** via `unix://@android_mqtt_ipc`.
- **High-Performance Multi-Threaded Data Streams**:
  - `DataStreamProducer`: Thread-safe, lock-free concurrent ingestion from multiple CPU cores with atomic sequence ordering and microsecond timestamps.
  - `DataStreamConsumer`: Multi-threaded subscriber stream with out-of-order reassembly window (`BTreeMap`), duplicate suppression, and gap detection.
- **Session Data Recovery Engine**:
  - In-flight QoS 1 and QoS 2 message tracking and automatic retransmission with `DUP = true` upon reconnection.
  - Automatic active subscription restoration across connection drops.
  - Offline queue buffering (`DropOldest`, `ErrorOnFull`, `Block`) during network loss.
  - Circular sliding recovery journal (`replay_recovery_journal`) for zero data loss.
- **Topic-Filtered Stream Routing**:
  - Trie-based routing engine with wildcard support (`+`, `#`).
  - `client.subscribe_stream(topic, qos)` returning dedicated async `TopicSubscription` streams.
- **MQTT over QUIC & Real-Time Datagrams**:
  - Control stream multiplexing eliminating TCP Head-of-Line blocking.
  - Unreliable zero-handshake datagram telemetry (`client.publish_datagram()`).
- **Examples & Test Suite**:
  - `examples/tokio_basic_pubsub.rs` and `examples/tokio_reconnect_resilience.rs`.
  - Integration test suite in `tests/tokio_client_tests.rs` with mock broker.
  - All 28 tests passing across `no_std` and `tokio-client` features.

---

## [1.2.0] - 2026-08-18

### Added
- **Real-Time Chunk Streaming**:
  - `StreamMode::RealTimeStreaming` and `MqttOptions::with_stream_mode()`.
  - `begin_stream_publish()` and `MqttStreamWriter` for zero-RAM chunked publishing across transports.
  - Dedicated QUIC telemetry streams (`QuicMqttClient::open_telemetry_stream`).
  - Example: `examples/realtime_stream.rs`.
- **Universal Adapters**: `EmbeddedIoTransport` and `EmbeddedIoSplitTransport` for `embedded-io-async` (`esp-hal`, `esp-wifi`, `esp-idf-svc`, `embassy-net`).
- **ESP32 Support**: ESP32-S series (S2, S3 [Xtensa]) and ESP32-C series (C2, C3, C6, H2 [RISC-V]). Example: `examples/esp32_wifi_embassy.rs`.
- **Transport Accessors**: `transport()`, `transport_mut()`, and `into_transport()` on `MqttClient` and `QuicMqttClient`.
- **Microcontroller Logging**: `defmt` derivations across all packets, events, and errors.
- **Protocol Features**:
  - Last Will and Testament (LWT) support in `MqttOptions` and `Connect`.
  - `UNSUBSCRIBE` and `UNSUBACK` packet codecs and `MqttClient::unsubscribe()` API.
  - Packet decoders for `PUBREC`, `PUBREL`, `PUBCOMP`, and `PINGRESP`.
- **CI & Tests**: GitHub Actions matrix for `no_std`, `thumbv7em`, `riscv32`, `defmt`, clippy, formatting, and 22 integration/unit tests.

### Fixed
- Fixed bounds check panics on corrupted/truncated MQTT v5 User Properties (`0x26`).
- Fixed bounds check panics on encoding packets into zero-length or undersized buffers.
- Return explicit `ProtocolError::UnsupportedQoS` when publishing with `QoS::ExactlyOnce`.
- Fixed `get_next_packet_id()` initialization sequence.
- Cleaned up root directory and removed legacy `example/` path.
