# Changelog

All notable changes to this project are documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
