# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-08-18

### Added
- Universal `embedded-io-async` adapters (`EmbeddedIoTransport` and `EmbeddedIoSplitTransport`) providing seamless compatibility with `esp-hal`, `esp-wifi`, `esp-idf-svc`, and `embassy-net`.
- First-class support for **ESP32-S Series** (S2, S3) [Xtensa] and **ESP32-C Series** (C2, C3, C6, H2) [RISC-V].
- Transport accessors on `MqttClient` and `QuicMqttClient` (`transport()`, `transport_mut()`, and `into_transport()`).
- Zero-overhead `defmt` logging derivations (`#[cfg_attr(feature = "defmt", derive(defmt::Format))]`) across all packet schemas, client events, options, and error enums.
- Dedicated ESP32 Wi-Fi & Embassy example (`examples/esp32_wifi_embassy.rs`).
- Last Will and Testament (LWT) support in `MqttOptions` (`with_will`) and `Connect` packet encoding/decoding.
- `UNSUBSCRIBE` and `UNSUBACK` packet structures, wire codec, and `MqttClient::unsubscribe()` API.
- Support decoding `PUBREC`, `PUBREL`, `PUBCOMP`, and `PINGRESP` packets.
- License file (`LICENSE` - GNU General Public License v3.0 or later).
- Expanded integration and unit test suite (20 comprehensive tests total).
- Automated GitHub Actions CI workflow covering `no_std`, `thumbv7em-none-eabihf`, `riscv32imc-unknown-none-elf`, `riscv32imac-unknown-none-elf`, `defmt`, clippy, and formatting.

### Fixed
- Bounds check panics on parsing MQTT v5 User Properties (`0x26`) with corrupted/truncated length headers.
- Bounds check panics on encoding packets (`Connect`, `Publish`, `PubAck`, `Subscribe`, `Disconnect`) into zero-length or undersized buffers.
- Return explicit `ProtocolError::UnsupportedQoS` when attempting to publish with `QoS::ExactlyOnce` rather than producing invalid protocol sequences.
- Fixed `get_next_packet_id()` starting sequence.
- Cleaned up repository structure and removed deprecated `example/` directory.
