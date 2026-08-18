# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-08-18

### Added
- Last Will and Testament (LWT) support in `MqttOptions` (`with_will`) and `Connect` packet encoding/decoding.
- `UNSUBSCRIBE` and `UNSUBACK` packet structures, wire codec, and `MqttClient::unsubscribe()` API.
- Support decoding `PUBREC`, `PUBREL`, and `PUBCOMP` packets.
- License file (`LICENSE` - GNU General Public License v3.0 or later).
- Automated GitHub Actions CI workflow covering `no_std`, `thumbv7em-none-eabihf`, testing, and clippy.

### Fixed
- Bounds check panics on parsing MQTT v5 User Properties (`0x26`) with corrupted/truncated length headers.
- Bounds check panics on encoding packets (`Connect`, `Publish`, `PubAck`, `Subscribe`, `Disconnect`) into zero-length or undersized buffers.
- Return explicit `ProtocolError::UnsupportedQoS` when attempting to publish with `QoS::ExactlyOnce` rather than producing invalid protocol sequences.
- Cleaned up repository structure and removed deprecated `example/` directory.
