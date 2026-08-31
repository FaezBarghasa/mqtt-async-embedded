# `mqtt-tokio`

High-throughput, asynchronous host MQTT client for Tokio, offering:
- **Zero-Copy Batch Publishing** with `bytes::Bytes`.
- **Topic Subscription Streams & Trie Filtering** (`subscribe_stream`).
- **Offline Queues and Auto-Reconnect Recovery**.
- **MQTT-over-QUIC and Smart TCP/TLS Fallback**.
- **`#![forbid(unsafe_code)]`**.

## License
Licensed under either of Apache License, Version 2.0 or MIT license at your option.
