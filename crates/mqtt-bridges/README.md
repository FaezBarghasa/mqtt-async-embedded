# `mqtt-bridges`

[![Crates.io](https://img.shields.io/crates/v/mqtt-bridges.svg)](https://crates.io/crates/mqtt-bridges)
[![Documentation](https://docs.rs/mqtt-bridges/badge.svg)](https://docs.rs/mqtt-bridges)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](../../LICENSE-MIT)
[![Safety: forbid(unsafe_code)](https://img.shields.io/badge/unsafe_code-forbidden-brightgreen.svg)](src/lib.rs)

Web server bridges (Axum, Actix-web, SSE, MJPEG) and Slint UI real-time bindings for `mqtt-tokio`.

---

## Features

- **`MqttBroadcastHub`**: 1-to-N fanout hub distributing a single MQTT feed to thousands of concurrent HTTP/SSE connections.
- **`CameraMjpegBridge`**: Multipart `multipart/x-mixed-replace` MJPEG stream formatter for browser HTML `<img>` rendering.
- **`TelemetrySseBridge`**: Server-Sent Events (SSE) stream bridge for web telemetry dashboards.
- **`SlintStreamBinding`**: Safe UI event loop property and video frame dispatching for Slint applications.

---

## Quick Example (Slint UI Binding)

```rust,no_run
use mqtt_bridges::SlintStreamBinding;
use mqtt_tokio::{Client, ClientOptions};
use mqtt_packet::QoS;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (client, _handle) = Client::connect(ClientOptions::new("slint-app", "127.0.0.1", 1883));

    let _telemetry_binding = SlintStreamBinding::bind_string_property(
        &client,
        "sensors/temperature",
        QoS::AtLeastOnce,
        |topic, value| {
            println!("UI Update -> {topic}: {value}");
        },
    ).await?;

    Ok(())
}
```

---

## License

Licensed under either of:
- **MIT License** ([LICENSE-MIT](../../LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
- **Apache License, Version 2.0** ([LICENSE-APACHE](../../LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

at your option.
