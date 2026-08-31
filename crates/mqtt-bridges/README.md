# `mqtt-bridges`

Web server bridges (Axum, Actix, SSE, MJPEG) and Slint UI real-time streaming integrations for `mqtt-tokio`.

## Features
- **`MqttBroadcastHub`**: 1-to-N fanout for web servers.
- **`CameraMjpegBridge`**: Multipart MJPEG streaming for HTML `<img>` elements.
- **`TelemetrySseBridge`**: Server-Sent Events (SSE) formatter.
- **`SlintStreamBinding`**: Safe UI event loop property and video frame updates.

## License
Licensed under either of Apache License, Version 2.0 or MIT license at your option.
