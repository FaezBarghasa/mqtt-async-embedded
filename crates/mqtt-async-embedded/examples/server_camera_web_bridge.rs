//! # Web Server Security Camera Stream Bridge (Axum / Actix-web Integration)
//!
//! Demonstrates running `mqtt-async-embedded` inside a backend Web Server:
//! 1. Connects to MQTT broker and subscribes to security camera video & telemetry topics.
//! 2. Creates multi-client `MqttBroadcastHub` instances for camera video and motion events.
//! 3. Bridges incoming camera JPEG frames into standard HTTP `multipart/x-mixed-replace` (MJPEG) streams.
//! 4. Bridges incoming telemetry and AI motion alerts into Server-Sent Events (SSE).
//! 5. Distributes live feeds to multiple web browser clients simultaneously with zero MQTT topic contention.

use bytes::Bytes;
use futures_util::StreamExt;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;

use mqtt_async_embedded::bridges::{CameraMjpegBridge, MqttBroadcastHub, TelemetrySseBridge};
use mqtt_async_embedded::packet::QoS;
use mqtt_async_embedded::tokio_client::{Client, ClientOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("============================================================");
    println!("   mqtt-async-embedded: Web Server Camera Stream Bridge     ");
    println!("============================================================");

    // 1. Configure server-side MQTT client
    let options = ClientOptions::new("web-server-backend", "127.0.0.1", 1883)
        .with_keep_alive(Duration::from_secs(30));

    let (mqtt_client, _driver_handle) = Client::connect(options);
    println!("[*] Backend web server connected to MQTT broker.");

    // 2. Create high-performance multi-client broadcast hubs for camera feeds
    let camera_video_hub = MqttBroadcastHub::new(
        &mqtt_client,
        "security/camera/front_door/mjpeg",
        QoS::AtMostOnce,
        64,
    )
    .await?;

    let camera_telemetry_hub = MqttBroadcastHub::new(
        &mqtt_client,
        "security/camera/front_door/events",
        QoS::AtMostOnce,
        64,
    )
    .await?;

    println!("[*] Camera video & event broadcast hubs initialized.");

    // 3. Simulate an edge security camera publishing live frames & motion events
    let camera_edge_client = mqtt_client.clone();
    tokio::spawn(async move {
        println!("[Edge Camera] Starting 10 FPS simulated security video stream...");
        // Minimal valid JPEG SOI/EOI frame
        let mock_jpeg_frame =
            b"\xFF\xD8\xFF\xE0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\x00\x01\x00\x00\xFF\xD9";
        let mut frame_count = 0u64;

        loop {
            frame_count += 1;
            // Publish video frame
            let _ = camera_edge_client
                .publish(
                    "security/camera/front_door/mjpeg",
                    QoS::AtMostOnce,
                    false,
                    Bytes::from_static(mock_jpeg_frame),
                )
                .await;

            // Publish periodic AI motion detection events
            if frame_count.is_multiple_of(30) {
                let event_json = format!(
                    r#"{{"event":"motion_detected","confidence":0.98,"frame":{frame_count}}}"#
                );
                let _ = camera_edge_client
                    .publish(
                        "security/camera/front_door/events",
                        QoS::AtLeastOnce,
                        false,
                        event_json,
                    )
                    .await;
            }

            tokio::time::sleep(Duration::from_millis(100)).await; // 10 FPS
        }
    });

    // 4. Start HTTP Streaming Server for Web Clients (Axum / Actix compatible stream engine)
    let http_listener = TcpListener::bind("127.0.0.1:0").await?;
    let http_addr: SocketAddr = http_listener.local_addr()?;
    println!("[HTTP Server] Listening on http://{http_addr}");
    println!("[HTTP Server] Endpoints available:");
    println!("   -> GET http://{http_addr}/api/camera/mjpeg   (Live multipart MJPEG video)");
    println!("   -> GET http://{http_addr}/api/camera/events  (Live Server-Sent Events)");

    // Spawn an HTTP server task accepting web browser connections
    let video_hub_clone = camera_video_hub.clone();
    let events_hub_clone = camera_telemetry_hub.clone();

    tokio::spawn(async move {
        while let Ok((mut socket, _client_addr)) = http_listener.accept().await {
            let v_hub = video_hub_clone.clone();
            let e_hub = events_hub_clone.clone();

            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let n = socket.try_read(&mut buf).unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);

                if req_str.contains("/api/camera/mjpeg") {
                    // Serve live multipart MJPEG video stream
                    let header = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nConnection: close\r\n\r\n",
                        CameraMjpegBridge::CONTENT_TYPE
                    );
                    let _ = socket.write_all(header.as_bytes()).await;

                    let mut mjpeg_stream = CameraMjpegBridge::new(&v_hub);
                    while let Some(Ok(chunk)) = mjpeg_stream.next().await {
                        if socket.write_all(&chunk).await.is_err() {
                            break; // Browser client disconnected
                        }
                    }
                } else if req_str.contains("/api/camera/events") {
                    // Serve live Server-Sent Events (SSE) stream
                    let header = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: {}\r\nCache-Control: no-cache\r\n\r\n",
                        TelemetrySseBridge::CONTENT_TYPE
                    );
                    let _ = socket.write_all(header.as_bytes()).await;

                    let mut sse_stream = TelemetrySseBridge::new(&e_hub);
                    while let Some(Ok(event_chunk)) = sse_stream.next().await {
                        if socket.write_all(&event_chunk).await.is_err() {
                            break; // Browser client disconnected
                        }
                    }
                } else {
                    // Serve Security Camera Web Dashboard HTML
                    let html = r#"<!DOCTYPE html>
<html>
<head><title>MQTT Security Camera Live Stream</title></head>
<body style="font-family:sans-serif; text-align:center; background:#111; color:#fff;">
  <h2>Live MJPEG Security Stream over MQTT</h2>
  <img src="/api/camera/mjpeg" style="border:2px solid #555; max-width:640px;" alt="Camera Stream"/>
  <h3>Real-Time Event Feed (SSE)</h3>
  <div id="events" style="font-family:monospace; color:#0f0;">Connecting...</div>
  <script>
    const evtSource = new EventSource("/api/camera/events");
    evtSource.onmessage = function(e) {
      document.getElementById("events").innerText = e.data;
    };
  </script>
</body>
</html>"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        html.len(),
                        html
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                }
            });
        }
    });

    tokio::time::sleep(Duration::from_millis(500)).await;
    println!("\n[*] Web Server Camera Stream Bridge running smoothly.");
    Ok(())
}
