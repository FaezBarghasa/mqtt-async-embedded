//! # Real-Time Streaming MQTT Example
//!
//! Demonstrates:
//! 1. `StreamMode::RealTimeStreaming` configuration for sub-millisecond fast-path dispatch.
//! 2. Zero-allocation chunked stream publishing (`begin_stream_publish`) for streaming large
//!    payloads (e.g. audio waveforms, camera stills, high-rate IMU) chunk-by-chunk without
//!    requiring large RAM buffers on embedded microcontrollers.

use embassy_time::Duration;
use mqtt_async_embedded::client::{MqttClient, MqttOptions, MqttVersion, StreamMode};
use mqtt_async_embedded::packet::QoS;
use mqtt_async_embedded::transport::EmbeddedIoTransport;
use std::collections::VecDeque;

/// Mock in-memory stream representing an async Wi-Fi / Ethernet or UART socket.
struct MockStream {
    rx_buf: VecDeque<u8>,
    tx_buf: VecDeque<u8>,
}

impl MockStream {
    fn new() -> Self {
        let mut s = Self {
            rx_buf: VecDeque::new(),
            tx_buf: VecDeque::new(),
        };
        // Simulated broker CONNACK
        s.rx_buf.extend([0x20, 0x02, 0x00, 0x00]);
        s
    }
}

impl embedded_io_async::ErrorType for MockStream {
    type Error = embedded_io_async::ErrorKind;
}

impl embedded_io_async::Read for MockStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        let to_read = std::cmp::min(buf.len(), self.rx_buf.len());
        for b in buf.iter_mut().take(to_read) {
            *b = self.rx_buf.pop_front().unwrap();
        }
        Ok(to_read)
    }
}

impl embedded_io_async::Write for MockStream {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        self.tx_buf.extend(buf.iter().copied());
        Ok(buf.len())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    println!("============================================================");
    println!("     Real-Time Streaming MQTT Telemetry Demonstration       ");
    println!("============================================================");

    // 1. Configure options with StreamMode::RealTimeStreaming
    let options = MqttOptions::new("edge-streaming-node", "broker.hivemq.com", 1883)
        .with_version(MqttVersion::V5)
        .with_keep_alive(Duration::from_secs(30))
        .with_stream_mode(StreamMode::RealTimeStreaming);

    println!("1. Configured MqttOptions with StreamMode::RealTimeStreaming.");

    let stream = MockStream::new();
    let transport = EmbeddedIoTransport::new(stream);

    // Static buffer of only 512 bytes on microcontroller
    let mut client: MqttClient<_, 8, 512> = MqttClient::new(transport, options);

    // 2. Connect
    client.connect().await.expect("Connect failed");
    println!("2. Connected to broker in real-time streaming mode.");

    // 3. Stream a 4 KB audio/sensor recording in 256-byte chunks
    // Even though the client buffer is only 512 bytes, we can stream 4096 bytes with 0 extra RAM!
    let total_audio_len = 4096;
    println!(
        "3. Streaming {} bytes of raw PCM sensor audio chunk-by-chunk...",
        total_audio_len
    );

    let mut stream_writer = client
        .begin_stream_publish("edge/microphone/stream", total_audio_len, QoS::AtMostOnce)
        .await
        .expect("Failed to begin stream publish");

    let chunk_sample = [0x5A; 256];
    let chunks_count = total_audio_len / chunk_sample.len();

    for i in 1..=chunks_count {
        stream_writer
            .write_chunk(&chunk_sample)
            .await
            .expect("Failed to write chunk");
        println!(
            "   -> Streamed chunk {}/{} ({} bytes remaining)",
            i,
            chunks_count,
            stream_writer.remaining_bytes()
        );
    }

    stream_writer
        .finish()
        .expect("Failed to finalize stream publish");

    println!("4. Real-time stream completed successfully with zero heap allocations!");
}
