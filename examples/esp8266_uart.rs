//! # ESP8266 / Cellular UART Modem MQTT Example
//!
//! Demonstrates how to adapt an asynchronous embedded UART driver (e.g. from `embedded-io-async`
//! or a hardware HAL) to the `MqttTransport` trait and run `MqttClient` on bare-metal.

use embassy_time::Duration;
use mqtt_async_embedded::transport::{MqttTransport, TransportError};
use mqtt_async_embedded::{MqttClient, MqttOptions};

/// A custom UART transport adapter wrapping raw embedded-io-async reader/writer.
pub struct UartMqttTransport<R, W> {
    reader: R,
    writer: W,
}

impl<R, W> UartMqttTransport<R, W> {
    pub fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }
}

#[derive(Debug)]
pub enum UartError {
    TxError,
    RxError,
}

impl TransportError for UartError {}

impl<R, W> MqttTransport for UartMqttTransport<R, W>
where
    R: embedded_io_async::Read,
    W: embedded_io_async::Write,
{
    type Error = UartError;

    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.writer
            .write_all(buf)
            .await
            .map_err(|_| UartError::TxError)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.reader.read(buf).await.map_err(|_| UartError::RxError)
    }
}

/// Simulated in-memory buffer ring matching embedded_io_async traits for testing.
struct MockUartIo;

impl embedded_io_async::ErrorType for MockUartIo {
    type Error = embedded_io_async::ErrorKind;
}

impl embedded_io_async::Read for MockUartIo {
    async fn read(&mut self, _buf: &mut [u8]) -> Result<usize, Self::Error> {
        // In real hardware, read bytes from the UART DMA/interrupt ringbuffer
        Ok(0)
    }
}

impl embedded_io_async::Write for MockUartIo {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        // In real hardware, write bytes to UART TX register / DMA
        Ok(buf.len())
    }
}

fn main() {
    println!("=== ESP8266 / UART Modem MQTT Transport Example ===");
    println!(
        "This example illustrates how to bind embedded-io-async UART drivers to MqttTransport."
    );

    let uart_rx = MockUartIo;
    let uart_tx = MockUartIo;
    let transport = UartMqttTransport::new(uart_rx, uart_tx);

    let options = MqttOptions::new("embedded-node-uart", "192.168.4.1", 1883)
        .with_keep_alive(Duration::from_secs(45))
        .with_clean_session(true);

    let _client: MqttClient<_, 8, 1024> = MqttClient::new(transport, options);
    println!("Client initialized with static 1KB buffers. Ready for Embassy executor loop!");
}
