//! # ESP32-C3 / RISC-V UART Modem MQTT Example
//!
//! Demonstrates how to adapt an asynchronous embedded UART / AT-modem driver
//! (via `embedded-io-async`) to `MqttTransport` on RISC-V bare-metal microcontrollers.

use embassy_time::Duration;
use mqtt_async_embedded::transport::EmbeddedIoTransport;
use mqtt_async_embedded::{MqttClient, MqttOptions, MqttVersion, QoS};

/// Simulated UART stream implementing `embedded-io-async` Read/Write.
struct MockEsp32UartStream {
    tx_count: usize,
}

impl embedded_io_async::ErrorType for MockEsp32UartStream {
    type Error = embedded_io_async::ErrorKind;
}

impl embedded_io_async::Read for MockEsp32UartStream {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if !buf.is_empty() {
            buf[0] = 0x20; // CONNACK
            buf[1] = 0x02;
            buf[2] = 0x00;
            buf[3] = 0x00;
            return Ok(4);
        }
        Ok(0)
    }
}

impl embedded_io_async::Write for MockEsp32UartStream {
    async fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        self.tx_count += buf.len();
        Ok(buf.len())
    }

    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    println!("=== ESP32-C3 / RISC-V UART Modem MQTT Example ===");
    println!("Target Architecture: RISC-V 32-bit (riscv32imc-unknown-none-elf)");
    println!("Transport: embedded-io-async UART serial bridge\n");

    let uart_stream = MockEsp32UartStream { tx_count: 0 };
    let transport = EmbeddedIoTransport::new(uart_stream);

    let options = MqttOptions::new("esp32c3-solar-sensor", "192.168.4.1", 1883)
        .with_version(MqttVersion::V5)
        .with_keep_alive(Duration::from_secs(60))
        .with_clean_session(true);

    let mut client: MqttClient<_, 4, 1024> = MqttClient::new(transport, options);
    client.connect().await.expect("Failed to connect");

    println!("Publishing solar telemetry over UART modem interface...");
    let payload = b"{\"solar_voltage_mv\": 3840, \"panel_temp_c\": 42.1}";
    client
        .publish("sensors/solar/status", payload, QoS::AtLeastOnce)
        .await
        .expect("Failed to publish");

    println!("Successfully transmitted sensor packet over UART stream.");
    println!("\nESP32-C3 embedded client configured successfully.");
}
