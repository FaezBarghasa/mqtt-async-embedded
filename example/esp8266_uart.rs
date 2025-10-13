//! Example skeleton for using the MQTT client with an ESP8266 WiFi module via UART.
//!
//! This is a conceptual example. A real implementation would require:
//! 1. A HAL crate for your specific MCU (e.g., `esp32c3-hal`).
//! 2. An async UART driver compatible with `embedded-hal-async`.
//! 3. An implementation of an "AT command" transport that uses the UART driver
//!    to manage a TCP connection on the ESP8266.

#![no_std]
#![no_main]
// Example requires a panic handler
use defmt_rtt as _;
use panic_probe as _;

use embassy_executor::Spawner;
use embassy_time::Timer;
use mqtt_async_embedded::{MqttClient, MqttOptions};

// --- FAKE HARDWARE ABSTRACTION ---
// In a real project, these would come from your HAL and async drivers.

struct FakeAsyncUart;
impl embedded_hal_async::serial::Write for FakeAsyncUart {
    async fn write(&mut self, _buf: &[u8]) -> Result<(), Self::Error> {
        // Pretend to write to UART
        Ok(())
    }
    async fn flush(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
impl embedded_hal_async::serial::Read for FakeAsyncUart {
    async fn read(&mut self, buf: &mut [u8]) -> Result<(), Self::Error> {
        // Pretend to read from UART, maybe returning a canned "OK"
        buf[0] = b'O';
        buf[1] = b'K';
        Ok(())
    }
}
#[derive(Debug)]
struct FakeUartError;
impl embedded_hal::serial::Error for FakeUartError {
    fn kind(&self) -> embedded_hal::serial::ErrorKind {
        embedded_hal::serial::ErrorKind::Other
    }
}
impl embedded_hal::digital::Error for FakeUartError {
    fn kind(&self) -> embedded_hal::digital::ErrorKind {
        embedded_hal::digital::ErrorKind::Other
    }
}
impl embedded_hal::serial::ErrorType for FakeAsyncUart {
    type Error = FakeUartError;
}

// --- FAKE AT-COMMAND TRANSPORT ---
// This would parse AT commands and manage the ESP8266's state.

use mqtt_async_embedded::transport::{MqttTransport, TransportError};

struct AtCommandTransport<UART> {
    uart: UART,
}

#[derive(Debug)]
struct AtError;
impl TransportError for AtError {}

impl<UART: embedded_hal_async::serial::Read + embedded_hal_async::serial::Write> MqttTransport
for AtCommandTransport<UART>
{
    type Error = AtError;
    async fn send(&mut self, _buf: &[u8]) -> Result<(), Self::Error> {
        // 1. Send AT+CIPSEND=<len>
        // 2. Wait for ">"
        // 3. Send the buffer
        // 4. Wait for "SEND OK"
        defmt::info!("AT Transport: Sending data...");
        Timer::after_millis(100).await;
        Ok(())
    }
    async fn recv<'a>(&mut self, _buf: &'a mut [u8]) -> Result<usize, Self::Error> {
        // Listen for "+IPD,<len>:<data>" from the ESP8266
        defmt::info!("AT Transport: Receiving data...");
        Timer::after_millis(500).await;
        // Pretend we received a CONNACK
        _buf[0] = 0x20;
        _buf[1] = 0x02;
        _buf[2] = 0x00;
        _buf[3] = 0x00;
        Ok(4)
    }
}

#[embassy_executor::main]
async fn main(_spawner: Spawner) {
    defmt::info!("Starting ESP8266 UART example...");

    let fake_uart = FakeAsyncUart;
    let transport = AtCommandTransport { uart: fake_uart };

    let options = MqttOptions::new("esp8266-device");
    let mut client = MqttClient::new(transport, options);

    defmt::info!("Connecting via AT transport...");
    match client.connect().await {
        Ok(_) => defmt::info!("MQTT Connected!"),
        Err(e) => defmt::error!("MQTT Connection Error: {:?}", e),
    }
}
