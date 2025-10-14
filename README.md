# Async Embedded MQTT Client

An `async`, `no_std`-compatible MQTT client library in Rust, designed for embedded systems using the [Embassy](https://embassy.dev/) async ecosystem and `embedded-hal` 1.0.0 traits.

## Core Features

- **Asynchronous:** Built on `async/await` and designed for the Embassy ecosystem.
- **`no_std` by default:** Suitable for bare-metal and resource-constrained devices.
- **Hardware Agnostic:** Uses `embedded-hal-async` traits to support various communication transports (TCP, UART, SPI, etc.).
- **Memory Efficient:** Leverages `heapless` to avoid dynamic memory allocation.
- **MQTT v3.1.1 and v5 Support:** Protocol version can be selected via feature flags.
- **QoS 0 & 1:** Support for "at most once" and "at least once" message delivery.

## Getting Started

To use this library, you need a transport that implements the `MqttTransport` trait. This trait is an abstraction over the underlying communication channel (e.g., a TCP socket).

Here is a conceptual example of how to use the client with an `embassy-net` TCP socket:

```rust,no_run
use mqtt_async_embedded::{MqttClient, MqttOptions, QoS};
use embassy_net::tcp::TcpSocket;
use embassy_time::Duration;

// Assume `socket` is an already connected `TcpSocket`
async fn run_mqtt(mut socket: TcpSocket<'_>) {
    let options = MqttOptions::new("my-embedded-device")
        .set_keep_alive(Duration::from_secs(30));

    let mut client: MqttClient<_, 1024, 1024> = MqttClient::new(socket, options);

    // Connect to the broker
    if let Err(e) = client.connect().await {
        // Handle connection error
        return;
    }

    // Publish a message
    client.publish("sensors/temp", b"25.3", QoS::AtLeastOnce, &[]).await.unwrap();

    // Subscribe to a topic
    client.subscribe("commands", QoS::AtLeastOnce).await.unwrap();

    // Disconnect
    client.disconnect().await.unwrap();
}
```

## Project Structure

The library is organized into a few key modules:

- `src/client.rs`: Contains the main `MqttClient` and its async state machine.
- `src/packet.rs`: Handles the encoding and decoding of MQTT control packets.
- `src/transport.rs`: Defines the `MqttTransport` trait for hardware abstraction.
- `src/error.rs`: Provides unified error types for the client.

## Feature Flags

The following feature flags are available:

- `v5`: Enables MQTT v5 support.
- `std`: Enables `std` library support for running examples and tests on a host machine.
- `defmt`: Enables logging via the `defmt` framework.
- `transport-smoltcp`: Enables integration with `embassy-net` for a full TCP/IP stack.
- `nom`: Enables the `nom` parser for decoding MQTT packets.

## Running Examples

The crate includes several examples in the `examples/` directory.

### Desktop Mock

This example uses `std::net::TcpStream` to connect to a local MQTT broker on your host machine.

**Prerequisites:** An MQTT broker (like Mosquitto) running on `localhost:1883`.

**Run command:**
```bash
cargo run --example desktop_mock --features std
```

### Embedded Examples

The `esp8266_uart.rs` and `smoltcp_ethernet.rs` examples are skeletons that demonstrate how the library would be integrated into a real embedded project. They require a specific HAL and driver setup to be fully functional.