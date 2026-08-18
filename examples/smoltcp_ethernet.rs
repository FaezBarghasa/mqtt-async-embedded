//! # Smoltcp / Embassy-Net Ethernet MQTT Example
//!
//! Demonstrates how to configure and run `MqttClient` using `TcpTransport` over
//! `embassy-net` TCP sockets on embedded bare-metal microcontrollers.

#[cfg(feature = "transport-smoltcp")]
fn main() {
    use embassy_time::Duration;
    use mqtt_async_embedded::{MqttOptions, MqttVersion};

    println!("=== Smoltcp / Embassy-Net Ethernet MQTT Example ===");
    println!("Demonstrating zero-allocation TCP socket integration on embedded systems.");

    // Options configuration
    let options = MqttOptions::new("embedded-stm32-eth", "192.168.1.50", 1883)
        .with_version(MqttVersion::V5)
        .with_keep_alive(Duration::from_secs(30))
        .with_clean_session(true);

    println!("Options configured for client '{}'.", options.client_id);
    println!("In an Embassy async task:");
    println!("  1. Create `embassy_net::tcp::TcpSocket::new(stack, &mut rx_buf, &mut tx_buf)`");
    println!("  2. Connect the socket: `socket.connect(broker_endpoint).await?`");
    println!("  3. Wrap socket in `TcpTransport::new(socket, timeout)`");
    println!(
        "  4. Run `let mut client: MqttClient<_, 8, 2048> = MqttClient::new(transport, options);`"
    );
    println!("  5. Call `client.connect().await?` and start the async poll/publish loop.");
}

#[cfg(not(feature = "transport-smoltcp"))]
fn main() {
    println!("Please enable the `transport-smoltcp` feature to run this example:");
    println!("  cargo run --example smoltcp_ethernet --features transport-smoltcp");
}
