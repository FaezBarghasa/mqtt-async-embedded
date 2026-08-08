//! Example skeleton for using the MQTT client with smoltcp over Ethernet.
//!
//! This is a conceptual example. A real implementation would require:
//! 1. A HAL crate for your specific MCU.
//! 2. An async Ethernet driver (e.g., from `embassy-stm32`).
//! 3. A complete `embassy-net` setup.

#![no_std]
#![no_main]
#![feature(type_alias_impl_trait)]

use defmt_rtt as _;
use panic_probe as _;

use embassy_executor::Spawner;
use embassy_net::tcp::TcpSocket;
use embassy_net::{Config, Ipv4Address, Stack, StackResources};
use embassy_time::{Duration, Timer};
use mqtt_async_embedded::{MqttClient, MqttOptions, QoS};
use static_cell::StaticCell;

// --- FAKE ETHERNET DEVICE ---
// In a real project, this would be your `embassy-stm32::eth::Ethernet` or similar.
struct FakeEthernetDevice;

impl embassy_net::Device for FakeEthernetDevice {
    // Implement the required methods for the Device trait
    // For this example, they can be mostly empty.
    fn is_link_up(&self) -> bool { true }
    fn MTU(&self) -> usize { 1500 }
    // ... other methods
}


#[embassy_executor::task]
async fn net_task(stack: &'static Stack<FakeEthernetDevice>) -> ! {
    stack.run().await
}

#[embassy_executor::main]
async fn main(spawner: Spawner) {
    defmt::info!("Starting smoltcp Ethernet example...");

    // Dummy network configuration
    let config = Config::ipv4_static(embassy_net::StaticConfigV4 {
        address: embassy_net::Ipv4Cidr::new(Ipv4Address::new(192, 168, 1, 69), 24),
        gateway: Some(Ipv4Address::new(192, 168, 1, 1)),
        dns_servers: Default::default(),
    });

    // Dummy device and stack setup
    let device = FakeEthernetDevice;
    static STACK: StaticCell<Stack<FakeEthernetDevice>> = StaticCell::new();
    static RESOURCES: StaticCell<StackResources<3>> = StaticCell::new();
    let stack = STACK.init(Stack::new(
        device,
        config,
        RESOURCES.init(StackResources::<3>::new()),
        1234, // seed
    ));

    spawner.spawn(net_task(stack)).unwrap();

    let mut rx_buffer = [0; 4096];
    let mut tx_buffer = [0; 4096];
    let mut socket = TcpSocket::new(stack, &mut rx_buffer, &mut tx_buffer);

    socket.set_timeout(Some(Duration::from_secs(10)));

    // MQTT Broker address
    let broker_addr = Ipv4Address::new(192, 168, 1, 10); // Change to your broker's IP

    defmt::info!("Connecting TCP socket...");
    let r = socket.connect((broker_addr, 1883)).await;
    if let Err(e) = r {
        defmt::error!("TCP Connect error: {:?}", e);
        return;
    }
    defmt::info!("TCP Connected!");

    let options = MqttOptions::new("smoltcp_device_001");
    let mut client = MqttClient::new(socket, options);

    defmt::info!("Connecting to MQTT broker...");
    match client.connect().await {
        Ok(_) => defmt::info!("MQTT Connected!"),
        Err(e) => defmt::error!("MQTT Connection Error: {:?}", e),
    }

    defmt::info!("Publishing message...");
    let result = client
        .publish("room/light", b"ON", QoS::AtMostOnce)
        .await;

    if let Err(e) = result {
        defmt::error!("Failed to publish: {:?}", e);
    } else {
        defmt::info!("Published successfully!");
    }

    client.disconnect().await.unwrap();
}
