use mqtt_async_embedded::{MqttClient, MqttOptions, QoS};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug)]
struct TransportError(tokio::io::Error);

impl mqtt_async_embedded::transport::TransportError for TransportError {}

struct TokioTransport {
    stream: TcpStream,
}

impl mqtt_async_embedded::transport::MqttTransport for TokioTransport {
    type Error = TransportError;

    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.stream.write_all(buf).await.map_err(TransportError)
    }

    async fn recv<'a>(&mut self, buf: &'a mut [u8]) -> Result<usize, Self::Error> {
        self.stream.read(buf).await.map_err(TransportError)
    }
}

#[tokio::main]
async fn main() {
    let stream = TcpStream::connect("127.0.0.1:1883").await.unwrap();
    let transport = TokioTransport { stream };

    let options = MqttOptions::new("desktop-client");
    let mut client: MqttClient<_, 1024, 1024> = MqttClient::new(transport, options);

    println!("Connecting to broker...");
    client.connect().await.unwrap();
    println!("Connected!");

    println!("Publishing message...");
    client
        .publish("test/topic", b"hello from desktop", QoS::AtMostOnce, &[])
        .await
        .unwrap();
    println!("Published!");

    println!("Disconnecting...");
    client.disconnect().await.unwrap();
    println!("Disconnected!");
}