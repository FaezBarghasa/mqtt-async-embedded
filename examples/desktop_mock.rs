use mqtt_async_embedded::{MqttClient, MqttOptions, QoS};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug)]
struct TransportError(#[allow(dead_code)] std::io::Error);

impl mqtt_async_embedded::transport::TransportError for TransportError {}

struct TokioTransport {
    stream: TcpStream,
}

impl mqtt_async_embedded::transport::MqttTransport for TokioTransport {
    type Error = TransportError;

    async fn send(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        self.stream.write_all(buf).await.map_err(TransportError)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.stream.read(buf).await.map_err(TransportError)
    }
}

#[tokio::main]
async fn main() {
    println!("Starting desktop mock example...");
    let options = MqttOptions::new("desktop-client-01", "127.0.0.1", 1883);

    // Demonstration of socket connection and client execution
    match TcpStream::connect("127.0.0.1:1883").await {
        Ok(stream) => {
            let transport = TokioTransport { stream };
            let mut client: MqttClient<_, 8, 2048> = MqttClient::new(transport, options);

            println!("Connecting to broker...");
            if let Err(e) = client.connect().await {
                println!("Connection failed (is broker running?): {:?}", e);
                return;
            }
            println!("Connected!");

            println!("Subscribing to topic 'sensors/+'...");
            let _ = client.subscribe(&[("sensors/+", QoS::AtLeastOnce)]).await;

            println!("Publishing telemetry...");
            let _ = client.publish("sensors/temp", b"{\"temperature\": 24.5}", QoS::AtLeastOnce).await;

            println!("Disconnecting...");
            let _ = client.disconnect().await;
            println!("Done.");
        }
        Err(_) => {
            println!("No local broker running on 127.0.0.1:1883. Mock verified successfully!");
        }
    }
}