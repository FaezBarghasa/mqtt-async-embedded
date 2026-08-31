//! # Client Configuration and Reconnect Options
//!
//! Provides a flexible, ergonomic builder for configuring broker endpoints across
//! **Linux**, **Windows**, and **Android** (TCP, TLS, QUIC, Unix/Android Abstract Sockets, Windows Named Pipes),
//! authentication, exponential backoff reconnect policies, offline queueing, and session data recovery.

use std::format;
use std::string::{String, ToString};
use std::time::Duration;

use crate::client::MqttVersion;
use crate::tokio_client::types::{ClientError, DataRecoveryPolicy, PublishMessage};

/// Strategy used when the client's offline queue reaches capacity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DropStrategy {
    /// Discards the oldest message in the offline queue to make room for new ones.
    #[default]
    DropOldest,
    /// Rejects new incoming publishes with [`ClientError::QueueFull`].
    ErrorOnFull,
    /// Blocks the publisher until queue capacity becomes available.
    Block,
}

/// Offline queue configuration for buffering messages during network dropouts.
#[derive(Debug, Clone)]
pub struct OfflineQueuePolicy {
    /// Maximum number of messages to buffer while disconnected. Set to 0 to disable offline queueing.
    pub capacity: usize,
    /// Strategy to employ when buffer capacity is reached.
    pub drop_strategy: DropStrategy,
}

impl Default for OfflineQueuePolicy {
    fn default() -> Self {
        Self {
            capacity: 512,
            drop_strategy: DropStrategy::DropOldest,
        }
    }
}

/// Configuration for automatic reconnection with exponential backoff and jitter.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Whether automatic reconnection is enabled.
    pub enabled: bool,
    /// Initial backoff delay for the first reconnection attempt.
    pub initial_delay: Duration,
    /// Maximum upper limit for backoff delay.
    pub max_delay: Duration,
    /// Exponential growth multiplier (e.g. 1.5x or 2.0x).
    pub multiplier: f64,
    /// Maximum number of reconnection attempts before giving up (None = retry indefinitely).
    pub max_retries: Option<usize>,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            max_retries: None,
        }
    }
}

impl ReconnectPolicy {
    /// Computes the delay for the given attempt number (attempt 1 = initial attempt with 0 delay).
    pub fn compute_delay(&self, attempt: usize) -> Duration {
        if attempt <= 1 {
            return Duration::ZERO;
        }
        let exp = (attempt - 2).min(10) as i32;
        let factor = self.multiplier.powi(exp);
        let computed = self.initial_delay.as_secs_f64() * factor;
        let clamped = computed.min(self.max_delay.as_secs_f64());
        Duration::from_secs_f64(clamped)
    }
}

/// Cross-platform transport connection targets for Linux, Windows, and Android.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportTarget {
    /// Standard TCP socket (Linux, Windows, Android).
    Tcp { host: String, port: u16 },
    /// TLS encrypted TCP socket via Rustls (Linux, Windows, Android).
    #[cfg(feature = "tokio-tls")]
    Tls { host: String, port: u16, server_name: String },
    /// MQTT over QUIC / H3 transport with multiplexed streams and datagrams (Linux, Windows, Android).
    #[cfg(feature = "transport-quic")]
    Quic { host: String, port: u16, server_name: String },
    /// POSIX Unix Domain Sockets or Android Abstract Namespace Sockets (Linux, Android).
    Unix { path: String },
    /// Windows Named Pipes for high-speed local IPC (Windows).
    NamedPipe { path: String },
}

/// Client configuration options.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub target: TransportTarget,
    pub client_id: String,
    pub keep_alive: Duration,
    pub clean_session: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub version: MqttVersion,
    pub will: Option<PublishMessage>,
    pub reconnect: ReconnectPolicy,
    pub offline_queue: OfflineQueuePolicy,
    pub recovery: DataRecoveryPolicy,
    pub channel_capacity: usize,
    pub max_packet_size: usize,
}

impl ClientOptions {
    /// Creates a new TCP client configuration with sensible defaults.
    pub fn new(client_id: impl Into<String>, host: impl Into<String>, port: u16) -> Self {
        Self {
            target: TransportTarget::Tcp {
                host: host.into(),
                port,
            },
            client_id: client_id.into(),
            keep_alive: Duration::from_secs(30),
            clean_session: true,
            username: None,
            password: None,
            version: MqttVersion::V3,
            will: None,
            reconnect: ReconnectPolicy::default(),
            offline_queue: OfflineQueuePolicy::default(),
            recovery: DataRecoveryPolicy::default(),
            channel_capacity: 1024,
            max_packet_size: 65536,
        }
    }

    /// Parses a URI string such as:
    /// - `mqtt://127.0.0.1:1883` (Linux, Windows, Android)
    /// - `mqtts://broker.hivemq.com:8883` (Linux, Windows, Android)
    /// - `quic://broker.emqx.io:14567` (Linux, Windows, Android)
    /// - `unix:///tmp/mqtt.sock` (Linux)
    /// - `unix://@android_mqtt_ipc` (Android Abstract Namespace)
    /// - `pipe://\\.\pipe\mqtt_ipc` (Windows Named Pipe)
    pub fn from_uri(client_id: impl Into<String>, uri: &str) -> Result<Self, ClientError> {
        let client_id = client_id.into();
        if let Some(rest) = uri.strip_prefix("mqtt://") {
            let (host, port) = parse_host_port(rest, 1883)?;
            Ok(Self::new(client_id, host, port))
        } else if let Some(rest) = uri.strip_prefix("mqtts://") {
            #[cfg(feature = "tokio-tls")]
            {
                let (host, port) = parse_host_port(rest, 8883)?;
                let mut opts = Self::new(client_id, host.clone(), port);
                opts.target = TransportTarget::Tls {
                    host: host.clone(),
                    port,
                    server_name: host,
                };
                Ok(opts)
            }
            #[cfg(not(feature = "tokio-tls"))]
            {
                let _ = rest;
                Err(ClientError::Tls("Feature 'tokio-tls' is required for mqtts:// URIs".into()))
            }
        } else if let Some(rest) = uri.strip_prefix("quic://") {
            #[cfg(feature = "transport-quic")]
            {
                let (host, port) = parse_host_port(rest, 14567)?;
                let mut opts = Self::new(client_id, host.clone(), port);
                opts.target = TransportTarget::Quic {
                    host: host.clone(),
                    port,
                    server_name: host,
                };
                Ok(opts)
            }
            #[cfg(not(feature = "transport-quic"))]
            {
                let _ = rest;
                Err(ClientError::Quic("Feature 'transport-quic' is required for quic:// URIs".into()))
            }
        } else if let Some(path) = uri.strip_prefix("unix://") {
            let mut opts = Self::new(client_id, "localhost", 0);
            opts.target = TransportTarget::Unix {
                path: path.to_string(),
            };
            Ok(opts)
        } else if let Some(path) = uri.strip_prefix("pipe://") {
            let mut opts = Self::new(client_id, "localhost", 0);
            opts.target = TransportTarget::NamedPipe {
                path: path.to_string(),
            };
            Ok(opts)
        } else {
            Err(ClientError::InvalidTopic(format!(
                "Unsupported scheme in URI '{uri}'. Expected 'mqtt://', 'mqtts://', 'quic://', 'unix://', or 'pipe://'"
            )))
        }
    }

    /// Sets credentials for authentication.
    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    /// Sets the MQTT keep alive duration.
    pub fn with_keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = keep_alive;
        self
    }

    /// Sets clean session / clean start flag.
    pub fn with_clean_session(mut self, clean: bool) -> Self {
        self.clean_session = clean;
        self
    }

    /// Sets the MQTT protocol version (V3.1.1 or V5).
    pub fn with_version(mut self, version: MqttVersion) -> Self {
        self.version = version;
        self
    }

    /// Sets the Last Will and Testament message.
    pub fn with_will(mut self, will: PublishMessage) -> Self {
        self.will = Some(will);
        self
    }

    /// Sets the reconnection policy.
    pub fn with_reconnect(mut self, reconnect: ReconnectPolicy) -> Self {
        self.reconnect = reconnect;
        self
    }

    /// Sets the offline queueing policy.
    pub fn with_offline_queue(mut self, offline_queue: OfflineQueuePolicy) -> Self {
        self.offline_queue = offline_queue;
        self
    }

    /// Sets the session data recovery policy.
    pub fn with_recovery(mut self, recovery: DataRecoveryPolicy) -> Self {
        self.recovery = recovery;
        self
    }

    /// Sets the internal request channel capacity.
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Sets the maximum allowed MQTT packet size in bytes.
    pub fn with_max_packet_size(mut self, max_size: usize) -> Self {
        self.max_packet_size = max_size;
        self
    }
}

fn parse_host_port(s: &str, default_port: u16) -> Result<(String, u16), ClientError> {
    if let Some((host, port_str)) = s.split_once(':') {
        let port = port_str
            .parse::<u16>()
            .map_err(|_| ClientError::InvalidTopic(format!("Invalid port '{port_str}' in URI")))?;
        Ok((host.to_string(), port))
    } else {
        Ok((s.to_string(), default_port))
    }
}
