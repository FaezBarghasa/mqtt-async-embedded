//! # Smart Transport with QUIC to TCP/TLS Fallback
//!
//! Provides automatic network fallback: attempts low-latency QUIC (UDP) first;
//! if blocked by firewall or network drop, seamlessly falls back to standard TCP/TLS.

use std::time::Duration;
use tracing::{info, warn};

use crate::options::{ClientOptions, TransportTarget};
use crate::transport::{BoxedTransport, connect_transport};
use crate::types::ClientError;

/// Smart transport connector that manages protocol negotiation and automatic fallback.
pub struct SmartTransport;

impl SmartTransport {
    /// Attempts to connect using the configured primary transport, falling back to secondary if configured.
    pub async fn connect(options: &ClientOptions) -> Result<BoxedTransport, ClientError> {
        let primary_result =
            tokio::time::timeout(Duration::from_secs(5), connect_transport(&options.target)).await;

        match primary_result {
            Ok(Ok(transport)) => {
                info!("Successfully established primary MQTT transport connection.");
                Ok(transport)
            }
            Ok(Err(err)) => {
                warn!("Primary transport connection failed: {err}. Checking for fallback...");
                Self::attempt_fallback(options, err).await
            }
            Err(_) => {
                warn!("Primary transport connection timed out. Attempting fallback...");
                Self::attempt_fallback(options, ClientError::ConnectionRefused(3)).await
            }
        }
    }

    async fn attempt_fallback(
        options: &ClientOptions,
        original_error: ClientError,
    ) -> Result<BoxedTransport, ClientError> {
        #[cfg(feature = "transport-quic")]
        if let TransportTarget::Quic { ref host, port, .. } = options.target {
            info!("Attempting automatic fallback from QUIC to standard TCP...");
            let fallback_target = TransportTarget::Tcp {
                host: host.clone(),
                port,
            };
            return connect_transport(&fallback_target).await;
        }

        let _ = options;
        Err(original_error)
    }
}
