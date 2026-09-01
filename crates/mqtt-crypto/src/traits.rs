//! # Cryptographic and TLS Traits
//!
//! Provides traits for hardware crypto offloading (e.g. STM32 CRYP, ESP32 SHA/AES)
//! and TLS stream abstractions.

use core::fmt::Debug;

/// Hardware crypto accelerator offload interface.
pub trait CryptoBackend {
    type Error: Debug + core::fmt::Display;

    /// Computes SHA-256 hash across the provided input slice.
    async fn sha256(&mut self, input: &[u8], output: &mut [u8; 32]) -> Result<(), Self::Error>;

    /// In-place AES-128/256-CBC/GCM encryption.
    async fn aes_encrypt(
        &mut self,
        key: &[u8],
        iv: &[u8],
        data: &mut [u8],
    ) -> Result<(), Self::Error>;

    /// In-place AES-128/256-CBC/GCM decryption.
    async fn aes_decrypt(
        &mut self,
        key: &[u8],
        iv: &[u8],
        data: &mut [u8],
    ) -> Result<(), Self::Error>;
}

/// Abstract TLS session provider.
pub trait TlsSession {
    /// Returns true if the TLS handshake has successfully completed.
    fn is_handshake_complete(&self) -> bool;

    /// Returns the negotiated ALPN protocol string if available.
    fn negotiated_alpn(&self) -> Option<&str>;
}
