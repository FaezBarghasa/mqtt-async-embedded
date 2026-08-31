//! # Zero-RAM Chunk and Direct DMA Stream Writer
//!
//! Allows streaming payloads directly over the wire chunk-by-chunk or from DMA buffers.

use crate::error::{MqttError, ProtocolError};
use crate::transport::{MqttTransport, TransportError};

/// Active stream writer for publishing arbitrary length payloads with zero buffer allocations.
pub struct MqttStreamWriter<'a, T: MqttTransport> {
    transport: &'a mut T,
    total_len: usize,
    bytes_remaining: usize,
}

impl<'a, T: MqttTransport> MqttStreamWriter<'a, T> {
    pub(crate) fn new(transport: &'a mut T, total_payload_len: usize) -> Self {
        Self {
            transport,
            total_len: total_payload_len,
            bytes_remaining: total_payload_len,
        }
    }

    /// Writes a chunk of payload directly to the network transport.
    pub async fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), MqttError<T::Error>>
    where
        T::Error: TransportError,
    {
        if chunk.len() > self.bytes_remaining {
            return Err(MqttError::BufferTooSmall);
        }

        self.transport.send(chunk).await?;
        self.bytes_remaining -= chunk.len();
        Ok(())
    }

    /// Streams directly from a contiguous DMA slice buffer without copies.
    pub async fn write_dma_slice(&mut self, dma_buffer: &[u8]) -> Result<(), MqttError<T::Error>>
    where
        T::Error: TransportError,
    {
        self.write_chunk(dma_buffer).await
    }

    /// Writes vectored DMA slices in sequence.
    pub async fn write_dma_vectored(&mut self, slices: &[&[u8]]) -> Result<(), MqttError<T::Error>>
    where
        T::Error: TransportError,
    {
        let total: usize = slices.iter().map(|s| s.len()).sum();
        if total > self.bytes_remaining {
            return Err(MqttError::BufferTooSmall);
        }

        self.transport.send_vectored(slices).await?;
        self.bytes_remaining -= total;
        Ok(())
    }

    /// Total declared bytes for this stream publish.
    pub fn total_bytes(&self) -> usize {
        self.total_len
    }

    /// Returns the remaining number of unwritten payload bytes for this message.
    pub fn remaining_bytes(&self) -> usize {
        self.bytes_remaining
    }

    /// Returns true when all announced payload bytes have been successfully transmitted.
    pub fn is_complete(&self) -> bool {
        self.bytes_remaining == 0
    }

    /// Finishes the stream, ensuring all declared bytes were sent.
    pub fn finish(self) -> Result<(), MqttError<T::Error>>
    where
        T::Error: TransportError,
    {
        if self.bytes_remaining == 0 {
            Ok(())
        } else {
            Err(MqttError::Protocol(ProtocolError::IncompletePacket))
        }
    }
}
