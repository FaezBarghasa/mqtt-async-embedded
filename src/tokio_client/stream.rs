//! # High-Performance Multi-Threaded Data Streams and Session Recovery
//!
//! Provides lock-free / low-contention multithreaded data stream publishers and consumers,
//! automatic sequence tracking, gap detection, out-of-order chunk reassembly, and
//! automated data recovery journals.

use std::collections::{BTreeMap, VecDeque};
use std::string::{String, ToString};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::sync::RwLock;

use crate::packet::QoS;
use crate::tokio_client::client::AsyncClient;
use crate::tokio_client::types::{ClientError, PublishMessage, TopicSubscription};

/// A sequenced data stream chunk with microsecond timestamps for high-frequency telemetry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamChunk {
    /// Monotonically increasing stream sequence identifier.
    pub seq_id: u64,
    /// Nanoseconds timestamp since UNIX epoch.
    pub timestamp_ns: u64,
    /// Stream topic filter / destination.
    pub topic: String,
    /// Zero-copy payload data.
    pub payload: Bytes,
    /// Whether this chunk was retransmitted as part of session data recovery.
    pub is_recovery: bool,
}

impl StreamChunk {
    /// Encodes the sequence ID, timestamp, and raw payload into an optimized binary wire format.
    pub fn encode_wire(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(16 + self.payload.len());
        buf.put_u64(self.seq_id);
        buf.put_u64(self.timestamp_ns);
        buf.put_slice(&self.payload);
        buf.freeze()
    }

    /// Decodes a wire-format payload into sequence ID, timestamp, and inner payload bytes.
    pub fn decode_wire(topic: &str, mut data: Bytes) -> Result<Self, ClientError> {
        if data.len() < 16 {
            return Ok(Self {
                seq_id: 0,
                timestamp_ns: 0,
                topic: topic.to_string(),
                payload: data,
                is_recovery: false,
            });
        }

        let seq_id = data.get_u64();
        let timestamp_ns = data.get_u64();
        Ok(Self {
            seq_id,
            timestamp_ns,
            topic: topic.to_string(),
            payload: data,
            is_recovery: false,
        })
    }
}

/// A thread-safe, cloneable multi-threaded data stream producer with automated recovery journal.
#[derive(Clone)]
pub struct DataStreamProducer {
    topic: Arc<str>,
    client: AsyncClient,
    sequence: Arc<AtomicU64>,
    qos: QoS,
    recovery_journal: Arc<RwLock<VecDeque<StreamChunk>>>,
    journal_capacity: usize,
}

impl DataStreamProducer {
    /// Creates a new high-performance multi-threaded data stream producer.
    pub fn new(client: AsyncClient, topic: impl Into<String>, qos: QoS, journal_capacity: usize) -> Self {
        Self {
            topic: Arc::from(topic.into()),
            client,
            sequence: Arc::new(AtomicU64::new(1)),
            qos,
            recovery_journal: Arc::new(RwLock::new(VecDeque::with_capacity(journal_capacity))),
            journal_capacity,
        }
    }

    /// Streams a data payload concurrently from any worker thread with monotonic sequence ordering.
    pub async fn send(&self, payload: impl Into<Bytes>) -> Result<u64, ClientError> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let chunk = StreamChunk {
            seq_id: seq,
            timestamp_ns: now,
            topic: self.topic.to_string(),
            payload: payload.into(),
            is_recovery: false,
        };

        // Record to recovery journal for loss recovery
        {
            let mut journal = self.recovery_journal.write().await;
            if journal.len() >= self.journal_capacity {
                journal.pop_front();
            }
            journal.push_back(chunk.clone());
        }

        let wire_payload = chunk.encode_wire();
        self.client
            .publish(&*self.topic, self.qos, false, wire_payload)
            .await?;

        Ok(seq)
    }

    /// Flushes and retransmits all journaled chunks as part of a session data recovery replay.
    pub async fn replay_recovery_journal(&self) -> Result<usize, ClientError> {
        let journal = self.recovery_journal.read().await;
        let mut replayed = 0;

        for chunk in journal.iter() {
            let mut wire_chunk = chunk.clone();
            wire_chunk.is_recovery = true;
            let wire_payload = wire_chunk.encode_wire();

            let mut msg = PublishMessage::new(&*self.topic, wire_payload);
            msg.dup = true;
            msg.qos = self.qos;

            self.client
                .publish(&*self.topic, self.qos, false, msg.payload)
                .await?;
            replayed += 1;
        }

        Ok(replayed)
    }

    /// Returns the current stream sequence number.
    pub fn current_sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }
}

/// A high-performance multi-threaded data stream consumer with gap detection and out-of-order reassembly.
pub struct DataStreamConsumer {
    subscription: TopicSubscription,
    expected_seq: u64,
    reorder_buffer: BTreeMap<u64, StreamChunk>,
    max_reorder_window: usize,
}

impl DataStreamConsumer {
    /// Creates a new sequenced data stream consumer.
    pub fn new(subscription: TopicSubscription, max_reorder_window: usize) -> Self {
        Self {
            subscription,
            expected_seq: 1,
            reorder_buffer: BTreeMap::new(),
            max_reorder_window,
        }
    }

    /// Receives the next in-order chunk, automatically reordering packets and detecting gaps.
    pub async fn recv_ordered(&mut self) -> Result<Option<StreamChunk>, ClientError> {
        // First check if the next expected sequence is already in the reorder buffer
        if let Some(chunk) = self.reorder_buffer.remove(&self.expected_seq) {
            self.expected_seq += 1;
            return Ok(Some(chunk));
        }

        while let Some(msg) = self.subscription.recv().await {
            let chunk = StreamChunk::decode_wire(&msg.topic, msg.payload)?;

            if chunk.seq_id == 0 {
                // Non-sequenced fallback chunk
                return Ok(Some(chunk));
            }

            if chunk.seq_id == self.expected_seq {
                self.expected_seq += 1;
                return Ok(Some(chunk));
            } else if chunk.seq_id > self.expected_seq {
                // Out of order future chunk -> buffer it
                self.reorder_buffer.insert(chunk.seq_id, chunk);

                // If reorder buffer exceeds window, force pop the smallest available
                if self.reorder_buffer.len() > self.max_reorder_window {
                    if let Some((&first_key, _)) = self.reorder_buffer.iter().next() {
                        let popped = self.reorder_buffer.remove(&first_key).unwrap();
                        self.expected_seq = popped.seq_id + 1;
                        return Ok(Some(popped));
                    }
                }
            }
            // If chunk.seq_id < self.expected_seq, it's an old duplicate -> drop it automatically
        }

        // Channel closed
        Ok(None)
    }

    /// The topic filter of the underlying stream subscription.
    pub fn topic_filter(&self) -> &str {
        self.subscription.topic_filter()
    }
}
