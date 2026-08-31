//! # High-Performance Universal Multi-Threaded Sensor & Media Data Streams
//!
//! Provides lock-free / low-contention multithreaded streaming engines supporting
//! **all sensor and media types**:
//! - **High-Frequency Time-Series Metrics**: Accelerometer, Gyroscope, IMU, Current/Voltage, Vibration (f32/f64).
//! - **Binary Sensor Buffers**: CAN bus frames, Modbus RTU, SPI DMA packets, raw binary structs.
//! - **Audio Streams**: Multi-channel PCM audio chunks, Opus frames.
//! - **Camera & Vision Feeds**: JPEG, PNG, H.264 video NALUs, thermal vision arrays.
//! - **Structured Metadata**: JSON, CBOR, Protobuf, string telemetry.
//! - **Automated Session Data Recovery**: Sliding recovery journals, sequence tracking, and out-of-order reassembly.

use std::collections::{BTreeMap, VecDeque};
use std::string::{String, ToString};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::sync::RwLock;

use crate::packet::QoS;
use crate::tokio_client::client::AsyncClient;
use crate::tokio_client::types::{ClientError, PublishMessage, TopicSubscription};

/// Universal sensor and media payload types supported by the stream engine.
#[derive(Debug, Clone, PartialEq)]
pub enum SensorDataType {
    /// Arbitrary raw binary data (e.g. CAN bus, Modbus, SPI DMA packets, binary telemetry structs).
    Raw(Bytes),
    /// High-frequency multi-axis floating point time-series (e.g. IMU $[x, y, z]$, vibration, power).
    TimeSeries(std::vec::Vec<f64>),
    /// 32-bit float time-series array for high-rate DSP telemetry.
    TimeSeriesF32(std::vec::Vec<f32>),
    /// Structured JSON text or state events.
    Json(String),
    /// Audio stream chunk (e.g. PCM 16-bit, AAC, Opus).
    AudioPcm {
        sample_rate: u32,
        channels: u8,
        data: Bytes,
    },
    /// Image or vision frame (e.g. JPEG, PNG, thermal heatmap, H.264 NAL).
    ImageFrame { mime: String, data: Bytes },
}

impl SensorDataType {
    /// Type discriminator tag.
    const TAG_RAW: u8 = 0x01;
    const TAG_TIMESERIES_F64: u8 = 0x02;
    const TAG_TIMESERIES_F32: u8 = 0x03;
    const TAG_JSON: u8 = 0x04;
    const TAG_AUDIO_PCM: u8 = 0x05;
    const TAG_IMAGE_FRAME: u8 = 0x06;

    /// Serializes the typed sensor data into an optimized zero-copy binary wire payload.
    pub fn encode(&self) -> Bytes {
        match self {
            Self::Raw(bytes) => {
                let mut buf = BytesMut::with_capacity(1 + bytes.len());
                buf.put_u8(Self::TAG_RAW);
                buf.put_slice(bytes);
                buf.freeze()
            }
            Self::TimeSeries(values) => {
                let mut buf = BytesMut::with_capacity(1 + 2 + values.len() * 8);
                buf.put_u8(Self::TAG_TIMESERIES_F64);
                buf.put_u16(values.len() as u16);
                for &v in values {
                    buf.put_f64(v);
                }
                buf.freeze()
            }
            Self::TimeSeriesF32(values) => {
                let mut buf = BytesMut::with_capacity(1 + 2 + values.len() * 4);
                buf.put_u8(Self::TAG_TIMESERIES_F32);
                buf.put_u16(values.len() as u16);
                for &v in values {
                    buf.put_f32(v);
                }
                buf.freeze()
            }
            Self::Json(json) => {
                let bytes = json.as_bytes();
                let mut buf = BytesMut::with_capacity(1 + bytes.len());
                buf.put_u8(Self::TAG_JSON);
                buf.put_slice(bytes);
                buf.freeze()
            }
            Self::AudioPcm {
                sample_rate,
                channels,
                data,
            } => {
                let mut buf = BytesMut::with_capacity(1 + 4 + 1 + data.len());
                buf.put_u8(Self::TAG_AUDIO_PCM);
                buf.put_u32(*sample_rate);
                buf.put_u8(*channels);
                buf.put_slice(data);
                buf.freeze()
            }
            Self::ImageFrame { mime, data } => {
                let mime_bytes = mime.as_bytes();
                let mut buf = BytesMut::with_capacity(1 + 1 + mime_bytes.len() + data.len());
                buf.put_u8(Self::TAG_IMAGE_FRAME);
                buf.put_u8(mime_bytes.len() as u8);
                buf.put_slice(mime_bytes);
                buf.put_slice(data);
                buf.freeze()
            }
        }
    }

    /// Deserializes a binary payload into a typed sensor structure.
    pub fn decode(payload: Bytes) -> Self {
        if payload.is_empty() {
            return Self::Raw(payload);
        }

        let tag = payload[0];
        let mut slice = payload.slice(1..);
        match tag {
            Self::TAG_RAW => Self::Raw(slice),
            Self::TAG_TIMESERIES_F64 => {
                if slice.len() >= 2 {
                    let count = slice.get_u16() as usize;
                    if slice.len() >= count * 8 {
                        let mut vals = std::vec::Vec::with_capacity(count);
                        for _ in 0..count {
                            vals.push(slice.get_f64());
                        }
                        return Self::TimeSeries(vals);
                    }
                }
                Self::Raw(payload)
            }
            Self::TAG_TIMESERIES_F32 => {
                if slice.len() >= 2 {
                    let count = slice.get_u16() as usize;
                    if slice.len() >= count * 4 {
                        let mut vals = std::vec::Vec::with_capacity(count);
                        for _ in 0..count {
                            vals.push(slice.get_f32());
                        }
                        return Self::TimeSeriesF32(vals);
                    }
                }
                Self::Raw(payload)
            }
            Self::TAG_JSON => {
                let s = String::from_utf8_lossy(&slice).to_string();
                Self::Json(s)
            }
            Self::TAG_AUDIO_PCM => {
                if slice.len() >= 5 {
                    let sample_rate = slice.get_u32();
                    let channels = slice.get_u8();
                    return Self::AudioPcm {
                        sample_rate,
                        channels,
                        data: slice,
                    };
                }
                Self::Raw(payload)
            }
            Self::TAG_IMAGE_FRAME => {
                if !slice.is_empty() {
                    let mime_len = slice.get_u8() as usize;
                    if slice.len() >= mime_len {
                        let mime = String::from_utf8_lossy(&slice[..mime_len]).to_string();
                        slice.advance(mime_len);
                        return Self::ImageFrame { mime, data: slice };
                    }
                }
                Self::Raw(payload)
            }
            _ => Self::Raw(payload),
        }
    }
}

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

    /// Decodes the payload into a typed sensor data variant.
    pub fn to_sensor_data(&self) -> SensorDataType {
        SensorDataType::decode(self.payload.clone())
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
    pub fn new(
        client: AsyncClient,
        topic: impl Into<String>,
        qos: QoS,
        journal_capacity: usize,
    ) -> Self {
        Self {
            topic: Arc::from(topic.into()),
            client,
            sequence: Arc::new(AtomicU64::new(1)),
            qos,
            recovery_journal: Arc::new(RwLock::new(VecDeque::with_capacity(journal_capacity))),
            journal_capacity,
        }
    }

    /// Streams a raw data payload concurrently from any worker thread with monotonic sequence ordering.
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

    /// Streams raw binary sensor packets (e.g. CAN bus, Modbus, SPI DMA).
    pub async fn send_raw(&self, data: &[u8]) -> Result<u64, ClientError> {
        let typed = SensorDataType::Raw(Bytes::copy_from_slice(data));
        self.send(typed.encode()).await
    }

    /// Streams a high-frequency time-series float array (e.g. IMU [x, y, z], power, vibration).
    pub async fn send_timeseries(&self, values: &[f64]) -> Result<u64, ClientError> {
        let typed = SensorDataType::TimeSeries(values.to_vec());
        self.send(typed.encode()).await
    }

    /// Streams a high-frequency 32-bit float time-series array.
    pub async fn send_timeseries_f32(&self, values: &[f32]) -> Result<u64, ClientError> {
        let typed = SensorDataType::TimeSeriesF32(values.to_vec());
        self.send(typed.encode()).await
    }

    /// Streams structured JSON text telemetry.
    pub async fn send_json(&self, json: impl Into<String>) -> Result<u64, ClientError> {
        let typed = SensorDataType::Json(json.into());
        self.send(typed.encode()).await
    }

    /// Streams audio PCM samples.
    pub async fn send_audio(
        &self,
        sample_rate: u32,
        channels: u8,
        pcm_data: impl Into<Bytes>,
    ) -> Result<u64, ClientError> {
        let typed = SensorDataType::AudioPcm {
            sample_rate,
            channels,
            data: pcm_data.into(),
        };
        self.send(typed.encode()).await
    }

    /// Streams camera, thermal, or vision image frames.
    pub async fn send_image(
        &self,
        mime: impl Into<String>,
        image_data: impl Into<Bytes>,
    ) -> Result<u64, ClientError> {
        let typed = SensorDataType::ImageFrame {
            mime: mime.into(),
            data: image_data.into(),
        };
        self.send(typed.encode()).await
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

    /// Receives the next in-order typed sensor payload.
    pub async fn recv_sensor_data(&mut self) -> Result<Option<(u64, SensorDataType)>, ClientError> {
        if let Some(chunk) = self.recv_ordered().await? {
            let data = chunk.to_sensor_data();
            Ok(Some((chunk.seq_id, data)))
        } else {
            Ok(None)
        }
    }

    /// The topic filter of the underlying stream subscription.
    pub fn topic_filter(&self) -> &str {
        self.subscription.topic_filter()
    }
}
