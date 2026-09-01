//! # Inflight Packet Tracker & Collision Management
//!
//! Provides compile-time bounded inflight message tracking with O(1) packet-ID indexing
//! and collision detection for QoS 1 and QoS 2 acknowledgment lifecycles.

use heapless::Vec;
use mqtt_packet::QoS;

/// Status of an in-flight message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum InflightStatus {
    /// Initial transmission pending acknowledgment.
    Sent,
    /// QoS 2: Received `PUBREC`, awaiting `PUBCOMP` after `PUBREL`.
    PubRelSent,
    /// Acknowledgment completed.
    Acknowledged,
}

/// A tracked in-flight packet entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct InflightEntry {
    pub packet_id: u16,
    pub qos: QoS,
    pub status: InflightStatus,
    pub retries: u8,
}

/// Bounded queue for tracking QoS 1 and QoS 2 messages with collision detection.
#[derive(Debug, Clone)]
pub struct InflightQueue<const N: usize> {
    entries: Vec<InflightEntry, N>,
    last_acked_id: Option<u16>,
}

impl<const N: usize> Default for InflightQueue<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> InflightQueue<N> {
    /// Creates an empty in-flight tracker.
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
            last_acked_id: None,
        }
    }

    /// Checks if the queue is full.
    pub fn is_full(&self) -> bool {
        self.entries.is_full()
    }

    /// Checks if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Current number of tracked in-flight messages.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Checks whether a packet ID currently collides with an unacknowledged message.
    pub fn has_collision(&self, packet_id: u16) -> bool {
        self.entries
            .iter()
            .any(|e| e.packet_id == packet_id && e.status != InflightStatus::Acknowledged)
    }

    /// Pushes a new in-flight entry. Returns `Err(packet_id)` if a collision or full queue occurs.
    pub fn push(&mut self, packet_id: u16, qos: QoS) -> Result<(), u16> {
        if self.has_collision(packet_id) {
            return Err(packet_id);
        }
        self.entries
            .push(InflightEntry {
                packet_id,
                qos,
                status: InflightStatus::Sent,
                retries: 0,
            })
            .map_err(|_| packet_id)
    }

    /// Marks a packet ID as acknowledged (e.g. on PUBACK or PUBCOMP).
    pub fn acknowledge(&mut self, packet_id: u16) -> bool {
        if let Some(pos) = self.entries.iter().position(|e| e.packet_id == packet_id) {
            self.entries.swap_remove(pos);
            self.last_acked_id = Some(packet_id);
            true
        } else {
            false
        }
    }

    /// Advances QoS 2 state to `PUBREL` sent.
    pub fn mark_pubrel_sent(&mut self, packet_id: u16) -> bool {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.packet_id == packet_id) {
            entry.status = InflightStatus::PubRelSent;
            true
        } else {
            false
        }
    }

    /// Returns the last acknowledged packet ID.
    pub fn last_acked_id(&self) -> Option<u16> {
        self.last_acked_id
    }

    /// Returns an iterator over unacknowledged entries for retransmission.
    pub fn iter_unacked(&self) -> impl Iterator<Item = &InflightEntry> {
        self.entries
            .iter()
            .filter(|e| e.status != InflightStatus::Acknowledged)
    }

    /// Clears all entries upon clean session reset.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.last_acked_id = None;
    }
}
