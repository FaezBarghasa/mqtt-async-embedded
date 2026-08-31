//! # Bounded Inflight Queue for Embedded QoS 1 and QoS 2
//!
//! Provides zero-allocation inflight state tracking using `heapless::Vec`.

use crate::error::MqttError;
use crate::transport::TransportError;
use heapless::Vec;
use mqtt_packet::QoS;

/// Status of an inflight QoS 1 or QoS 2 message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum InflightStatus {
    /// QoS 1: Published, awaiting PUBACK
    AwaitingPubAck,
    /// QoS 2: Published, awaiting PUBREC
    AwaitingPubRec,
    /// QoS 2: Received PUBREC, sent PUBREL, awaiting PUBCOMP
    AwaitingPubComp,
    /// QoS 2 (Incoming): Received PUBLISH, sent PUBREC, awaiting PUBREL
    AwaitingPubRel,
}

/// An entry in the bounded inflight table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct InflightEntry {
    pub packet_id: u16,
    pub qos: QoS,
    pub status: InflightStatus,
}

/// Bounded inflight manager storing up to `MAX_INFLIGHT` active unacknowledged messages.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct InflightQueue<const MAX_INFLIGHT: usize> {
    entries: Vec<InflightEntry, MAX_INFLIGHT>,
}

impl<const MAX_INFLIGHT: usize> Default for InflightQueue<MAX_INFLIGHT> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const MAX_INFLIGHT: usize> InflightQueue<MAX_INFLIGHT> {
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Registers a newly dispatched QoS 1 or QoS 2 packet.
    pub fn track_outbound<T: TransportError>(
        &mut self,
        packet_id: u16,
        qos: QoS,
    ) -> Result<(), MqttError<T>> {
        let status = match qos {
            QoS::AtLeastOnce => InflightStatus::AwaitingPubAck,
            QoS::ExactlyOnce => InflightStatus::AwaitingPubRec,
            QoS::AtMostOnce => return Ok(()),
        };

        if let Some(existing) = self.entries.iter_mut().find(|e| e.packet_id == packet_id) {
            existing.status = status;
            return Ok(());
        }

        self.entries
            .push(InflightEntry {
                packet_id,
                qos,
                status,
            })
            .map_err(|_| MqttError::InflightQueueFull)
    }

    /// Tracks an incoming QoS 2 publish that we acknowledged with PUBREC.
    pub fn track_inbound_qos2<T: TransportError>(&mut self, packet_id: u16) -> Result<(), MqttError<T>> {
        if let Some(existing) = self.entries.iter_mut().find(|e| e.packet_id == packet_id) {
            existing.status = InflightStatus::AwaitingPubRel;
            return Ok(());
        }

        self.entries
            .push(InflightEntry {
                packet_id,
                qos: QoS::ExactlyOnce,
                status: InflightStatus::AwaitingPubRel,
            })
            .map_err(|_| MqttError::InflightQueueFull)
    }

    /// Handles incoming PUBACK for QoS 1.
    pub fn handle_puback(&mut self, packet_id: u16) -> bool {
        if let Some(idx) = self
            .entries
            .iter()
            .position(|e| e.packet_id == packet_id && e.status == InflightStatus::AwaitingPubAck)
        {
            self.entries.swap_remove(idx);
            true
        } else {
            false
        }
    }

    /// Handles incoming PUBREC for outbound QoS 2. Transitions to AwaitingPubComp.
    pub fn handle_pubrec(&mut self, packet_id: u16) -> bool {
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|e| e.packet_id == packet_id && e.status == InflightStatus::AwaitingPubRec)
        {
            entry.status = InflightStatus::AwaitingPubComp;
            true
        } else {
            false
        }
    }

    /// Handles incoming PUBREL for inbound QoS 2. Removes entry and allows PUBCOMP.
    pub fn handle_pubrel(&mut self, packet_id: u16) -> bool {
        if let Some(idx) = self
            .entries
            .iter()
            .position(|e| e.packet_id == packet_id && e.status == InflightStatus::AwaitingPubRel)
        {
            self.entries.swap_remove(idx);
            true
        } else {
            false
        }
    }

    /// Handles incoming PUBCOMP for outbound QoS 2. Removes entry.
    pub fn handle_pubcomp(&mut self, packet_id: u16) -> bool {
        if let Some(idx) = self
            .entries
            .iter()
            .position(|e| e.packet_id == packet_id && e.status == InflightStatus::AwaitingPubComp)
        {
            self.entries.swap_remove(idx);
            true
        } else {
            false
        }
    }

    /// Clears all inflight entries (e.g. on clean session reconnect).
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Returns current number of inflight packets.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if no messages are currently inflight.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
