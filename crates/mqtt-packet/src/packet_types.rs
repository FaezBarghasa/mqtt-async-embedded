//! # MQTT Packet Data Structures

use crate::error::PacketError;
use crate::properties::Property;
use heapless::Vec;

/// Quality of Service (QoS) levels for MQTT messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
#[repr(u8)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl From<u8> for QoS {
    fn from(val: u8) -> Self {
        match val {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            _ => QoS::ExactlyOnce,
        }
    }
}

/// MQTT Protocol Version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MqttVersion {
    #[default]
    V3_1_1,
    V5,
}

/// Last Will and Testament configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Will<'a> {
    pub topic: &'a str,
    pub payload: &'a [u8],
    pub qos: QoS,
    pub retain: bool,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> Will<'a> {
    pub fn new(topic: &'a str, payload: &'a [u8], qos: QoS, retain: bool) -> Self {
        Self {
            topic,
            payload,
            qos,
            retain,
            properties: Vec::new(),
        }
    }
}

/// CONNECT packet.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Connect<'a> {
    pub clean_session: bool,
    pub keep_alive: u16,
    pub client_id: &'a str,
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
    pub will: Option<Will<'a>>,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> Connect<'a> {
    pub fn new(client_id: &'a str, keep_alive: u16, clean_session: bool) -> Self {
        Self {
            client_id,
            keep_alive,
            clean_session,
            username: None,
            password: None,
            will: None,
            properties: Vec::new(),
        }
    }
}

/// CONNACK packet.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct ConnAck<'a> {
    pub session_present: bool,
    pub reason_code: u8,
    pub properties: Vec<Property<'a>, 8>,
}

/// PUBLISH packet.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Publish<'a> {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: &'a str,
    pub packet_id: Option<u16>,
    pub payload: &'a [u8],
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> Publish<'a> {
    pub fn new(topic: &'a str, payload: &'a [u8], qos: QoS) -> Self {
        Self {
            dup: false,
            qos,
            retain: false,
            topic,
            packet_id: if qos == QoS::AtMostOnce { None } else { Some(1) },
            payload,
            properties: Vec::new(),
        }
    }
}

/// PUBACK packet (QoS 1 acknowledgement).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct PubAck<'a> {
    pub packet_id: u16,
    pub reason_code: u8,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> PubAck<'a> {
    pub fn new(packet_id: u16) -> Self {
        Self {
            packet_id,
            reason_code: 0,
            properties: Vec::new(),
        }
    }
}

/// PUBREC packet (QoS 2 publish received).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct PubRec<'a> {
    pub packet_id: u16,
    pub reason_code: u8,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> PubRec<'a> {
    pub fn new(packet_id: u16) -> Self {
        Self {
            packet_id,
            reason_code: 0,
            properties: Vec::new(),
        }
    }
}

/// PUBREL packet (QoS 2 publish release).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct PubRel<'a> {
    pub packet_id: u16,
    pub reason_code: u8,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> PubRel<'a> {
    pub fn new(packet_id: u16) -> Self {
        Self {
            packet_id,
            reason_code: 0,
            properties: Vec::new(),
        }
    }
}

/// PUBCOMP packet (QoS 2 publish complete).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct PubComp<'a> {
    pub packet_id: u16,
    pub reason_code: u8,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> PubComp<'a> {
    pub fn new(packet_id: u16) -> Self {
        Self {
            packet_id,
            reason_code: 0,
            properties: Vec::new(),
        }
    }
}

/// SUBSCRIBE packet.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Subscribe<'a> {
    pub packet_id: u16,
    pub topics: Vec<(&'a str, QoS), 8>,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> Subscribe<'a> {
    pub fn new(packet_id: u16) -> Self {
        Self {
            packet_id,
            topics: Vec::new(),
            properties: Vec::new(),
        }
    }

    pub fn add_topic(&mut self, topic: &'a str, qos: QoS) -> Result<(), PacketError> {
        self.topics
            .push((topic, qos))
            .map_err(|_| PacketError::BatchCapacityExceeded)
    }
}

/// SUBACK packet.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct SubAck<'a> {
    pub packet_id: u16,
    pub reason_codes: Vec<u8, 8>,
    pub properties: Vec<Property<'a>, 8>,
}

/// UNSUBSCRIBE packet.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Unsubscribe<'a> {
    pub packet_id: u16,
    pub topics: Vec<&'a str, 8>,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> Unsubscribe<'a> {
    pub fn new(packet_id: u16) -> Self {
        Self {
            packet_id,
            topics: Vec::new(),
            properties: Vec::new(),
        }
    }

    pub fn add_topic(&mut self, topic: &'a str) -> Result<(), PacketError> {
        self.topics
            .push(topic)
            .map_err(|_| PacketError::BatchCapacityExceeded)
    }
}

/// UNSUBACK packet.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct UnsubAck<'a> {
    pub packet_id: u16,
    pub reason_codes: Vec<u8, 8>,
    pub properties: Vec<Property<'a>, 8>,
}

/// PINGREQ packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct PingReq;

/// PINGRESP packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct PingResp;

/// DISCONNECT packet.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct Disconnect<'a> {
    pub reason_code: u8,
    pub properties: Vec<Property<'a>, 8>,
}

impl<'a> Disconnect<'a> {
    pub fn new() -> Self {
        Self {
            reason_code: 0,
            properties: Vec::new(),
        }
    }
}

impl<'a> Default for Disconnect<'a> {
    fn default() -> Self {
        Self::new()
    }
}

/// Enum of all decoded MQTT control packets.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum MqttPacket<'a> {
    Connect(Connect<'a>),
    ConnAck(ConnAck<'a>),
    Publish(Publish<'a>),
    PubAck(PubAck<'a>),
    PubRec(PubRec<'a>),
    PubRel(PubRel<'a>),
    PubComp(PubComp<'a>),
    Subscribe(Subscribe<'a>),
    SubAck(SubAck<'a>),
    Unsubscribe(Unsubscribe<'a>),
    UnsubAck(UnsubAck<'a>),
    PingReq,
    PingResp,
    Disconnect(Disconnect<'a>),
}
