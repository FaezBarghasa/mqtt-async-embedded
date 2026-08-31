//! # `mqtt-packet`
//!
//! A zero-allocation, `no_std`, `no_alloc` MQTT 3.1.1 and 5.0 packet encoder and decoder engine.

#![no_std]
#![forbid(unsafe_code)]

pub mod codec;
pub mod error;
pub mod packet_types;
pub mod properties;
pub mod varint;

pub use codec::{DecodePacket, EncodePacket, RawPacketFrameIter, decode};
pub use error::PacketError;
pub use packet_types::{
    ConnAck, Connect, Disconnect, MqttPacket, MqttVersion, PingReq, PingResp, PubAck, PubComp,
    PubRec, PubRel, Publish, QoS, SubAck, Subscribe, UnsubAck, Unsubscribe, Will,
};
pub use properties::{Property, read_properties, write_properties};
pub use varint::{
    peek_variable_byte_integer, read_binary_data, read_utf8_string, read_variable_byte_integer,
    write_binary_data, write_utf8_string, write_variable_byte_integer,
    write_variable_byte_integer_len,
};
