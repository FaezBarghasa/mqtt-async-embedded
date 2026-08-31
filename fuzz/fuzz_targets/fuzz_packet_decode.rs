#![no_main]

use libfuzzer_sys::fuzz_target;
use mqtt_packet::{DecodePacket, MqttVersion, decode};

fuzz_target!(|data: &[u8]| {
    // Fuzz generic packet decoding on MQTT 3.1.1 and 5.0
    let _ = decode(data, MqttVersion::V3_1_1);
    let _ = decode(data, MqttVersion::V5);

    // Fuzz individual packet decoders directly
    let _ = mqtt_packet::Connect::decode(data, MqttVersion::V3_1_1);
    let _ = mqtt_packet::Connect::decode(data, MqttVersion::V5);
    let _ = mqtt_packet::ConnAck::decode(data, MqttVersion::V3_1_1);
    let _ = mqtt_packet::ConnAck::decode(data, MqttVersion::V5);
    let _ = mqtt_packet::Publish::decode(data, MqttVersion::V3_1_1);
    let _ = mqtt_packet::Publish::decode(data, MqttVersion::V5);
    let _ = mqtt_packet::PubAck::decode(data, MqttVersion::V3_1_1);
    let _ = mqtt_packet::PubAck::decode(data, MqttVersion::V5);
    let _ = mqtt_packet::PubRec::decode(data, MqttVersion::V3_1_1);
    let _ = mqtt_packet::PubRec::decode(data, MqttVersion::V5);
    let _ = mqtt_packet::PubRel::decode(data, MqttVersion::V3_1_1);
    let _ = mqtt_packet::PubRel::decode(data, MqttVersion::V5);
    let _ = mqtt_packet::PubComp::decode(data, MqttVersion::V3_1_1);
    let _ = mqtt_packet::PubComp::decode(data, MqttVersion::V5);
    let _ = mqtt_packet::Subscribe::decode(data, MqttVersion::V3_1_1);
    let _ = mqtt_packet::Subscribe::decode(data, MqttVersion::V5);
    let _ = mqtt_packet::SubAck::decode(data, MqttVersion::V3_1_1);
    let _ = mqtt_packet::SubAck::decode(data, MqttVersion::V5);
    let _ = mqtt_packet::Unsubscribe::decode(data, MqttVersion::V3_1_1);
    let _ = mqtt_packet::Unsubscribe::decode(data, MqttVersion::V5);
    let _ = mqtt_packet::UnsubAck::decode(data, MqttVersion::V3_1_1);
    let _ = mqtt_packet::UnsubAck::decode(data, MqttVersion::V5);
    let _ = mqtt_packet::Disconnect::decode(data, MqttVersion::V3_1_1);
    let _ = mqtt_packet::Disconnect::decode(data, MqttVersion::V5);
});
