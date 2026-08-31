//! Unit and Integration Tests for High-Performance MQTT Engine

use mqtt_packet::MqttVersion;
use mqtt_packet::{
    Connect, DecodePacket, Disconnect, EncodePacket, MqttPacket, PacketError, PingReq, PubAck,
    Publish, QoS, RawPacketFrameIter, Subscribe, peek_variable_byte_integer, read_utf8_string,
    write_utf8_string, write_variable_byte_integer,
};

#[test]
fn test_variable_byte_integer() {
    let mut buf = [0u8; 10];
    let mut cursor = 0;
    write_variable_byte_integer(&mut cursor, &mut buf, 64).unwrap();
    assert_eq!(cursor, 1);
    assert_eq!(buf[0], 64);

    let (val, len) = peek_variable_byte_integer(&buf[..cursor]).unwrap();
    assert_eq!(val, 64);
    assert_eq!(len, 1);

    cursor = 0;
    write_variable_byte_integer(&mut cursor, &mut buf, 321).unwrap();
    let (val, len) = peek_variable_byte_integer(&buf[..cursor]).unwrap();
    assert_eq!(val, 321);
    assert_eq!(len, cursor);
}

#[test]
fn test_utf8_string_codec() {
    let mut buf = [0u8; 64];
    let written = write_utf8_string(&mut buf, "sensors/temperature").unwrap();
    assert_eq!(written, 2 + 19);

    let mut cursor = 0;
    let decoded = read_utf8_string(&mut cursor, &buf[..written]).unwrap();
    assert_eq!(decoded, "sensors/temperature");
    assert_eq!(cursor, written);
}

#[test]
fn test_publish_encode_decode_roundtrip() {
    let mut buf = [0u8; 128];
    let pub_msg = Publish::new("home/livingroom/temp", b"23.8", QoS::AtLeastOnce);
    let len = pub_msg.encode(&mut buf, MqttVersion::V3).unwrap();

    let decoded = Publish::decode(&buf[..len], MqttVersion::V3).unwrap();
    assert_eq!(decoded.topic, "home/livingroom/temp");
    assert_eq!(decoded.payload, b"23.8");
    assert_eq!(decoded.qos, QoS::AtLeastOnce);
    assert_eq!(decoded.packet_id, Some(1));
}

#[test]
fn test_subscribe_encode_decode_roundtrip() {
    let mut buf = [0u8; 128];
    let mut sub = Subscribe::new(42);
    sub.add_topic("home/livingroom/temp", QoS::AtLeastOnce)
        .unwrap();
    sub.add_topic("home/kitchen/humidity", QoS::AtMostOnce)
        .unwrap();
    let len = sub.encode(&mut buf, MqttVersion::V3).unwrap();

    let decoded = Subscribe::decode(&buf[..len], MqttVersion::V3).unwrap();
    assert_eq!(decoded.packet_id, 42);
    assert_eq!(decoded.topics.len(), 2);
    assert_eq!(decoded.topics[0].0, "home/livingroom/temp");
    assert_eq!(decoded.topics[0].1, QoS::AtLeastOnce);
    assert_eq!(decoded.topics[1].0, "home/kitchen/humidity");
    assert_eq!(decoded.topics[1].1, QoS::AtMostOnce);
}

#[test]
fn test_raw_packet_frame_iter_multipacket() {
    let mut buffer = [0u8; 256];
    let mut cursor = 0;

    let p1 = Publish::new("topic1", b"payload1", QoS::AtMostOnce);
    let l1 = p1.encode(&mut buffer[cursor..], MqttVersion::V3).unwrap();
    cursor += l1;

    let ping = PingReq;
    let l2 = ping.encode(&mut buffer[cursor..], MqttVersion::V3).unwrap();
    cursor += l2;

    let p2 = Publish::new("topic2", b"payload2", QoS::AtLeastOnce);
    let l3 = p2.encode(&mut buffer[cursor..], MqttVersion::V3).unwrap();
    cursor += l3;

    let iter = RawPacketFrameIter::new(&buffer[..cursor]);
    let frames: Vec<&[u8]> = iter.map(|r| r.unwrap()).collect();

    assert_eq!(frames.len(), 3);
    assert_eq!(frames[0].len(), l1);
    assert_eq!(frames[1].len(), l2);
    assert_eq!(frames[2].len(), l3);
}

#[test]
fn test_user_property_malformed_truncated_does_not_panic() {
    let malformed_v5_prop = [
        0x30, 0x07, // PUBLISH, remaining length 7
        0x00, 0x01, b'a', // topic "a"
        0x03, // Property length 3
        0x26, 0x00, 0x05, b'k', // User Property: Key length 5, but only 1 byte present
    ];

    let result = Publish::decode(&malformed_v5_prop, MqttVersion::V5);
    assert!(result.is_err());
}

#[test]
fn test_encode_empty_or_undersized_buffer_returns_buffer_too_small() {
    let mut empty_buf = [0u8; 0];
    let pub_msg = Publish::new("a", b"b", QoS::AtMostOnce);
    assert_eq!(
        pub_msg.encode(&mut empty_buf, MqttVersion::V3),
        Err(PacketError::BufferTooSmall)
    );

    let connect_msg = Connect::new("client", 30, true);
    assert_eq!(
        connect_msg.encode(&mut empty_buf, MqttVersion::V3),
        Err(PacketError::BufferTooSmall)
    );

    let puback = PubAck::new(1);
    assert_eq!(
        puback.encode(&mut empty_buf, MqttVersion::V3),
        Err(PacketError::BufferTooSmall)
    );

    let mut sub = Subscribe::new(1);
    let _ = sub.add_topic("a", QoS::AtMostOnce);
    assert_eq!(
        sub.encode(&mut empty_buf, MqttVersion::V3),
        Err(PacketError::BufferTooSmall)
    );

    let disc = Disconnect::new();
    assert_eq!(
        disc.encode(&mut empty_buf, MqttVersion::V3),
        Err(PacketError::BufferTooSmall)
    );
}

#[test]
fn test_unsubscribe_encode_decode_roundtrip() {
    let mut buf = [0u8; 128];
    let mut unsub = mqtt_packet::Unsubscribe::new(101);
    unsub.add_topic("sensors/temp").unwrap();
    unsub.add_topic("sensors/humidity").unwrap();

    let len = unsub.encode(&mut buf, MqttVersion::V3).unwrap();
    let decoded = mqtt_packet::Unsubscribe::decode(&buf[..len], MqttVersion::V3).unwrap();

    assert_eq!(decoded.packet_id, 101);
    assert_eq!(decoded.topics.len(), 2);
    assert_eq!(decoded.topics[0], "sensors/temp");
    assert_eq!(decoded.topics[1], "sensors/humidity");
}

#[test]
fn test_connect_with_last_will_and_testament() {
    let mut buf = [0u8; 256];
    let mut connect = Connect::new("device-1", 30, true);
    connect.will = Some(mqtt_packet::Will::new(
        "device-1/status",
        b"offline",
        QoS::AtLeastOnce,
        true,
    ));

    let len = connect.encode(&mut buf, MqttVersion::V3).unwrap();
    let decoded = Connect::decode(&buf[..len], MqttVersion::V3).unwrap();

    assert_eq!(decoded.client_id, "device-1");
    assert_eq!(decoded.keep_alive, 30);
    assert!(decoded.clean_session);
    let will = decoded.will.expect("Will must be present");
    assert_eq!(will.topic, "device-1/status");
    assert_eq!(will.payload, b"offline");
    assert_eq!(will.qos, QoS::AtLeastOnce);
    assert!(will.retain);
}

#[test]
fn test_pubrec_pubrel_pubcomp_decoding() {
    // PUBREC header 0x50, remaining length 2, packet_id 42 (0x00, 0x2A)
    let pubrec_bytes = [0x50, 0x02, 0x00, 0x2A];
    let packet = mqtt_packet::decode(&pubrec_bytes, MqttVersion::V3)
        .unwrap()
        .unwrap();
    if let MqttPacket::PubRec(pubrec) = packet {
        assert_eq!(pubrec.packet_id, 42);
        assert_eq!(pubrec.reason_code, 0);
    } else {
        panic!("Expected PubRec");
    }

    // PUBREL header 0x62, remaining length 2, packet_id 43 (0x00, 0x2B)
    let pubrel_bytes = [0x62, 0x02, 0x00, 0x2B];
    let packet = mqtt_packet::decode(&pubrel_bytes, MqttVersion::V3)
        .unwrap()
        .unwrap();
    if let MqttPacket::PubRel(pubrel) = packet {
        assert_eq!(pubrel.packet_id, 43);
        assert_eq!(pubrel.reason_code, 0);
    } else {
        panic!("Expected PubRel");
    }

    // PUBCOMP header 0x70, remaining length 2, packet_id 44 (0x00, 0x2C)
    let pubcomp_bytes = [0x70, 0x02, 0x00, 0x2C];
    let packet = mqtt_packet::decode(&pubcomp_bytes, MqttVersion::V3)
        .unwrap()
        .unwrap();
    if let MqttPacket::PubComp(pubcomp) = packet {
        assert_eq!(pubcomp.packet_id, 44);
        assert_eq!(pubcomp.reason_code, 0);
    } else {
        panic!("Expected PubComp");
    }
}
