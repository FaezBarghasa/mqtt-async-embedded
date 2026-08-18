//! Unit and Integration Tests for High-Performance MQTT Engine

use mqtt_async_embedded::packet::{
    Connect, DecodePacket, Disconnect, EncodePacket, MqttPacket, PingReq, PubAck,
    Publish, QoS, Subscribe,
};
use mqtt_async_embedded::client::MqttVersion;
use mqtt_async_embedded::util::{
    peek_variable_byte_integer, read_utf8_string, write_utf8_string,
    write_variable_byte_integer, RawPacketFrameIter,
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
    sub.add_topic("telemetry/#", QoS::AtLeastOnce).unwrap();
    sub.add_topic("status/+", QoS::AtMostOnce).unwrap();

    let len = sub.encode(&mut buf, MqttVersion::V3).unwrap();
    let decoded = Subscribe::decode(&buf[..len], MqttVersion::V3).unwrap();
    assert_eq!(decoded.packet_id, 42);
    assert_eq!(decoded.topics.len(), 2);
    assert_eq!(decoded.topics[0], ("telemetry/#", QoS::AtLeastOnce));
    assert_eq!(decoded.topics[1], ("status/+", QoS::AtMostOnce));
}

#[test]
fn test_raw_packet_frame_iter_multipacket() {
    let mut stream_buf = [0u8; 512];
    let mut offset = 0;

    // Encode 3 packets back-to-back in the same buffer
    let p1 = Publish::new("a/1", b"one", QoS::AtMostOnce);
    let l1 = p1.encode(&mut stream_buf[offset..], MqttVersion::V3).unwrap();
    offset += l1;

    let p2 = Publish::new("a/2", b"two", QoS::AtMostOnce);
    let l2 = p2.encode(&mut stream_buf[offset..], MqttVersion::V3).unwrap();
    offset += l2;

    let ping_len = PingReq.encode(&mut stream_buf[offset..], MqttVersion::V3).unwrap();
    offset += ping_len;

    let iter = RawPacketFrameIter::new(&stream_buf[..offset]);
    let frames: heapless::Vec<&[u8], 8> = iter.map(|r| r.unwrap()).collect();

    assert_eq!(frames.len(), 3);
    assert_eq!(frames[0].len(), l1);
    assert_eq!(frames[1].len(), l2);
    assert_eq!(frames[2].len(), ping_len);
}

#[test]
fn test_user_property_malformed_truncated_does_not_panic() {
    // Malformed packet testing: User property 0x26 with truncated header
    // Property length: 4 bytes, Property ID: 0x26, but key length specifies bytes out of bounds
    let malformed_props = [0x04, 0x26, 0x00, 0x10]; // declares 16 bytes key len, but buffer ends
    let mut cursor = 0;
    let res = mqtt_async_embedded::util::read_properties(&mut cursor, &malformed_props);
    assert!(res.is_err());
}

#[test]
fn test_encode_empty_or_undersized_buffer_returns_buffer_too_small() {
    let mut empty_buf = [0u8; 0];
    let pub_msg = Publish::new("t", b"p", QoS::AtMostOnce);
    assert_eq!(
        pub_msg.encode(&mut empty_buf, MqttVersion::V3),
        Err(mqtt_async_embedded::error::MqttError::BufferTooSmall)
    );

    let connect_msg = Connect::new("client", 60, true);
    assert_eq!(
        connect_msg.encode(&mut empty_buf, MqttVersion::V3),
        Err(mqtt_async_embedded::error::MqttError::BufferTooSmall)
    );

    let puback = PubAck::new(1);
    assert_eq!(
        puback.encode(&mut empty_buf, MqttVersion::V3),
        Err(mqtt_async_embedded::error::MqttError::BufferTooSmall)
    );

    let mut sub = Subscribe::new(1);
    let _ = sub.add_topic("a", QoS::AtMostOnce);
    assert_eq!(
        sub.encode(&mut empty_buf, MqttVersion::V3),
        Err(mqtt_async_embedded::error::MqttError::BufferTooSmall)
    );

    let disc = Disconnect::new();
    assert_eq!(
        disc.encode(&mut empty_buf, MqttVersion::V3),
        Err(mqtt_async_embedded::error::MqttError::BufferTooSmall)
    );
}

#[test]
fn test_unsubscribe_encode_decode_roundtrip() {
    let mut buf = [0u8; 128];
    let mut unsub = mqtt_async_embedded::packet::Unsubscribe::new(101);
    unsub.add_topic("sensors/temp").unwrap();
    unsub.add_topic("sensors/humidity").unwrap();

    let len = unsub.encode(&mut buf, MqttVersion::V3).unwrap();
    let decoded = mqtt_async_embedded::packet::Unsubscribe::decode(&buf[..len], MqttVersion::V3).unwrap();

    assert_eq!(decoded.packet_id, 101);
    assert_eq!(decoded.topics.len(), 2);
    assert_eq!(decoded.topics[0], "sensors/temp");
    assert_eq!(decoded.topics[1], "sensors/humidity");
}

#[test]
fn test_connect_with_last_will_and_testament() {
    let mut buf = [0u8; 256];
    let mut connect = Connect::new("device-1", 30, true);
    connect.will = Some(mqtt_async_embedded::packet::Will::new(
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
    let packet = mqtt_async_embedded::packet::decode::<()>(&pubrec_bytes, MqttVersion::V3).unwrap().unwrap();
    if let MqttPacket::PubRec { packet_id, reason_code } = packet {
        assert_eq!(packet_id, 42);
        assert_eq!(reason_code, 0);
    } else {
        panic!("Expected PubRec");
    }

    // PUBREL header 0x62, remaining length 2, packet_id 43 (0x00, 0x2B)
    let pubrel_bytes = [0x62, 0x02, 0x00, 0x2B];
    let packet = mqtt_async_embedded::packet::decode::<()>(&pubrel_bytes, MqttVersion::V3).unwrap().unwrap();
    if let MqttPacket::PubRel { packet_id, reason_code } = packet {
        assert_eq!(packet_id, 43);
        assert_eq!(reason_code, 0);
    } else {
        panic!("Expected PubRel");
    }

    // PUBCOMP header 0x70, remaining length 2, packet_id 44 (0x00, 0x2C)
    let pubcomp_bytes = [0x70, 0x02, 0x00, 0x2C];
    let packet = mqtt_async_embedded::packet::decode::<()>(&pubcomp_bytes, MqttVersion::V3).unwrap().unwrap();
    if let MqttPacket::PubComp { packet_id, reason_code } = packet {
        assert_eq!(packet_id, 44);
        assert_eq!(reason_code, 0);
    } else {
        panic!("Expected PubComp");
    }
}
