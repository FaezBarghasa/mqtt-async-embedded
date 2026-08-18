//! Unit and Integration Tests for High-Performance MQTT Engine

use mqtt_async_embedded::packet::{
    Connect, ConnAck, DecodePacket, Disconnect, EncodePacket, MqttPacket, PingReq, PubAck,
    Publish, QoS, Subscribe, SubAck,
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
