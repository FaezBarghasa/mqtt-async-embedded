use mqtt_packet::*;
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_varint_roundtrip(val in 0usize..=268_435_455) {
        let mut buf = [0u8; 4];
        let mut cursor = 0;
        write_variable_byte_integer(&mut cursor, &mut buf, val).unwrap();
        prop_assert_eq!(cursor, write_variable_byte_integer_len(&mut [0u8; 4], val).unwrap());

        let mut read_cursor = 0;
        let decoded = read_variable_byte_integer(&mut read_cursor, &buf).unwrap();
        prop_assert_eq!(read_cursor, cursor);
        prop_assert_eq!(decoded, val);
    }

    #[test]
    fn test_utf8_string_roundtrip(s in "\\PC{0,256}") {
        let mut buf = [0u8; 1024];
        let written = write_utf8_string(&mut buf, &s).unwrap();
        prop_assert_eq!(written, 2 + s.len());

        let mut cursor = 0;
        let decoded = read_utf8_string(&mut cursor, &buf[..written]).unwrap();
        prop_assert_eq!(cursor, written);
        prop_assert_eq!(decoded, s.as_str());
    }

    #[test]
    fn test_publish_packet_roundtrip(
        topic in "[a-zA-Z0-9_/]{1,64}",
        payload in proptest::collection::vec(any::<u8>(), 0..256),
        qos_raw in 0u8..=2,
        dup in any::<bool>(),
        retain in any::<bool>(),
        packet_id in 1u16..=65535,
    ) {
        let qos = QoS::from(qos_raw);
        let mut pub_pkt = Publish::new(&topic, &payload, qos);
        pub_pkt.dup = dup;
        pub_pkt.retain = retain;
        if qos != QoS::AtMostOnce {
            pub_pkt.packet_id = Some(packet_id);
        } else {
            pub_pkt.packet_id = None;
        }

        let mut buf = [0u8; 1024];
        let encoded_len = pub_pkt.encode(&mut buf, MqttVersion::V3_1_1).unwrap();

        let decoded = Publish::decode(&buf[..encoded_len], MqttVersion::V3_1_1).unwrap();
        prop_assert_eq!(decoded.topic, topic.as_str());
        prop_assert_eq!(decoded.payload, payload.as_slice());
        prop_assert_eq!(decoded.qos, qos);
        prop_assert_eq!(decoded.dup, dup);
        prop_assert_eq!(decoded.retain, retain);
        prop_assert_eq!(decoded.packet_id, pub_pkt.packet_id);
    }

    #[test]
    fn test_puback_roundtrip(packet_id in 1u16..=65535) {
        let ack = PubAck::new(packet_id);
        let mut buf = [0u8; 32];
        let len = ack.encode(&mut buf, MqttVersion::V3_1_1).unwrap();
        let decoded = PubAck::decode(&buf[..len], MqttVersion::V3_1_1).unwrap();
        prop_assert_eq!(decoded.packet_id, packet_id);
    }

    #[test]
    fn test_pubrec_pubrel_pubcomp_roundtrip(packet_id in 1u16..=65535) {
        {
            let rec = PubRec::new(packet_id);
            let mut buf = [0u8; 32];
            let len = rec.encode(&mut buf, MqttVersion::V3_1_1).unwrap();
            let dec_rec = PubRec::decode(&buf[..len], MqttVersion::V3_1_1).unwrap();
            prop_assert_eq!(dec_rec.packet_id, packet_id);
        }
        {
            let rel = PubRel::new(packet_id);
            let mut buf = [0u8; 32];
            let len = rel.encode(&mut buf, MqttVersion::V3_1_1).unwrap();
            let dec_rel = PubRel::decode(&buf[..len], MqttVersion::V3_1_1).unwrap();
            prop_assert_eq!(dec_rel.packet_id, packet_id);
        }
        {
            let comp = PubComp::new(packet_id);
            let mut buf = [0u8; 32];
            let len = comp.encode(&mut buf, MqttVersion::V3_1_1).unwrap();
            let dec_comp = PubComp::decode(&buf[..len], MqttVersion::V3_1_1).unwrap();
            prop_assert_eq!(dec_comp.packet_id, packet_id);
        }
    }

    #[test]
    fn test_subscribe_roundtrip(
        packet_id in 1u16..=65535,
        topic in "[a-zA-Z0-9_/]{1,32}",
        qos_raw in 0u8..=2,
    ) {
        let mut sub = Subscribe::new(packet_id);
        sub.add_topic(&topic, QoS::from(qos_raw)).unwrap();

        let mut buf = [0u8; 256];
        let len = sub.encode(&mut buf, MqttVersion::V3_1_1).unwrap();
        let decoded = Subscribe::decode(&buf[..len], MqttVersion::V3_1_1).unwrap();
        prop_assert_eq!(decoded.packet_id, packet_id);
        prop_assert_eq!(decoded.topics.len(), 1);
        prop_assert_eq!(decoded.topics[0].0, topic.as_str());
        prop_assert_eq!(decoded.topics[0].1, QoS::from(qos_raw));
    }

    #[test]
    fn test_unsubscribe_roundtrip(
        packet_id in 1u16..=65535,
        topic in "[a-zA-Z0-9_/]{1,32}",
    ) {
        let mut unsub = Unsubscribe::new(packet_id);
        unsub.add_topic(&topic).unwrap();

        let mut buf = [0u8; 256];
        let len = unsub.encode(&mut buf, MqttVersion::V3_1_1).unwrap();
        let decoded = Unsubscribe::decode(&buf[..len], MqttVersion::V3_1_1).unwrap();
        prop_assert_eq!(decoded.packet_id, packet_id);
        prop_assert_eq!(decoded.topics.len(), 1);
        prop_assert_eq!(decoded.topics[0], topic.as_str());
    }

    #[test]
    fn test_fuzz_random_bytes_no_panic(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = decode(&bytes, MqttVersion::V3_1_1);
        let _ = decode(&bytes, MqttVersion::V5);
        let _ = Connect::decode(&bytes, MqttVersion::V3_1_1);
        let _ = Connect::decode(&bytes, MqttVersion::V5);
        let _ = ConnAck::decode(&bytes, MqttVersion::V3_1_1);
        let _ = ConnAck::decode(&bytes, MqttVersion::V5);
        let _ = Publish::decode(&bytes, MqttVersion::V3_1_1);
        let _ = Publish::decode(&bytes, MqttVersion::V5);
        let _ = PubAck::decode(&bytes, MqttVersion::V3_1_1);
        let _ = PubAck::decode(&bytes, MqttVersion::V5);
        let _ = PubRec::decode(&bytes, MqttVersion::V3_1_1);
        let _ = PubRec::decode(&bytes, MqttVersion::V5);
        let _ = PubRel::decode(&bytes, MqttVersion::V3_1_1);
        let _ = PubRel::decode(&bytes, MqttVersion::V5);
        let _ = PubComp::decode(&bytes, MqttVersion::V3_1_1);
        let _ = PubComp::decode(&bytes, MqttVersion::V5);
        let _ = Subscribe::decode(&bytes, MqttVersion::V3_1_1);
        let _ = Subscribe::decode(&bytes, MqttVersion::V5);
        let _ = SubAck::decode(&bytes, MqttVersion::V3_1_1);
        let _ = SubAck::decode(&bytes, MqttVersion::V5);
        let _ = Unsubscribe::decode(&bytes, MqttVersion::V3_1_1);
        let _ = Unsubscribe::decode(&bytes, MqttVersion::V5);
        let _ = UnsubAck::decode(&bytes, MqttVersion::V3_1_1);
        let _ = UnsubAck::decode(&bytes, MqttVersion::V5);
        let _ = Disconnect::decode(&bytes, MqttVersion::V3_1_1);
        let _ = Disconnect::decode(&bytes, MqttVersion::V5);
    }
}
