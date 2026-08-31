use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mqtt_packet::{
    Connect, EncodePacket, MqttVersion, Publish, QoS, decode, read_variable_byte_integer,
    write_variable_byte_integer,
};

fn bench_publish_encode_decode(c: &mut Criterion) {
    let payload = vec![0xAB; 256];
    let pub_pkt = Publish::new("sensor/temperature/living_room", &payload, QoS::AtLeastOnce);
    let mut buf = [0u8; 1024];

    c.bench_function("publish_encode_256b", |b| {
        b.iter(|| {
            let len = pub_pkt
                .encode(black_box(&mut buf), MqttVersion::V3_1_1)
                .unwrap();
            black_box(len);
        })
    });

    let encoded_len = pub_pkt.encode(&mut buf, MqttVersion::V3_1_1).unwrap();

    c.bench_function("publish_decode_256b", |b| {
        b.iter(|| {
            let pkt = decode(black_box(&buf[..encoded_len]), MqttVersion::V3_1_1).unwrap();
            black_box(pkt);
        })
    });
}

fn bench_varint_and_connect(c: &mut Criterion) {
    let mut buf = [0u8; 16];

    c.bench_function("varint_encode_decode", |b| {
        b.iter(|| {
            let mut cursor = 0;
            write_variable_byte_integer(&mut cursor, &mut buf, black_box(16384)).unwrap();
            let mut read_cursor = 0;
            let val = read_variable_byte_integer(&mut read_cursor, &buf).unwrap();
            black_box(val);
        })
    });

    let connect = Connect::new("embedded-sensor-node-01", 60, true);
    let mut conn_buf = [0u8; 256];

    c.bench_function("connect_packet_encode", |b| {
        b.iter(|| {
            let len = connect
                .encode(black_box(&mut conn_buf), MqttVersion::V5)
                .unwrap();
            black_box(len);
        })
    });
}

criterion_group!(
    benches,
    bench_publish_encode_decode,
    bench_varint_and_connect
);
criterion_main!(benches);
