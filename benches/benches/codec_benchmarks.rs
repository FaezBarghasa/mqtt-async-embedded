use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mqtt_packet::{EncodePacket, MqttVersion, Publish, QoS, decode};

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

criterion_group!(benches, bench_publish_encode_decode);
criterion_main!(benches);
