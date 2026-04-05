//! Benchmark: publish throughput with NoopTransport.
//!
//! Measures pure engine publish path — append + signal drain.
//! Max 1000 messages per iteration (bench safety rule).

use criterion::{criterion_group, criterion_main, Criterion, BatchSize};
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32};

use arbitro_engine::{EngineBuilder, NoopTransport};
use arbitro_proto::action::Action;
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use arbitro_proto::wire::publish::PublishEntry;

/// Build a publish frame with `count` entries, each with the given subject and payload.
fn build_publish_frame(stream_id: u32, subject: &[u8], payload: &[u8], count: u16) -> Vec<u8> {
    let entry_size = 12 + subject.len() + payload.len();
    let body_size = 2 + entry_size * count as usize;

    let envelope = Envelope {
        action: U16::new(Action::Publish.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(body_size as u32),
        env_seq: U32::new(1),
    };

    let mut frame = Vec::with_capacity(ENVELOPE_SIZE + body_size);
    frame.extend_from_slice(envelope.as_bytes());

    // Body: [2 count][entries...]
    frame.extend_from_slice(&count.to_le_bytes());

    let entry = PublishEntry {
        data_len: U32::new(payload.len() as u32),
        subj_len: U16::new(subject.len() as u16),
        reply_len: U16::new(0),
        flags: 0,
        _pad: [0u8; 3],
    };

    for _ in 0..count {
        frame.extend_from_slice(entry.as_bytes());
        frame.extend_from_slice(subject);
        frame.extend_from_slice(payload);
    }

    frame
}

/// Build a CreateStream frame.
fn build_create_stream_frame(name: &[u8]) -> Vec<u8> {
    use arbitro_proto::wire::stream::CreateStreamFixed;
    use zerocopy::byteorder::little_endian::U64;

    let fixed = CreateStreamFixed {
        name_len: U16::new(name.len() as u16),
        _pad: U16::new(0),
        max_msgs: U64::new(0),
        max_bytes: U64::new(0),
        max_age_secs: U64::new(0),
        replicas: 1,
        journal_kind: 0,
        retention: 0,
        _pad2: 0,
    };

    let body_len = 32 + name.len();
    let envelope = Envelope {
        action: U16::new(Action::CreateStream.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(0),
        msg_len: U32::new(body_len as u32),
        env_seq: U32::new(0),
    };

    let mut frame = Vec::with_capacity(ENVELOPE_SIZE + body_len);
    frame.extend_from_slice(envelope.as_bytes());
    frame.extend_from_slice(fixed.as_bytes());
    frame.extend_from_slice(name);
    frame
}

fn bench_publish_single(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish");
    group.measurement_time(std::time::Duration::from_secs(5));

    // Single message publish
    group.bench_function("single_msg_64B", |b| {
        b.iter_batched(
            || {
                let mut engine = EngineBuilder::new()
                    .transport(NoopTransport)
                    .build();

                let create = build_create_stream_frame(b"bench");
                engine.process_frame(1, &create);

                let payload = vec![0u8; 64];
                let frame = build_publish_frame(
                    arbitro_proto::config::fnv1a_32(b"bench"),
                    b"bench.test",
                    &payload,
                    1,
                );
                (engine, frame)
            },
            |(mut engine, frame)| {
                engine.process_frame(1, &frame);
            },
            BatchSize::SmallInput,
        );
    });

    // Batch of 100 messages
    group.bench_function("batch_100_msg_64B", |b| {
        b.iter_batched(
            || {
                let mut engine = EngineBuilder::new()
                    .transport(NoopTransport)
                    .build();

                let create = build_create_stream_frame(b"bench");
                engine.process_frame(1, &create);

                let payload = vec![0u8; 64];
                let frame = build_publish_frame(
                    arbitro_proto::config::fnv1a_32(b"bench"),
                    b"bench.test",
                    &payload,
                    100,
                );
                (engine, frame)
            },
            |(mut engine, frame)| {
                engine.process_frame(1, &frame);
            },
            BatchSize::SmallInput,
        );
    });

    // Batch of 1000 messages (max per safety rule)
    group.bench_function("batch_1000_msg_64B", |b| {
        b.iter_batched(
            || {
                let mut engine = EngineBuilder::new()
                    .transport(NoopTransport)
                    .build();

                let create = build_create_stream_frame(b"bench");
                engine.process_frame(1, &create);

                let payload = vec![0u8; 64];
                let frame = build_publish_frame(
                    arbitro_proto::config::fnv1a_32(b"bench"),
                    b"bench.test",
                    &payload,
                    1000,
                );
                (engine, frame)
            },
            |(mut engine, frame)| {
                engine.process_frame(1, &frame);
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_publish_single);
criterion_main!(benches);
