use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32};
use arbitro_proto::wire::envelope::Envelope;
use arbitro_proto::wire::publish::PublishEntry;
use arbitro_proto::action::Action;

// ── Baseline: Current Manual (Multiple extends) ───────────────────

#[inline(always)]
fn build_manual(
    stream_id: u32,
    count: u16,
    subject: &[u8],
    payload: &[u8],
    buf: &mut Vec<u8>
) {
    buf.clear();
    let entry_header = PublishEntry {
        data_len: U32::new(payload.len() as u32),
        subj_len: U16::new(subject.len() as u16),
        reply_len: U16::new(0),
        flags: 0,
        _pad: [0; 3],
    };
    
    let env = Envelope {
        action: U16::new(Action::Publish.as_u16()),
        flags: 0,
        _rsv: 0,
        stream_id: U32::new(stream_id),
        msg_len: U32::new(0), // omitted for bench
        env_seq: U32::new(1),
    };

    buf.extend_from_slice(env.as_bytes());
    buf.extend_from_slice(&count.to_le_bytes());
    
    for _ in 0..count {
        buf.extend_from_slice(entry_header.as_bytes());
        buf.extend_from_slice(subject);
        buf.extend_from_slice(payload);
    }
}

// ── Proposed: Composite Struct (Single header write) ──────────────

#[repr(C, packed)]
#[derive(IntoBytes, zerocopy::Immutable, zerocopy::KnownLayout)]
struct CompositeHeader {
    env: Envelope,
    count: U16,
}

#[inline(always)]
fn build_struct(
    stream_id: u32,
    count: u16,
    subject: &[u8],
    payload: &[u8],
    buf: &mut Vec<u8>
) {
    buf.clear();
    let header = CompositeHeader {
        env: Envelope {
            action: U16::new(Action::Publish.as_u16()),
            flags: 0,
            _rsv: 0,
            stream_id: U32::new(stream_id),
            msg_len: U32::new(0),
            env_seq: U32::new(1),
        },
        count: U16::new(count),
    };
    
    let entry_header = PublishEntry {
        data_len: U32::new(payload.len() as u32),
        subj_len: U16::new(subject.len() as u16),
        reply_len: U16::new(0),
        flags: 0,
        _pad: [0; 3],
    };

    // One big extend for the overall frame header
    buf.extend_from_slice(header.as_bytes());
    
    for _ in 0..count {
        buf.extend_from_slice(entry_header.as_bytes());
        buf.extend_from_slice(subject);
        buf.extend_from_slice(payload);
    }
}

fn bench_protocol(c: &mut Criterion) {
    let mut buf = Vec::with_capacity(4096);
    let subject = b"orders.created";
    let payload = vec![0u8; 64];

    let mut group = c.benchmark_group("protocol_building");
    group.throughput(Throughput::Elements(1));

    group.bench_function("manual_multiple_extends", |b| {
        b.iter(|| {
            build_manual(black_box(1), black_box(10), black_box(subject), black_box(&payload), &mut buf);
        });
    });

    group.bench_function("struct_composite_header", |b| {
        b.iter(|| {
            build_struct(black_box(1), black_box(10), black_box(subject), black_box(&payload), &mut buf);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_protocol);
criterion_main!(benches);
