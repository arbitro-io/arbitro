//! Action dispatch benchmark: envelope → match action → zerocopy decode.
//!
//! Simulates the broker's hot path: read envelope, match on action,
//! decode the action-specific struct, extract fields.
//!
//! Measures:
//! 1. Individual action decode cost
//! 2. Full dispatch (match + decode) across mixed action types
//! 3. Variable-length actions (fixed header + name slice)

use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use zerocopy::byteorder::little_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

// ── Envelope (common to all frames) ─────────────────────────────────────────

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct Envelope {
    action: U16,
    flags: u8,
    _rsv: u8,
    stream_id: U32,
    msg_len: U32,
    env_seq: U32,
}
const ENV: usize = std::mem::size_of::<Envelope>();
const _: () = assert!(ENV == 16);

// ── Action codes ────────────────────────────────────────────────────────────

const ACT_PUBLISH: u16     = 0x0101;
const ACT_ACK: u16         = 0x0201;
const ACT_NACK: u16        = 0x0202;
const ACT_REPOK: u16       = 0x0203;
const ACT_REPERROR: u16    = 0x0204;
const ACT_SUBSCRIBE: u16   = 0x0301;
const ACT_UNSUBSCRIBE: u16 = 0x0302;
const ACT_CREATE_STREAM: u16 = 0x0401;
const ACT_DELETE_STREAM: u16 = 0x0402;
const ACT_PING: u16        = 0x0501;

// ── Action structs (all fixed-size, zerocopy) ───────────────────────────────

/// Publish entry header: [data_len][subj_len][reply_len][flags][pad×3]
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct PublishEntry {
    data_len: U32,
    subj_len: U16,
    reply_len: U16,
    flags: u8,
    _pad: [u8; 3],
}
const _: () = assert!(std::mem::size_of::<PublishEntry>() == 12);

/// Ack: confirm delivery of a message.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct AckAction {
    sequence: U64,
    consumer_id: U32,
    _pad: U32,
}
const _: () = assert!(std::mem::size_of::<AckAction>() == 16);

/// Nack: reject a message (redeliver).
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct NackAction {
    sequence: U64,
    consumer_id: U32,
    delay_ms: U32,
}
const _: () = assert!(std::mem::size_of::<NackAction>() == 16);

/// RepOk: server acknowledges a request.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct RepOkAction {
    ref_seq: U64,
    _pad: U64,
}
const _: () = assert!(std::mem::size_of::<RepOkAction>() == 16);

/// RepError: server reports an error.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct RepErrorAction {
    ref_seq: U64,
    error_code: U16,
    _pad: [u8; 6],
}
const _: () = assert!(std::mem::size_of::<RepErrorAction>() == 16);

/// Subscribe: fixed part. Subject follows as variable bytes.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct SubscribeFixed {
    consumer_id: U32,
    subj_len: U16,
    max_inflight: U16,
    deliver_policy: u8,
    deliver_mode: u8,
    _pad: [u8; 2],
    start_seq: U64,
}
const _: () = assert!(std::mem::size_of::<SubscribeFixed>() == 20);

/// Unsubscribe: fully fixed.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct UnsubscribeAction {
    consumer_id: U32,
    _pad: U32,
}
const _: () = assert!(std::mem::size_of::<UnsubscribeAction>() == 8);

/// CreateStream: fixed part. Name follows as variable bytes.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct CreateStreamFixed {
    name_len: U16,
    _pad: U16,
    max_msgs: U64,
    max_bytes: U64,
    max_age_secs: U64,
    replicas: u8,
    journal_kind: u8,
    _pad2: [u8; 2],
}
const _: () = assert!(std::mem::size_of::<CreateStreamFixed>() == 32);

/// DeleteStream: fixed part. Name follows as variable bytes.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct DeleteStreamFixed {
    name_len: U16,
    _pad: [u8; 6],
}
const _: () = assert!(std::mem::size_of::<DeleteStreamFixed>() == 8);

/// Ping: fully fixed, zero body.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Clone, Copy)]
#[repr(C)]
struct PingAction {
    ping_id: U64,
}
const _: () = assert!(std::mem::size_of::<PingAction>() == 8);

// ── Frame builders ──────────────────────────────────────────────────────────

fn build_frame(action: u16, body: &[u8]) -> Vec<u8> {
    let env = Envelope {
        action: action.into(),
        flags: 0,
        _rsv: 0,
        stream_id: 42u32.into(),
        msg_len: (body.len() as u32).into(),
        env_seq: 1u32.into(),
    };
    let mut buf = Vec::with_capacity(ENV + body.len());
    buf.extend_from_slice(env.as_bytes());
    buf.extend_from_slice(body);
    buf
}

fn build_publish() -> Vec<u8> {
    let subject = b"orders.created";
    let reply = b"_INBOX.abc";
    let data = b"{\"id\":1}";
    let eh = PublishEntry {
        data_len: (data.len() as u32).into(),
        subj_len: (subject.len() as u16).into(),
        reply_len: (reply.len() as u16).into(),
        flags: 0,
        _pad: [0; 3],
    };
    let count = 1u16;
    let mut body = Vec::new();
    body.extend_from_slice(&count.to_le_bytes());
    body.extend_from_slice(eh.as_bytes());
    body.extend_from_slice(subject);
    body.extend_from_slice(reply);
    body.extend_from_slice(data);
    build_frame(ACT_PUBLISH, &body)
}

fn build_ack() -> Vec<u8> {
    let a = AckAction { sequence: 42u64.into(), consumer_id: 7u32.into(), _pad: 0u32.into() };
    build_frame(ACT_ACK, a.as_bytes())
}

fn build_nack() -> Vec<u8> {
    let n = NackAction { sequence: 42u64.into(), consumer_id: 7u32.into(), delay_ms: 1000u32.into() };
    build_frame(ACT_NACK, n.as_bytes())
}

fn build_repok() -> Vec<u8> {
    let r = RepOkAction { ref_seq: 42u64.into(), _pad: 0u64.into() };
    build_frame(ACT_REPOK, r.as_bytes())
}

fn build_reperror() -> Vec<u8> {
    let r = RepErrorAction { ref_seq: 42u64.into(), error_code: 404u16.into(), _pad: [0; 6] };
    build_frame(ACT_REPERROR, r.as_bytes())
}

fn build_subscribe() -> Vec<u8> {
    let subject = b"orders.*";
    let s = SubscribeFixed {
        consumer_id: 1u32.into(),
        subj_len: (subject.len() as u16).into(),
        max_inflight: 100u16.into(),
        deliver_policy: 0,
        deliver_mode: 0,
        _pad: [0; 2],
        start_seq: 0u64.into(),
    };
    let mut body = Vec::new();
    body.extend_from_slice(s.as_bytes());
    body.extend_from_slice(subject);
    build_frame(ACT_SUBSCRIBE, &body)
}

fn build_unsubscribe() -> Vec<u8> {
    let u = UnsubscribeAction { consumer_id: 1u32.into(), _pad: 0u32.into() };
    build_frame(ACT_UNSUBSCRIBE, u.as_bytes())
}

fn build_create_stream() -> Vec<u8> {
    let name = b"ORDERS";
    let cs = CreateStreamFixed {
        name_len: (name.len() as u16).into(),
        _pad: 0u16.into(),
        max_msgs: 1_000_000u64.into(),
        max_bytes: (1024 * 1024 * 512u64).into(),
        max_age_secs: 86400u64.into(),
        replicas: 1,
        journal_kind: 0,
        _pad2: [0; 2],
    };
    let mut body = Vec::new();
    body.extend_from_slice(cs.as_bytes());
    body.extend_from_slice(name);
    build_frame(ACT_CREATE_STREAM, &body)
}

fn build_delete_stream() -> Vec<u8> {
    let name = b"ORDERS";
    let ds = DeleteStreamFixed {
        name_len: (name.len() as u16).into(),
        _pad: [0; 6],
    };
    let mut body = Vec::new();
    body.extend_from_slice(ds.as_bytes());
    body.extend_from_slice(name);
    build_frame(ACT_DELETE_STREAM, &body)
}

fn build_ping() -> Vec<u8> {
    let p = PingAction { ping_id: 1u64.into() };
    build_frame(ACT_PING, p.as_bytes())
}

// ── Dispatch function (simulates broker hot path) ───────────────────────────

#[derive(Debug)]
enum Dispatched<'a> {
    Publish { subject: &'a [u8], reply: &'a [u8], data: &'a [u8] },
    Ack { seq: u64, consumer_id: u32 },
    Nack { seq: u64, consumer_id: u32, delay_ms: u32 },
    RepOk { ref_seq: u64 },
    RepError { ref_seq: u64, code: u16 },
    Subscribe { consumer_id: u32, subject: &'a [u8] },
    Unsubscribe { consumer_id: u32 },
    CreateStream { name: &'a [u8], max_msgs: u64 },
    DeleteStream { name: &'a [u8] },
    Ping { id: u64 },
    Unknown(u16),
}

#[inline(always)]
fn dispatch(buf: &[u8]) -> Dispatched<'_> {
    let env = Envelope::ref_from_bytes(&buf[..ENV]).unwrap();
    let body = &buf[ENV..];

    match env.action.get() {
        ACT_PUBLISH => {
            let count_end = 2;
            let eh_start = count_end;
            let eh = PublishEntry::ref_from_bytes(
                &body[eh_start..eh_start + std::mem::size_of::<PublishEntry>()]
            ).unwrap();
            let sl = eh.subj_len.get() as usize;
            let rl = eh.reply_len.get() as usize;
            let dl = eh.data_len.get() as usize;
            let var = eh_start + std::mem::size_of::<PublishEntry>();
            Dispatched::Publish {
                subject: &body[var..var + sl],
                reply: &body[var + sl..var + sl + rl],
                data: &body[var + sl + rl..var + sl + rl + dl],
            }
        }
        ACT_ACK => {
            let a = AckAction::ref_from_bytes(&body[..std::mem::size_of::<AckAction>()]).unwrap();
            Dispatched::Ack { seq: a.sequence.get(), consumer_id: a.consumer_id.get() }
        }
        ACT_NACK => {
            let n = NackAction::ref_from_bytes(&body[..std::mem::size_of::<NackAction>()]).unwrap();
            Dispatched::Nack { seq: n.sequence.get(), consumer_id: n.consumer_id.get(), delay_ms: n.delay_ms.get() }
        }
        ACT_REPOK => {
            let r = RepOkAction::ref_from_bytes(&body[..std::mem::size_of::<RepOkAction>()]).unwrap();
            Dispatched::RepOk { ref_seq: r.ref_seq.get() }
        }
        ACT_REPERROR => {
            let r = RepErrorAction::ref_from_bytes(&body[..std::mem::size_of::<RepErrorAction>()]).unwrap();
            Dispatched::RepError { ref_seq: r.ref_seq.get(), code: r.error_code.get() }
        }
        ACT_SUBSCRIBE => {
            let s = SubscribeFixed::ref_from_bytes(&body[..std::mem::size_of::<SubscribeFixed>()]).unwrap();
            let sl = s.subj_len.get() as usize;
            let var = std::mem::size_of::<SubscribeFixed>();
            Dispatched::Subscribe { consumer_id: s.consumer_id.get(), subject: &body[var..var + sl] }
        }
        ACT_UNSUBSCRIBE => {
            let u = UnsubscribeAction::ref_from_bytes(&body[..std::mem::size_of::<UnsubscribeAction>()]).unwrap();
            Dispatched::Unsubscribe { consumer_id: u.consumer_id.get() }
        }
        ACT_CREATE_STREAM => {
            let cs = CreateStreamFixed::ref_from_bytes(&body[..std::mem::size_of::<CreateStreamFixed>()]).unwrap();
            let nl = cs.name_len.get() as usize;
            let var = std::mem::size_of::<CreateStreamFixed>();
            Dispatched::CreateStream { name: &body[var..var + nl], max_msgs: cs.max_msgs.get() }
        }
        ACT_DELETE_STREAM => {
            let ds = DeleteStreamFixed::ref_from_bytes(&body[..std::mem::size_of::<DeleteStreamFixed>()]).unwrap();
            let nl = ds.name_len.get() as usize;
            let var = std::mem::size_of::<DeleteStreamFixed>();
            Dispatched::DeleteStream { name: &body[var..var + nl] }
        }
        ACT_PING => {
            let p = PingAction::ref_from_bytes(&body[..std::mem::size_of::<PingAction>()]).unwrap();
            Dispatched::Ping { id: p.ping_id.get() }
        }
        other => Dispatched::Unknown(other),
    }
}

// ── Benchmarks ──────────────────────────────────────────────────────────────

fn bench_individual(c: &mut Criterion) {
    let publish = build_publish();
    let ack = build_ack();
    let nack = build_nack();
    let subscribe = build_subscribe();
    let create_stream = build_create_stream();
    let ping = build_ping();

    let mut g = c.benchmark_group("01_individual_dispatch");
    g.throughput(Throughput::Elements(1));
    g.sample_size(100);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(3));

    g.bench_function("publish", |b| {
        b.iter(|| black_box(dispatch(black_box(&publish))))
    });
    g.bench_function("ack", |b| {
        b.iter(|| black_box(dispatch(black_box(&ack))))
    });
    g.bench_function("nack", |b| {
        b.iter(|| black_box(dispatch(black_box(&nack))))
    });
    g.bench_function("subscribe", |b| {
        b.iter(|| black_box(dispatch(black_box(&subscribe))))
    });
    g.bench_function("create_stream", |b| {
        b.iter(|| black_box(dispatch(black_box(&create_stream))))
    });
    g.bench_function("ping", |b| {
        b.iter(|| black_box(dispatch(black_box(&ping))))
    });

    g.finish();
}

fn bench_mixed_dispatch(c: &mut Criterion) {
    // Realistic mix: 80% publish, 10% ack, 5% nack, 3% subscribe, 2% ping
    let frames: Vec<Vec<u8>> = (0..1000).map(|i| match i % 100 {
        0..80  => build_publish(),
        80..90 => build_ack(),
        90..95 => build_nack(),
        95..98 => build_subscribe(),
        _      => build_ping(),
    }).collect();

    let mut g = c.benchmark_group("02_mixed_dispatch");
    g.throughput(Throughput::Elements(1000));
    g.sample_size(100);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(3));

    g.bench_function("1000_frames_mixed", |b| {
        b.iter(|| {
            for f in &frames {
                black_box(dispatch(black_box(f)));
            }
        })
    });

    g.finish();
}

fn bench_correctness(_c: &mut Criterion) {
    // Verify all dispatches produce correct results
    match dispatch(&build_publish()) {
        Dispatched::Publish { subject, reply, data } => {
            assert_eq!(subject, b"orders.created");
            assert_eq!(reply, b"_INBOX.abc");
            assert_eq!(data, b"{\"id\":1}");
        }
        other => panic!("expected Publish, got {:?}", other),
    }
    match dispatch(&build_ack()) {
        Dispatched::Ack { seq: 42, consumer_id: 7 } => {}
        other => panic!("expected Ack, got {:?}", other),
    }
    match dispatch(&build_nack()) {
        Dispatched::Nack { seq: 42, consumer_id: 7, delay_ms: 1000 } => {}
        other => panic!("expected Nack, got {:?}", other),
    }
    match dispatch(&build_repok()) {
        Dispatched::RepOk { ref_seq: 42 } => {}
        other => panic!("expected RepOk, got {:?}", other),
    }
    match dispatch(&build_reperror()) {
        Dispatched::RepError { ref_seq: 42, code: 404 } => {}
        other => panic!("expected RepError, got {:?}", other),
    }
    match dispatch(&build_subscribe()) {
        Dispatched::Subscribe { consumer_id: 1, subject } => {
            assert_eq!(subject, b"orders.*");
        }
        other => panic!("expected Subscribe, got {:?}", other),
    }
    match dispatch(&build_unsubscribe()) {
        Dispatched::Unsubscribe { consumer_id: 1 } => {}
        other => panic!("expected Unsubscribe, got {:?}", other),
    }
    match dispatch(&build_create_stream()) {
        Dispatched::CreateStream { name, max_msgs: 1_000_000 } => {
            assert_eq!(name, b"ORDERS");
        }
        other => panic!("expected CreateStream, got {:?}", other),
    }
    match dispatch(&build_delete_stream()) {
        Dispatched::DeleteStream { name } => {
            assert_eq!(name, b"ORDERS");
        }
        other => panic!("expected DeleteStream, got {:?}", other),
    }
    match dispatch(&build_ping()) {
        Dispatched::Ping { id: 1 } => {}
        other => panic!("expected Ping, got {:?}", other),
    }
    eprintln!("all dispatch correctness checks passed");
}

// ── 03: Subject size impact ─────────────────────────────────────────────────

fn build_publish_sized(subject: &[u8], reply: &[u8], data: &[u8]) -> Vec<u8> {
    let eh = PublishEntry {
        data_len: (data.len() as u32).into(),
        subj_len: (subject.len() as u16).into(),
        reply_len: (reply.len() as u16).into(),
        flags: 0,
        _pad: [0; 3],
    };
    let count = 1u16;
    let mut body = Vec::new();
    body.extend_from_slice(&count.to_le_bytes());
    body.extend_from_slice(eh.as_bytes());
    body.extend_from_slice(subject);
    body.extend_from_slice(reply);
    body.extend_from_slice(data);
    build_frame(ACT_PUBLISH, &body)
}

fn bench_subject_sizes(c: &mut Criterion) {
    // Realistic subjects at different scales
    let tiny     = b"o.c";                                                          //  3B
    let short    = b"orders.created";                                               // 14B
    let medium   = b"devices.factory-01.line-3.sensor-temp.readings";               // 48B
    let long     = b"com.acme.production.us-east-1.factory-01.line-3.sensor-temp.readings.v2"; // 72B
    let huge     = b"com.acme.production.us-east-1.datacenter-04.building-a.floor-3.zone-west.rack-17.device-temperature-sensor-003.telemetry.readings.celsius.v2.raw"; // 155B

    let reply = b"_INBOX.xK7mQ9z";  // 15B — realistic inbox
    let data  = b"{\"temp\":22.5}";  // 14B — small payload

    let f_tiny   = build_publish_sized(tiny, reply, data);
    let f_short  = build_publish_sized(short, reply, data);
    let f_medium = build_publish_sized(medium, reply, data);
    let f_long   = build_publish_sized(long, reply, data);
    let f_huge   = build_publish_sized(huge, reply, data);

    eprintln!("\n=== Subject size impact ===");
    eprintln!("tiny   {:>3}B subject → frame {}B", tiny.len(), f_tiny.len());
    eprintln!("short  {:>3}B subject → frame {}B", short.len(), f_short.len());
    eprintln!("medium {:>3}B subject → frame {}B", medium.len(), f_medium.len());
    eprintln!("long   {:>3}B subject → frame {}B", long.len(), f_long.len());
    eprintln!("huge   {:>3}B subject → frame {}B", huge.len(), f_huge.len());
    eprintln!("===\n");

    let mut g = c.benchmark_group("03_subject_size");
    g.throughput(Throughput::Elements(1));
    g.sample_size(100);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(3));

    g.bench_function("03B_tiny", |b| {
        b.iter(|| black_box(dispatch(black_box(&f_tiny))))
    });
    g.bench_function("14B_short", |b| {
        b.iter(|| black_box(dispatch(black_box(&f_short))))
    });
    g.bench_function("48B_medium", |b| {
        b.iter(|| black_box(dispatch(black_box(&f_medium))))
    });
    g.bench_function("72B_long", |b| {
        b.iter(|| black_box(dispatch(black_box(&f_long))))
    });
    g.bench_function("155B_huge", |b| {
        b.iter(|| black_box(dispatch(black_box(&f_huge))))
    });

    g.finish();
}

// ── 04: Lazy view vs eager dispatch ─────────────────────────────────────────

/// Lazy view: stores raw &[u8], decodes on access via getters.
struct FrameView<'a> {
    buf: &'a [u8],
}

impl<'a> FrameView<'a> {
    #[inline(always)]
    fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    #[inline(always)]
    fn envelope(&self) -> &Envelope {
        Envelope::ref_from_bytes(&self.buf[..ENV]).unwrap()
    }

    #[inline(always)]
    fn action(&self) -> u16 {
        self.envelope().action.get()
    }

    #[inline(always)]
    fn stream_id(&self) -> u32 {
        self.envelope().stream_id.get()
    }

    #[inline(always)]
    fn body(&self) -> &'a [u8] {
        &self.buf[ENV..]
    }
}

/// Lazy publish view: only decodes fields when accessed.
struct PublishView<'a> {
    body: &'a [u8], // starts after envelope, at count
}

impl<'a> PublishView<'a> {
    #[inline(always)]
    fn new(body: &'a [u8]) -> Self {
        Self { body }
    }

    #[inline(always)]
    fn count(&self) -> u16 {
        u16::from_le_bytes([self.body[0], self.body[1]])
    }

    #[inline(always)]
    fn entry_header(&self) -> &PublishEntry {
        PublishEntry::ref_from_bytes(&self.body[2..2 + std::mem::size_of::<PublishEntry>()]).unwrap()
    }

    #[inline(always)]
    fn subject(&self) -> &'a [u8] {
        let sl = self.entry_header().subj_len.get() as usize;
        let start = 2 + std::mem::size_of::<PublishEntry>();
        &self.body[start..start + sl]
    }

    #[inline(always)]
    fn reply_to(&self) -> &'a [u8] {
        let eh = self.entry_header();
        let sl = eh.subj_len.get() as usize;
        let rl = eh.reply_len.get() as usize;
        let start = 2 + std::mem::size_of::<PublishEntry>() + sl;
        &self.body[start..start + rl]
    }

    #[inline(always)]
    fn payload(&self) -> &'a [u8] {
        let eh = self.entry_header();
        let sl = eh.subj_len.get() as usize;
        let rl = eh.reply_len.get() as usize;
        let dl = eh.data_len.get() as usize;
        let start = 2 + std::mem::size_of::<PublishEntry>() + sl + rl;
        &self.body[start..start + dl]
    }
}

/// Lazy ack view.
struct AckView<'a> {
    body: &'a [u8],
}

impl<'a> AckView<'a> {
    #[inline(always)]
    fn new(body: &'a [u8]) -> Self { Self { body } }

    #[inline(always)]
    fn inner(&self) -> &AckAction {
        AckAction::ref_from_bytes(&self.body[..std::mem::size_of::<AckAction>()]).unwrap()
    }

    #[inline(always)]
    fn sequence(&self) -> u64 { self.inner().sequence.get() }

    #[inline(always)]
    fn consumer_id(&self) -> u32 { self.inner().consumer_id.get() }
}

fn bench_lazy_vs_eager(c: &mut Criterion) {
    let publish = build_publish();
    let ack = build_ack();

    // Verify lazy correctness
    {
        let fv = FrameView::new(&publish);
        assert_eq!(fv.action(), ACT_PUBLISH);
        assert_eq!(fv.stream_id(), 42);
        let pv = PublishView::new(fv.body());
        assert_eq!(pv.count(), 1);
        assert_eq!(pv.subject(), b"orders.created");
        assert_eq!(pv.reply_to(), b"_INBOX.abc");
        assert_eq!(pv.payload(), b"{\"id\":1}");
    }
    {
        let fv = FrameView::new(&ack);
        assert_eq!(fv.action(), ACT_ACK);
        let av = AckView::new(fv.body());
        assert_eq!(av.sequence(), 42);
        assert_eq!(av.consumer_id(), 7);
    }

    let mut g = c.benchmark_group("04_lazy_vs_eager");
    g.throughput(Throughput::Elements(1));
    g.sample_size(100);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(3));

    // ── Publish: eager (dispatch extracts all fields) ──
    g.bench_function("publish_eager_all", |b| {
        b.iter(|| black_box(dispatch(black_box(&publish))))
    });

    // ── Publish: lazy, access ALL fields (worst case) ──
    g.bench_function("publish_lazy_all", |b| {
        b.iter(|| {
            let fv = FrameView::new(black_box(&publish));
            let _action = black_box(fv.action());
            let _sid = black_box(fv.stream_id());
            let pv = PublishView::new(fv.body());
            let _subj = black_box(pv.subject());
            let _reply = black_box(pv.reply_to());
            let _data = black_box(pv.payload());
        })
    });

    // ── Publish: lazy, only subject (hot path — routing) ──
    g.bench_function("publish_lazy_subject_only", |b| {
        b.iter(|| {
            let fv = FrameView::new(black_box(&publish));
            let _action = black_box(fv.action());
            let pv = PublishView::new(fv.body());
            let _subj = black_box(pv.subject());
        })
    });

    // ── Publish: lazy, only action (dispatch decision) ──
    g.bench_function("publish_lazy_action_only", |b| {
        b.iter(|| {
            let fv = FrameView::new(black_box(&publish));
            black_box(fv.action());
        })
    });

    // ── Ack: eager vs lazy ──
    g.bench_function("ack_eager_all", |b| {
        b.iter(|| black_box(dispatch(black_box(&ack))))
    });

    g.bench_function("ack_lazy_all", |b| {
        b.iter(|| {
            let fv = FrameView::new(black_box(&ack));
            let _action = black_box(fv.action());
            let av = AckView::new(fv.body());
            let _seq = black_box(av.sequence());
            let _cid = black_box(av.consumer_id());
        })
    });

    g.finish();
}

// ── 05: Lazy batch iteration ────────────────────────────────────────────────

fn build_batch_publish(count: usize, subject: &[u8], reply: &[u8], data: &[u8]) -> Vec<u8> {
    let entry_body = std::mem::size_of::<PublishEntry>() + subject.len() + reply.len() + data.len();
    let msg_len = 2 + count * entry_body;

    let mut buf = Vec::with_capacity(ENV + msg_len);
    let env = Envelope {
        action: ACT_PUBLISH.into(),
        flags: 0, _rsv: 0,
        stream_id: 42u32.into(),
        msg_len: (msg_len as u32).into(),
        env_seq: 1u32.into(),
    };
    buf.extend_from_slice(env.as_bytes());
    buf.extend_from_slice(&(count as u16).to_le_bytes());

    for _ in 0..count {
        let eh = PublishEntry {
            data_len: (data.len() as u32).into(),
            subj_len: (subject.len() as u16).into(),
            reply_len: (reply.len() as u16).into(),
            flags: 0, _pad: [0; 3],
        };
        buf.extend_from_slice(eh.as_bytes());
        buf.extend_from_slice(subject);
        buf.extend_from_slice(reply);
        buf.extend_from_slice(data);
    }
    buf
}

/// Lazy batch iterator — yields PublishView per entry without allocating.
struct BatchIter<'a> {
    body: &'a [u8],
    offset: usize,
    remaining: u16,
}

impl<'a> BatchIter<'a> {
    #[inline(always)]
    fn new(body: &'a [u8]) -> Self {
        let count = u16::from_le_bytes([body[0], body[1]]);
        Self { body, offset: 2, remaining: count }
    }
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = PublishView<'a>;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 { return None; }
        self.remaining -= 1;

        let eh = PublishEntry::ref_from_bytes(
            &self.body[self.offset..self.offset + std::mem::size_of::<PublishEntry>()]
        ).unwrap();
        let entry_start = self.offset;
        let sl = eh.subj_len.get() as usize;
        let rl = eh.reply_len.get() as usize;
        let dl = eh.data_len.get() as usize;
        // Advance past this entry: header + subject + reply + data
        self.offset += std::mem::size_of::<PublishEntry>() + sl + rl + dl;

        // Return a view starting at entry header (skip count prefix for sub-view)
        // We need to adjust: PublishView expects body starting at count,
        // but for iteration we build a view directly over the entry.
        // Simplest: return a slice starting 2 bytes before entry header
        // Actually, let's just make an EntryView instead.
        Some(PublishView { body: &self.body[entry_start - 2..] })
    }
}

fn bench_lazy_batch(c: &mut Criterion) {
    let subject = b"orders.created";
    let reply = b"_INBOX.xK7mQ9z";
    let data = b"{\"temp\":22.5}";

    let batch_100 = build_batch_publish(100, subject, reply, data);
    let batch_1000 = build_batch_publish(1000, subject, reply, data);

    // Verify lazy batch correctness
    {
        let fv = FrameView::new(&batch_100);
        let mut iter = BatchIter::new(fv.body());
        let first = iter.next().unwrap();
        assert_eq!(first.subject(), subject);
        assert_eq!(first.reply_to(), reply);
        assert_eq!(first.payload(), data);
        assert_eq!(iter.count(), 99); // 99 remaining
    }

    eprintln!("batch 100: {}B, batch 1000: {}B", batch_100.len(), batch_1000.len());

    let mut g = c.benchmark_group("05_lazy_batch");
    g.sample_size(100);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(3));

    // Iterate 100 entries, access only subject per entry
    g.throughput(Throughput::Elements(100));
    g.bench_function("100_entries_subject_only", |b| {
        b.iter(|| {
            let fv = FrameView::new(black_box(&batch_100));
            let iter = BatchIter::new(fv.body());
            for entry in iter {
                black_box(entry.subject());
            }
        })
    });

    // Iterate 100 entries, access ALL fields per entry
    g.bench_function("100_entries_all_fields", |b| {
        b.iter(|| {
            let fv = FrameView::new(black_box(&batch_100));
            let iter = BatchIter::new(fv.body());
            for entry in iter {
                black_box(entry.subject());
                black_box(entry.reply_to());
                black_box(entry.payload());
            }
        })
    });

    // Iterate 1000 entries, subject only
    g.throughput(Throughput::Elements(1000));
    g.bench_function("1000_entries_subject_only", |b| {
        b.iter(|| {
            let fv = FrameView::new(black_box(&batch_1000));
            let iter = BatchIter::new(fv.body());
            for entry in iter {
                black_box(entry.subject());
            }
        })
    });

    // Iterate 1000 entries, all fields
    g.bench_function("1000_entries_all_fields", |b| {
        b.iter(|| {
            let fv = FrameView::new(black_box(&batch_1000));
            let iter = BatchIter::new(fv.body());
            for entry in iter {
                black_box(entry.subject());
                black_box(entry.reply_to());
                black_box(entry.payload());
            }
        })
    });

    g.finish();
}

// ── 06: Lazy view vs raw zerocopy (no wrapper) ─���───────────────────────────

fn bench_view_vs_raw(c: &mut Criterion) {
    let publish = build_publish();
    let ack = build_ack();

    let mut g = c.benchmark_group("06_view_vs_raw_zerocopy");
    g.throughput(Throughput::Elements(1));
    g.sample_size(100);
    g.warm_up_time(Duration::from_secs(1));
    g.measurement_time(Duration::from_secs(3));

    // ── Publish: all fields ──

    // Raw zerocopy — direct ref_from_bytes, no wrapper
    g.bench_function("publish_raw_all", |b| {
        b.iter(|| {
            let buf = black_box(&publish);
            let env = Envelope::ref_from_bytes(&buf[..ENV]).unwrap();
            let _action = black_box(env.action.get());
            let _sid = black_box(env.stream_id.get());
            let body = &buf[ENV..];
            let eh = PublishEntry::ref_from_bytes(&body[2..2 + std::mem::size_of::<PublishEntry>()]).unwrap();
            let sl = eh.subj_len.get() as usize;
            let rl = eh.reply_len.get() as usize;
            let dl = eh.data_len.get() as usize;
            let var = 2 + std::mem::size_of::<PublishEntry>();
            let _subj = black_box(&body[var..var + sl]);
            let _reply = black_box(&body[var + sl..var + sl + rl]);
            let _data = black_box(&body[var + sl + rl..var + sl + rl + dl]);
        })
    });

    // Lazy view — same fields through wrapper
    g.bench_function("publish_view_all", |b| {
        b.iter(|| {
            let fv = FrameView::new(black_box(&publish));
            let _action = black_box(fv.action());
            let _sid = black_box(fv.stream_id());
            let pv = PublishView::new(fv.body());
            let _subj = black_box(pv.subject());
            let _reply = black_box(pv.reply_to());
            let _data = black_box(pv.payload());
        })
    });

    // ── Publish: subject only ──

    g.bench_function("publish_raw_subject_only", |b| {
        b.iter(|| {
            let buf = black_box(&publish);
            let env = Envelope::ref_from_bytes(&buf[..ENV]).unwrap();
            let _action = black_box(env.action.get());
            let body = &buf[ENV..];
            let eh = PublishEntry::ref_from_bytes(&body[2..2 + std::mem::size_of::<PublishEntry>()]).unwrap();
            let sl = eh.subj_len.get() as usize;
            let var = 2 + std::mem::size_of::<PublishEntry>();
            let _subj = black_box(&body[var..var + sl]);
        })
    });

    g.bench_function("publish_view_subject_only", |b| {
        b.iter(|| {
            let fv = FrameView::new(black_box(&publish));
            let _action = black_box(fv.action());
            let pv = PublishView::new(fv.body());
            let _subj = black_box(pv.subject());
        })
    });

    // ── Ack: all fields ──

    g.bench_function("ack_raw_all", |b| {
        b.iter(|| {
            let buf = black_box(&ack);
            let env = Envelope::ref_from_bytes(&buf[..ENV]).unwrap();
            let _action = black_box(env.action.get());
            let body = &buf[ENV..];
            let a = AckAction::ref_from_bytes(&body[..std::mem::size_of::<AckAction>()]).unwrap();
            let _seq = black_box(a.sequence.get());
            let _cid = black_box(a.consumer_id.get());
        })
    });

    g.bench_function("ack_view_all", |b| {
        b.iter(|| {
            let fv = FrameView::new(black_box(&ack));
            let _action = black_box(fv.action());
            let av = AckView::new(fv.body());
            let _seq = black_box(av.sequence());
            let _cid = black_box(av.consumer_id());
        })
    });

    g.finish();
}

criterion_group!(benches, bench_correctness, bench_individual, bench_mixed_dispatch, bench_subject_sizes, bench_lazy_vs_eager, bench_lazy_batch, bench_view_vs_raw);
criterion_main!(benches);
