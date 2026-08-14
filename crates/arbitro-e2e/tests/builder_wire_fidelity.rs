//! Builder → wire → server field fidelity.
//!
//! `ConsumerBuilder` and `StreamBuilder` name every field, but between the
//! builder and the socket the value crosses three POSITIONAL hops:
//!
//! ```text
//! Builder::create()
//!   → Client::create_consumer_with_limits(11 positional args)
//!     → manage::create_consumer(11 positional args)
//!       → encode_create_consumer_v2(11 positional args)
//!         → CreateConsumer { .. }   // named at last
//! ```
//!
//! Every hop passes same-typed neighbours (`ack_policy`, `deliver_policy`
//! and `deliver_mode` are three consecutive `u8`s; `name`, `group` and
//! `subject` three consecutive `&[u8]`s). A transposition anywhere in that
//! chain compiles, encodes, and is accepted by the broker — it just means
//! something else. The unit tests in `consumer_builder.rs` cannot catch it:
//! they re-type the argument list themselves, so they would reproduce the
//! same swap.
//!
//! These tests read the value back out of an artefact **the server itself
//! wrote**, on the far side of a real socket: the metadata command log. The
//! dispatcher decodes the frame, then re-encodes the parsed fields into
//! `metadata.log` before replying `RepOk`. So a record in that file proves
//! three things at once — the byte left the client in the right slot, it
//! crossed the wire intact, and the server parsed it into the field it was
//! meant for.
//!
//! The consumer subject filter is the field that motivated this file: it
//! travels correctly and lands in the server's own log, yet delivery never
//! consults it (see `catalog_invariants::nested_consumer_filters_are_
//! independent`). These tests fence off the transport half of that
//! question, so the defect can only be on the delivery side.

mod test_helper;
use test_helper::TestServerBuilder;

use std::path::Path;

use arbitro_client_tokio::{
    AckPolicy, ConsumerBuilder, DeliverMode, DeliverPolicy, DiscardPolicy, JournalKind,
    RetentionPolicy, StreamBuilder,
};
use arbitro_proto::metadata::{MetadataCommandView, CMD_CREATE_CONSUMER, CMD_CREATE_STREAM};
use arbitro_proto::wire::manager::CreateConsumerView;
use arbitro_proto::wire::stream::CreateStreamView;

// ── Reading the server's own command log ─────────────────────────────────

/// Parse `metadata.log` into `(command_type, body)` pairs, in write order.
///
/// Framing is `[4 len_le][4 crc32_le][payload]` and the payload is
/// `[1 command_type][body]` — see `persistence::command_log`. The body is
/// byte-identical to the wire frame body for that action, which is why the
/// `wire::` views below can read it directly.
///
/// A partial trailing record is ignored rather than treated as corruption:
/// the cursor-persist task may be mid-write while we read, and every record
/// this file cares about was fsynced before its `RepOk` was sent.
fn read_metadata_log(data_dir: &Path) -> Vec<(u8, Vec<u8>)> {
    let raw = std::fs::read(data_dir.join("metadata.log"))
        .expect("the server must have written a metadata.log");

    let mut out = Vec::new();
    let mut pos = 0usize;
    while pos + 8 <= raw.len() {
        let len = u32::from_le_bytes(raw[pos..pos + 4].try_into().expect("4 bytes")) as usize;
        pos += 8; // length + crc32
        if pos + len > raw.len() {
            break; // torn tail from a concurrent write
        }
        let view = MetadataCommandView::new(&raw[pos..pos + len])
            .expect("the server wrote a well-formed metadata command");
        out.push((view.command_type(), view.body().to_vec()));
        pos += len;
    }
    out
}

/// The `CreateConsumer` record the server persisted under `name`.
fn consumer_record<'a>(records: &'a [(u8, Vec<u8>)], name: &[u8]) -> &'a [u8] {
    records
        .iter()
        .filter(|(kind, _)| *kind == CMD_CREATE_CONSUMER)
        .map(|(_, body)| body.as_slice())
        .find(|body| CreateConsumerView::new(body).name() == name)
        .unwrap_or_else(|| {
            panic!(
                "the server logged no CreateConsumer named `{}`",
                String::from_utf8_lossy(name)
            )
        })
}

/// The `CreateStream` record the server persisted under `name`.
fn stream_record<'a>(records: &'a [(u8, Vec<u8>)], name: &[u8]) -> &'a [u8] {
    records
        .iter()
        .filter(|(kind, _)| *kind == CMD_CREATE_STREAM)
        .map(|(_, body)| body.as_slice())
        .find(|body| CreateStreamView::new(body).name() == name)
        .unwrap_or_else(|| {
            panic!(
                "the server logged no CreateStream named `{}`",
                String::from_utf8_lossy(name)
            )
        })
}

// ═══════════════════════════════════════════════════════════════════════
// 1. ConsumerBuilder — every field lands in its own slot.
//
// Each value is distinct from every neighbour of the same type, so a
// transposition cannot hide behind a shared default. `max_inflight = 7`
// against `ack_wait_ms = 11_000`, `ack_policy = 1` against
// `deliver_policy = 2` against `deliver_mode = 1` — and the two subject
// limits differ in both pattern and value so their ORDER is pinned too.
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn consumer_builder_puts_every_field_in_its_own_slot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut server = TestServerBuilder::new()
        .data_dir(dir.path().to_str().expect("utf-8 temp path"))
        .spawn()
        .await;
    let client = server.connect().await;

    let stream_id = StreamBuilder::new(b"fidelity_consumer")
        .filter(b"orders.>")
        .create(&client)
        .await
        .expect("create_stream must succeed");

    ConsumerBuilder::new(b"fidelity_worker")
        .group(b"fidelity_group")
        .filter(b"orders.premium.>")
        .max_inflight(7)
        .ack_policy(AckPolicy::Explicit)
        .deliver_policy(DeliverPolicy::ByStartSeq)
        .deliver_mode(DeliverMode::Queue)
        .ack_wait_ms(11_000)
        .start_seq(4_242)
        .max_subject_inflight(b"orders.premium.eu.>", 3)
        .max_subject_inflight(b"orders.premium.us.>", 5)
        .create(&client, stream_id)
        .await
        .expect("create_consumer must succeed");

    // The record is written BEFORE the RepOk that unblocked the await
    // above, and the log fsyncs on every record, so it is already durable.
    server.shutdown().await;

    let records = read_metadata_log(dir.path());
    let view = CreateConsumerView::new(consumer_record(&records, b"fidelity_worker"));

    assert_eq!(view.stream_id(), stream_id, "stream_id");
    assert_eq!(view.name(), b"fidelity_worker", "name");
    assert_eq!(view.group(), b"fidelity_group", "group");
    assert_eq!(
        view.subject(),
        b"orders.premium.>",
        "the consumer subject filter must reach the server verbatim"
    );
    assert_eq!(view.max_inflight(), 7, "max_inflight");
    assert_eq!(view.ack_policy(), AckPolicy::Explicit as u8, "ack_policy");
    assert_eq!(
        view.deliver_policy(),
        DeliverPolicy::ByStartSeq as u8,
        "deliver_policy"
    );
    assert_eq!(
        view.deliver_mode(),
        DeliverMode::Queue as u8,
        "deliver_mode"
    );
    assert_eq!(view.ack_wait_ms(), 11_000, "ack_wait_ms");
    assert_eq!(view.start_seq(), 4_242, "start_seq");

    let limits: Vec<(Vec<u8>, u32)> = view
        .subject_limits()
        .map(|entry| (entry.pattern.to_vec(), entry.limit))
        .collect();
    assert_eq!(
        limits,
        vec![
            (b"orders.premium.eu.>".to_vec(), 3),
            (b"orders.premium.us.>".to_vec(), 5),
        ],
        "per-subject limits must arrive with their patterns and values paired \
         correctly, in builder order"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// 2. StreamBuilder — same contract.
//
// `replicas`, `journal_kind`, `retention` and `discard` are four adjacent
// `u8`s; they carry 3 / 2 / 1 / 0 here so every pairwise swap changes at
// least one assertion. The three `u64` limits likewise differ from each
// other and from the `u32` idempotency window.
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn stream_builder_puts_every_field_in_its_own_slot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut server = TestServerBuilder::new()
        .data_dir(dir.path().to_str().expect("utf-8 temp path"))
        .spawn()
        .await;
    let client = server.connect().await;

    StreamBuilder::new(b"fidelity_stream")
        .filter(b"telemetry.>")
        .max_msgs(1_111)
        .max_bytes(2_222)
        .max_age_secs(3_333)
        .replicas(3)
        .journal_kind(JournalKind::Tolerant)
        .retention(RetentionPolicy::Interest)
        .discard(DiscardPolicy::Old)
        .idempotency_window_ms(4_444)
        .create(&client)
        .await
        .expect("create_stream must succeed");

    server.shutdown().await;

    let records = read_metadata_log(dir.path());
    let view = CreateStreamView::new(stream_record(&records, b"fidelity_stream"));

    assert_eq!(view.name(), b"fidelity_stream", "name");
    assert_eq!(
        view.filter(),
        b"telemetry.>",
        "the stream subject filter must reach the server verbatim"
    );
    assert_eq!(view.max_msgs(), 1_111, "max_msgs");
    assert_eq!(view.max_bytes(), 2_222, "max_bytes");
    assert_eq!(
        view.max_age_secs(),
        3_333,
        "max_age_secs — the server converts to ms internally but logs seconds"
    );
    assert_eq!(view.replicas(), 3, "replicas");
    assert_eq!(
        view.journal_kind(),
        JournalKind::Tolerant as u8,
        "journal_kind"
    );
    assert_eq!(
        view.retention(),
        RetentionPolicy::Interest as u8,
        "retention"
    );
    assert_eq!(view.discard(), DiscardPolicy::Old as u8, "discard");
    assert_eq!(
        view.idempotency_window_ms(),
        4_444,
        "idempotency_window_ms"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// 3. Two consumers, two different filters — the server holds BOTH.
//
// This is the transport half of the open delivery question. Two consumers
// on one stream, each built with its own filter, produce two records that
// differ in exactly that field. So by the time the broker is deciding what
// to deliver, it is holding two distinct filters — and it still hands both
// consumers the same messages. That makes the defect a delivery-side one,
// not a client or wire one.
// ═══════════════════════════════════════════════════════════════════════

#[tokio::test(flavor = "multi_thread")]
async fn distinct_consumer_filters_arrive_distinct_at_the_server() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut server = TestServerBuilder::new()
        .data_dir(dir.path().to_str().expect("utf-8 temp path"))
        .spawn()
        .await;
    let client = server.connect().await;

    let stream_id = StreamBuilder::new(b"fidelity_two")
        .filter(b"orders.>")
        .create(&client)
        .await
        .expect("create_stream must succeed");

    for (name, filter) in [
        (&b"wide_reader"[..], &b"orders.>"[..]),
        (&b"nested_reader"[..], &b"orders.premium.>"[..]),
    ] {
        ConsumerBuilder::new(name)
            .filter(filter)
            .ack_policy(AckPolicy::Explicit)
            .max_inflight(100)
            .create(&client, stream_id)
            .await
            .expect("create_consumer must succeed");
    }

    server.shutdown().await;

    let records = read_metadata_log(dir.path());
    let wide = CreateConsumerView::new(consumer_record(&records, b"wide_reader"));
    let nested = CreateConsumerView::new(consumer_record(&records, b"nested_reader"));

    assert_eq!(wide.subject(), b"orders.>");
    assert_eq!(nested.subject(), b"orders.premium.>");
    assert_ne!(
        wide.subject(),
        nested.subject(),
        "the two consumers must reach the server carrying DIFFERENT filters — \
         identical delivery is therefore a delivery-side defect, not a \
         transport one"
    );
}
