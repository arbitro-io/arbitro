//! Drainer v2 — Command-pipeline scaffold (W4 Fase 2).
//!
//! This is the forward-looking replacement for `roles/drainer.rs`. It
//! demonstrates the full `store.for_each → group_ops::group → engine.execute
//! → dispatch` loop end-to-end for one stream, using the new kernel
//! vocabulary (`Command`, `MsgRef`, `CommandScratch`).
//!
//! It is **not** wired into `ShardWorker::run()` yet. The legacy drainer
//! in `roles/drainer.rs` remains the only path hit by live traffic. This
//! file exists so:
//!   1. The Command-based pipeline can be exercised against a real
//!      `ShardWorker` in isolated tests (parity + bench).
//!   2. The eventual swap in Fase 3 is a single call-site change in
//!      `worker.rs`, not a greenfield write.
//!
//! Scope of this scaffold — intentionally narrow:
//!   * Single stream per invocation (caller iterates `stores` if needed).
//!   * Fanout-only (every binding on the stream is treated as a fanout
//!     target with its own connection). Queue-group winner resolution
//!     will land when `StreamStateRef` is exposed from the engine.
//!   * Frame emission matches the legacy on-wire format (`RepBatch`
//!     envelope + `RepBatchFixed` + `DeliveryEntryHeader` × N).
//!   * Ack/inflight bookkeeping is NOT performed — this path is
//!     observational for bench purposes until `engine.execute(Command)`
//!     grows the real state-mutation body (currently metrics-only).
//!
//! See `plans/concurrent-coalescing-marble.md` Fase 2 for the full target.

use arbitro_engine_v2::command::{Command, MsgRef};
use arbitro_engine_v2::runtime::group_ops::{
    self, CommandScratch, FanoutTarget, StoreEntry,
};
use arbitro_engine_v2::types::{ConsumerId, StreamId};
use arbitro_proto::action::Action;
use arbitro_proto::wire::delivery::{DeliveryEntryHeader, RepBatchFixed};
use arbitro_proto::wire::envelope::{Envelope, ENVELOPE_SIZE};
use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::{U16, U32, U64};

use crate::shard::worker::ShardWorker;

/// Per-shard scratch owned by v2. Kept distinct from the legacy
/// `scratch_*` fields on `ShardWorker` so the two paths cannot alias.
#[derive(Default)]
pub struct DrainerV2Scratch {
    /// Reusable command buffer — zero-alloc after warmup.
    pub commands: CommandScratch,
    /// Per-target consumer lists, cleared between cycles.
    pub target_consumers: Vec<Vec<ConsumerId>>,
    /// Per-cycle StoreEntry holding area — lets us decouple the
    /// store `for_each` borrow from the `group_ops::group` call.
    pub entries: Vec<OwnedStoreEntry>,
    /// Reusable frame body.
    pub frame: BytesMut,
}

/// Heap-owned mirror of `StoreEntry<'a>`. The legacy store API yields
/// borrowed `Entry<'_>` inside a `for_each` closure; we copy subject
/// and clone `Bytes` (Arc bump) into this struct so the borrow can be
/// released before `group_ops::group` runs.
pub struct OwnedStoreEntry {
    pub seq: u64,
    pub timestamp_ms: u64,
    pub subject_hash: u32,
    pub subject: Vec<u8>,
    pub payload: Bytes,
    pub tombstoned: bool,
}

impl OwnedStoreEntry {
    fn as_store_entry(&self) -> StoreEntry<'_> {
        StoreEntry {
            seq: self.seq,
            timestamp_ms: self.timestamp_ms,
            subject_hash: self.subject_hash,
            subject: &self.subject,
            payload: self.payload.clone(),
            tombstoned: self.tombstoned,
        }
    }
}

impl ShardWorker {
    /// Run the Command-pipeline drain for a single stream.
    ///
    /// Returns the number of entries observed (whether delivered,
    /// tombstoned, or dropped). `0` means nothing was ready.
    ///
    /// NOT wired into the run loop — see module docs. Callable from
    /// tests / benches via `#[cfg(any(test, feature = "drainer-v2"))]`
    /// once that feature lands; meanwhile `#[allow(dead_code)]` keeps
    /// the scaffold compilable without warnings.
    #[allow(dead_code)]
    pub(in crate::shard) fn drain_stream_v2(
        &mut self,
        stream_id: StreamId,
        now_ms: u64,
        max_age_ms: u64,
        scratch: &mut DrainerV2Scratch,
    ) -> usize {
        // ── 1. Build FanoutTargets from bindings on this stream ─────
        scratch.target_consumers.clear();
        // One target per binding; keeps parity with the legacy drainer
        // which treats each binding as its own delivery site. Queue
        // coalescing will come with StreamStateRef.
        let mut target_specs: Vec<(
            arbitro_engine_v2::types::ConnectionId,
            mpsc::Sender<Bytes>,
        )> = Vec::new();

        for binding in &self.bindings {
            if binding.stream_id != stream_id || binding.paused {
                continue;
            }
            scratch
                .target_consumers
                .push(vec![binding.consumer_id]);
            target_specs.push((binding.connection_id, binding.tx.clone()));
        }

        if target_specs.is_empty() {
            return 0;
        }

        // ── 2. Load entries from store into OwnedStoreEntry ─────────
        scratch.entries.clear();
        let last = self.last_engine_seq.get(&stream_id).copied().unwrap_or(0);
        let Some(store) = self.stores.get(&stream_id) else {
            return 0;
        };
        let info = store.info();
        if info.last_seq <= last {
            return 0;
        }
        let start = last + 1;
        let cap = self.max_feed_per_cycle as u64;
        let end = (start + cap).min(info.last_seq + 1);

        let entries_out = &mut scratch.entries;
        store
            .for_each(start, end, &mut |entry| {
                entries_out.push(OwnedStoreEntry {
                    seq: entry.seq,
                    timestamp_ms: 0, // store Entry doesn't carry ts yet
                    subject_hash: arbitro_engine_v2::catalog::fnv1a_32(entry.subject),
                    subject: entry.subject.to_vec(),
                    payload: Bytes::copy_from_slice(entry.payload),
                    tombstoned: false,
                });
            })
            .ok();

        let observed = scratch.entries.len();
        if observed == 0 {
            return 0;
        }
        let last_seq = scratch.entries.last().map(|e| e.seq).unwrap_or(last);

        // ── 3. Build FanoutTargets borrowing from scratch ───────────
        let fanout_targets: Vec<FanoutTarget<'_>> = target_specs
            .iter()
            .zip(scratch.target_consumers.iter())
            .map(|((conn, _tx), consumers)| FanoutTarget {
                connection_id: *conn,
                consumers: consumers.as_slice(),
            })
            .collect();

        // ── 4. Run group_ops → CommandScratch ───────────────────────
        scratch.commands.clear();
        let owned_entries: Vec<StoreEntry<'_>> =
            scratch.entries.iter().map(|e| e.as_store_entry()).collect();
        group_ops::group(
            stream_id,
            owned_entries,
            now_ms,
            max_age_ms,
            &fanout_targets,
            |_hash, _seq| None, // no queue-group resolution yet
            &mut scratch.commands,
        );

        // ── 5. Dispatch: engine.execute + frame emission ────────────
        let engine = &mut self.engine;
        let names = &self.names;
        let frame_buf = &mut scratch.frame;
        group_ops::drain_into(&scratch.commands, &fanout_targets, |cmd| {
            // Observational engine execute — currently metrics-only.
            engine.execute(cmd);

            // Frame emission for Fanout. Queue/Tombstone/Ack/etc are
            // no-ops in this scaffold — the legacy path still owns
            // their wire emission.
            if let Command::Fanout {
                stream_id,
                connection_id,
                consumers,
                entries,
            } = *cmd
            {
                if entries.is_empty() || consumers.is_empty() {
                    return;
                }
                emit_fanout_frame(frame_buf, stream_id, consumers, entries, names);

                // Send via the first matching target's tx (lookup by
                // connection_id). Cheap linear scan — target_specs
                // is typically a handful of bindings.
                let frozen = frame_buf.split().freeze();
                if let Some((_, tx)) = target_specs
                    .iter()
                    .find(|(c, _)| *c == connection_id)
                {
                    let _ = tx.try_send(frozen);
                }
            }
        });

        // ── 6. Advance last_engine_seq ──────────────────────────────
        self.last_engine_seq.insert(stream_id, last_seq);

        observed
    }
}

/// Build a RepBatch frame into `buf`. Matches the legacy on-wire
/// layout from `roles/drainer.rs` so the client receiver path works
/// unchanged.
fn emit_fanout_frame(
    buf: &mut BytesMut,
    stream_id: StreamId,
    consumers: &[ConsumerId],
    entries: &[MsgRef<'_>],
    names: &crate::common::NameRegistry,
) {
    buf.clear();

    // Envelope placeholder — patched after body is complete.
    buf.extend_from_slice(&[0u8; ENVELOPE_SIZE]);

    // RepBatchFixed header — one frame carries entries for one
    // consumer. For multi-consumer fanout we emit the same payload
    // range but only address the first consumer; receiver-side demux
    // (SubjectTrie) handles per-sub routing. Queue-group distribution
    // to distinct consumers on the same connection will come when
    // queue winner resolution lands.
    let consumer_id = consumers.first().copied().unwrap_or(ConsumerId(0));
    buf.extend_from_slice(
        RepBatchFixed {
            consumer_id: U32::new(consumer_id.0),
            count: U16::new(entries.len() as u16),
            _pad: U16::new(0),
        }
        .as_bytes(),
    );

    for entry in entries {
        let subj_len = entry.subject.len();
        let data_len = subj_len + entry.payload.len();
        let header = DeliveryEntryHeader {
            seq: U64::new(entry.seq),
            subj_len: U16::new(subj_len as u16),
            data_len: U32::new(data_len as u32),
        };
        buf.extend_from_slice(header.as_bytes());
        buf.extend_from_slice(entry.subject);
        buf.extend_from_slice(&entry.payload);
    }

    // Patch envelope now that body_len is known.
    let wire_stream_id = names
        .stream_wire(stream_id)
        .unwrap_or_else(|| stream_id.raw());
    let body_len = buf.len() - ENVELOPE_SIZE;
    let envelope = Envelope::new(Action::RepBatch, wire_stream_id, body_len as u32, 0);
    buf[..ENVELOPE_SIZE].copy_from_slice(envelope.as_bytes());
}
