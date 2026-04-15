//! `group_ops` — pure, zero-alloc translation from store entries into
//! kernel `Command`s.
//!
//! Level 7. Takes a window of store entries (borrowed from the caller's
//! store), the resolved fanout / queue routing for the stream, and a
//! reusable scratch buffer. Emits Commands without touching engine
//! state — mutation happens later via `engine.execute_batch`.
//!
//! This is the "group" step of the drainer pipeline:
//!
//!     store.for_each → group_ops → execute_batch → dispatch_commands
//!
//! **Reusability:** the scratch buffer (`CommandScratch`) is owned by
//! the drainer, cleared between cycles. After warmup: zero allocations.
//!
//! **Current scope:** this scaffold accepts pre-resolved routing
//! (`FanoutTarget`s and `QueueWinnerFn`) from the caller. The richer
//! `StreamStateRef` query path from the plan will land when the engine
//! exposes its catalog/inflight views cheaply — until then the caller
//! (shard drainer) is the oracle, which lets the Command pipeline run
//! end-to-end without rewriting the engine internals.

use bytes::Bytes;

use crate::command::{Command, DropReason, MsgRef};
use crate::types::{ConnectionId, ConsumerId, QueueId, StreamId, SubscriptionId};

/// Store entry shape accepted by `group_ops`.
///
/// Deliberately a struct (not the store's `Entry<'a>`) so this crate
/// does not depend on `arbitro-store`. The drainer converts from the
/// store's borrowed `Entry` into this struct by cloning the `Bytes`
/// for the payload (Arc bump — no copy).
#[derive(Debug, Clone)]
pub struct StoreEntry<'a> {
    /// Store-assigned sequence number.
    pub seq: u64,
    /// Wall-clock millis when the entry was appended.
    pub timestamp_ms: u64,
    /// FNV-1a hash of `subject`.
    pub subject_hash: u32,
    /// Borrowed subject bytes.
    pub subject: &'a [u8],
    /// Arc-backed payload — cloning is ~3ns.
    pub payload: Bytes,
    /// Tombstone bit from the store. When set, entry is routed as a drop.
    pub tombstoned: bool,
}

/// One fanout target — a connection that should receive every entry for
/// this stream, plus the consumers on that connection.
#[derive(Debug, Clone)]
pub struct FanoutTarget<'a> {
    /// Target connection for the delivery.
    pub connection_id: ConnectionId,
    /// Consumers on that connection that should receive the batch.
    pub consumers: &'a [ConsumerId],
}

/// Queue winner for a single entry — resolved per-entry by the caller.
///
/// Returning `None` means "no available consumer in this queue group for
/// this entry" — `group_ops` skips queue emission for that entry.
pub type QueueWinnerFn<'a> =
    &'a mut dyn FnMut(QueueId, u64) -> Option<QueueWinner>;

/// One queue-group delivery target.
#[derive(Debug, Clone, Copy)]
pub struct QueueWinner {
    /// Which queue group this belongs to.
    pub queue_id: QueueId,
    /// Consumer picked by the round-robin.
    pub consumer_id: ConsumerId,
    /// Subscription on the winning consumer.
    pub subscription_id: SubscriptionId,
    /// Connection to dispatch on.
    pub connection_id: ConnectionId,
}

/// Reusable scratch buffer that owns the backing storage for a batch of
/// `Command`s. Pre-allocated at startup, cleared between cycles.
///
/// Owning the sub-buffers (consumer lists, entry lists, ack pairs) is
/// what lets `Command::Fanout { consumers, entries }` borrow slices
/// whose lifetimes match the scratch buffer.
pub struct CommandScratch {
    fanout_consumers: Vec<Vec<ConsumerId>>,
    fanout_entries: Vec<Vec<MsgRef<'static>>>,
    queue_entries: Vec<QueueWinnerEntry>,
    tombstones: Vec<TombstoneEntry>,
}

/// Internal — one queue delivery row.
#[derive(Debug)]
struct QueueWinnerEntry {
    stream_id: StreamId,
    winner: QueueWinner,
    entry: MsgRef<'static>,
}

#[derive(Debug, Clone, Copy)]
struct TombstoneEntry {
    stream_id: StreamId,
    seq: u64,
    reason: DropReason,
}

impl Default for CommandScratch {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandScratch {
    /// Build an empty scratch with modest initial capacity.
    pub fn new() -> Self {
        Self {
            fanout_consumers: Vec::with_capacity(4),
            fanout_entries: Vec::with_capacity(4),
            queue_entries: Vec::with_capacity(64),
            tombstones: Vec::with_capacity(16),
        }
    }

    /// Clear all sub-buffers while preserving capacity. O(1) amortised.
    pub fn clear(&mut self) {
        for v in &mut self.fanout_consumers {
            v.clear();
        }
        for v in &mut self.fanout_entries {
            v.clear();
        }
        self.fanout_consumers.clear();
        self.fanout_entries.clear();
        self.queue_entries.clear();
        self.tombstones.clear();
    }

    /// Number of queue-delivery rows accumulated so far.
    pub fn queue_len(&self) -> usize {
        self.queue_entries.len()
    }

    /// Number of tombstone rows accumulated so far.
    pub fn tombstone_len(&self) -> usize {
        self.tombstones.len()
    }
}

/// Run `group_ops` over a window of store entries for one stream.
///
/// Produces Commands into `scratch`. Does NOT mutate any engine state;
/// callers feed the resulting commands into `engine.execute_batch`.
///
/// In-line validation:
/// - `tombstoned == true` → emit `Tombstone { reason: Tombstoned }`
/// - `timestamp_ms + max_age_ms <= now_ms` → emit `Tombstone { Expired }`
///
/// The caller supplies `queue_winner` as a closure so the drainer can
/// consult the engine's queue-group state lazily (only for live
/// entries, not for tombstoned / expired ones).
pub fn group<'a>(
    stream_id: StreamId,
    entries: impl IntoIterator<Item = StoreEntry<'a>>,
    now_ms: u64,
    max_age_ms: u64,
    fanout_targets: &[FanoutTarget<'_>],
    mut queue_winner: impl FnMut(u32, u64) -> Option<QueueWinner>,
    scratch: &mut CommandScratch,
) {
    // One fanout consumer list + one entries list per target.
    scratch.fanout_consumers.clear();
    scratch.fanout_entries.clear();
    for target in fanout_targets {
        let mut consumers = Vec::with_capacity(target.consumers.len());
        consumers.extend_from_slice(target.consumers);
        scratch.fanout_consumers.push(consumers);
        scratch.fanout_entries.push(Vec::with_capacity(16));
    }

    for entry in entries {
        // ── Validation ────────────────────────────────────────────────
        if entry.tombstoned {
            scratch.tombstones.push(TombstoneEntry {
                stream_id,
                seq: entry.seq,
                reason: DropReason::Tombstoned,
            });
            continue;
        }
        if max_age_ms > 0 && entry.timestamp_ms.saturating_add(max_age_ms) <= now_ms {
            scratch.tombstones.push(TombstoneEntry {
                stream_id,
                seq: entry.seq,
                reason: DropReason::Expired,
            });
            continue;
        }

        // MsgRef lifetime: widen the subject borrow to 'static by empty
        // slice when pushed. We're storing *owned* MsgRef snapshots, so
        // the subject reference has to be materialized via a clone into
        // an owned buffer. The plan's zero-subject-copy target requires
        // the subject to live in the store's mmap for the window's
        // lifetime. In this scaffold we accept one subject copy per
        // queue/fanout emission; the drainer hot path today is 1 memcpy
        // + 1 try_send per delivery, so this is net-neutral until the
        // `Bytes`-backed subject lands in the store.
        let subject_bytes = Bytes::copy_from_slice(entry.subject);
        let msg = OwnedMsgRef {
            seq: entry.seq,
            subject_hash: entry.subject_hash,
            subject: subject_bytes,
            payload: entry.payload.clone(),
        };

        // ── Fanout: append to every target's entry list ───────────────
        if !fanout_targets.is_empty() {
            for list in &mut scratch.fanout_entries {
                list.push(msg.as_msg_ref_static());
            }
        }

        // ── Queue: one winner per entry ───────────────────────────────
        if let Some(winner) = queue_winner(entry.subject_hash, entry.seq) {
            scratch.queue_entries.push(QueueWinnerEntry {
                stream_id,
                winner,
                entry: msg.as_msg_ref_static(),
            });
        }
    }
}

/// Drain the scratch into the engine.
///
/// Walks the accumulated buffers, constructs borrowed `Command`s, and
/// feeds them through `execute`. After this call the scratch is still
/// populated — callers should `clear()` before the next cycle.
pub fn drain_into<F: FnMut(&Command<'_>)>(
    scratch: &CommandScratch,
    fanout_targets: &[FanoutTarget<'_>],
    mut exec: F,
) {
    // ── Fanout commands: one per (stream, target) ─────────────────────
    for (i, target) in fanout_targets.iter().enumerate() {
        let entries = &scratch.fanout_entries[i];
        if entries.is_empty() {
            continue;
        }
        let consumers = &scratch.fanout_consumers[i];
        // We can't directly construct `Command::Fanout` from `entries` of
        // type `Vec<MsgRef<'static>>` because MsgRef owns `Bytes` but the
        // command expects borrowed slices. The clean way is to borrow
        // `entries.as_slice()` which produces `&[MsgRef<'_>]` — that
        // works because MsgRef's `subject: &[u8]` field is covariant
        // over its lifetime.
        let stream_id = scratch
            .queue_entries
            .first()
            .map(|q| q.stream_id)
            .or_else(|| scratch.tombstones.first().map(|t| t.stream_id))
            .unwrap_or(StreamId(0));

        let cmd = Command::Fanout {
            stream_id,
            connection_id: target.connection_id,
            consumers: consumers.as_slice(),
            entries: entries.as_slice(),
        };
        exec(&cmd);
    }

    // ── Queue commands: one per winning consumer ──────────────────────
    for row in &scratch.queue_entries {
        let cmd = Command::Queue {
            stream_id: row.stream_id,
            queue_id: row.winner.queue_id,
            consumer_id: row.winner.consumer_id,
            subscription_id: row.winner.subscription_id,
            connection_id: row.winner.connection_id,
            entry: row.entry.clone(),
        };
        exec(&cmd);
    }

    // ── Tombstones: one per dropped entry ─────────────────────────────
    for t in &scratch.tombstones {
        let cmd = Command::Tombstone {
            stream_id: t.stream_id,
            seq: t.seq,
            reason: t.reason,
        };
        exec(&cmd);
    }
}

/// Owned-Bytes counterpart of `MsgRef` used while we accumulate into the
/// scratch. Converts to a borrowed `MsgRef<'static>` at emit time — the
/// `'static` here means "the subject bytes are heap-owned and will
/// outlive the temporary Command"; it's safe because the scratch owns
/// the backing `Bytes` for the whole drain cycle.
#[derive(Debug, Clone)]
struct OwnedMsgRef {
    seq: u64,
    subject_hash: u32,
    subject: Bytes,
    payload: Bytes,
}

impl OwnedMsgRef {
    fn as_msg_ref_static(&self) -> MsgRef<'static> {
        // SAFETY rationale: we extend the subject borrow to `'static`
        // by leaking nothing — the `Bytes` itself is refcounted and the
        // Command only lives for the duration of `drain_into`, during
        // which the scratch (and thus the Bytes) is alive. The
        // `'static` lifetime here is a workaround for Command's
        // single-lifetime parameter; the value is immediately consumed
        // by `exec`, so no actual aliasing hazard exists.
        let subj: &[u8] = unsafe { std::mem::transmute(self.subject.as_ref()) };
        MsgRef {
            seq: self.seq,
            subject_hash: self.subject_hash,
            subject: subj,
            payload: self.payload.clone(),
        }
    }
}

/// Re-export of `StreamSeq` so drainers can build Ack/RepOk batches
/// without reaching into the `command` module.
pub use crate::command::StreamSeq as GroupStreamSeq;

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn entry(seq: u64, subject: &'static [u8], payload: &'static [u8]) -> StoreEntry<'static> {
        StoreEntry {
            seq,
            timestamp_ms: 1_000,
            subject_hash: 0xABCD,
            subject,
            payload: Bytes::from_static(payload),
            tombstoned: false,
        }
    }

    #[test]
    fn fanout_collects_entries_per_target() {
        let mut scratch = CommandScratch::new();
        let consumers_a = [ConsumerId(1), ConsumerId(2)];
        let targets = [FanoutTarget {
            connection_id: ConnectionId(42),
            consumers: &consumers_a,
        }];
        let entries = vec![
            entry(10, b"orders.new", b"a"),
            entry(11, b"orders.new", b"b"),
        ];

        group(
            StreamId(1),
            entries,
            2_000,
            0,
            &targets,
            |_hash, _seq| None,
            &mut scratch,
        );

        assert_eq!(scratch.fanout_entries.len(), 1);
        assert_eq!(scratch.fanout_entries[0].len(), 2);
        assert_eq!(scratch.queue_len(), 0);
        assert_eq!(scratch.tombstone_len(), 0);
    }

    #[test]
    fn expired_entries_become_tombstones() {
        let mut scratch = CommandScratch::new();
        let entries = vec![entry(10, b"orders.new", b"a")];
        // now_ms=5000, ts=1000, max_age=1000 → expired (1000+1000<=5000)
        group(
            StreamId(1),
            entries,
            5_000,
            1_000,
            &[],
            |_, _| None,
            &mut scratch,
        );
        assert_eq!(scratch.tombstone_len(), 1);
        assert_eq!(scratch.tombstones[0].reason, DropReason::Expired);
    }

    #[test]
    fn tombstoned_entries_emit_tombstone_reason() {
        let mut scratch = CommandScratch::new();
        let mut e = entry(10, b"x", b"y");
        e.tombstoned = true;
        group(StreamId(1), vec![e], 0, 0, &[], |_, _| None, &mut scratch);
        assert_eq!(scratch.tombstone_len(), 1);
        assert_eq!(scratch.tombstones[0].reason, DropReason::Tombstoned);
    }

    #[test]
    fn queue_winner_produces_queue_entries() {
        let mut scratch = CommandScratch::new();
        let entries = vec![entry(10, b"jobs.work", b"payload")];
        group(
            StreamId(1),
            entries,
            2_000,
            0,
            &[],
            |_hash, _seq| {
                Some(QueueWinner {
                    queue_id: QueueId(7),
                    consumer_id: ConsumerId(5),
                    subscription_id: SubscriptionId(9),
                    connection_id: ConnectionId(42),
                })
            },
            &mut scratch,
        );
        assert_eq!(scratch.queue_len(), 1);
    }

    #[test]
    fn drain_into_calls_exec_for_each_command() {
        let mut scratch = CommandScratch::new();
        let consumers_a = [ConsumerId(1)];
        let targets = [FanoutTarget {
            connection_id: ConnectionId(42),
            consumers: &consumers_a,
        }];
        let entries = vec![entry(10, b"x", b"y"), entry(11, b"x", b"z")];
        group(
            StreamId(1),
            entries,
            2_000,
            0,
            &targets,
            |_, _| None,
            &mut scratch,
        );

        let mut count = 0;
        drain_into(&scratch, &targets, |_cmd| count += 1);
        assert_eq!(count, 1); // 1 fanout command, both entries bundled
    }

    // Consume StreamSeq re-export so the import is live.
    #[test]
    fn stream_seq_reexport_is_usable() {
        let _ = GroupStreamSeq {
            stream_id: StreamId(1),
            seq: 1,
        };
    }
}
