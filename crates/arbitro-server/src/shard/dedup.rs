//! Shard-local message-id deduplication.
//!
//! Single-thread ownership:
//! - no Arc
//! - no Mutex
//! - no atomics
//! - no cross-core synchronization
//!
//! The caller only knows:
//!
//! ```text
//! dedup.admit(...)
//! ```
//!
//! Scheduling and expiration belong entirely to Dedup.

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::rc::Rc;

use arbitro_common::foldhash::fast::FixedState;
use arbitro_engine_v2::types::StreamId;
use arbitro_kit::scheduler::{Ns, Scheduler};

type MsgHash = u64;
type TimestampMs = u64;
type Token = u64;

/// Scheduler resolution. Dedup windows are configured in whole seconds, so
/// retiring within 100 ms of the deadline is far finer than anything the
/// contract promises.
pub(crate) const TICK_MS: u64 = 100;

/// Returns the key it was handed.
///
/// `seen` is keyed by a hash already. std's default is SipHash-1-3, which
/// would run a keyed permutation over those 8 bytes on every publish to
/// re-randomize something already uniform.
#[derive(Default)]
struct Identity(u64);

impl Hasher for Identity {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }

    fn write(&mut self, _: &[u8]) {
        unreachable!("seen is keyed by MsgHash");
    }
}

/// Keyed by a hash: no rehash.
type SeenMap = HashMap<MsgHash, Seen, BuildHasherDefault<Identity>>;

/// Keyed by a dense u32: foldhash, not SipHash.
type StreamMap = HashMap<StreamId, StreamDedup, FixedState>;

#[derive(Clone, Copy)]
struct Expiry {
    stream: StreamId,
    hash: MsgHash,
    token: Token,
}

#[derive(Clone, Copy)]
struct Seen {
    expires_at: TimestampMs,

    /// Unique admission identity.
    ///
    /// Protects against stale scheduler events after:
    /// - logical expiration + readmission
    /// - stream deletion + recreation
    /// - clear() + later reuse
    token: Token,
}

#[derive(Default)]
struct StreamDedup {
    seen: SeenMap,
}

struct Tables {
    streams: StreamMap,

    /// Number of tracked msg ids across all streams.
    live: usize,

    /// Never reset by clear/remove_stream.
    ///
    /// That makes already queued scheduler events unable to accidentally
    /// match a future admission.
    next_token: Token,
}

impl Default for Tables {
    fn default() -> Self {
        Self {
            streams: StreamMap::default(),
            live: 0,
            next_token: 1,
        }
    }
}

impl Tables {
    #[inline(always)]
    fn alloc_token(next: &mut Token) -> Token {
        let token = *next;

        let mut n = token.wrapping_add(1);

        // Reserve 0.
        if n == 0 {
            n = 1;
        }

        *next = n;
        token
    }

    /// Expire exactly the admission represented by `token`.
    ///
    /// A stale event is therefore only a couple of lookups + comparisons.
    #[inline]
    fn expire(&mut self, stream: StreamId, hash: MsgHash, token: Token) {
        // Split fields explicitly so Rust knows these borrows are disjoint.
        let Tables { streams, live, .. } = self;

        let Entry::Occupied(mut stream_entry) = streams.entry(stream) else {
            // Stream deleted.
            return;
        };

        let remove_stream = {
            let table = stream_entry.get_mut();

            let Entry::Occupied(entry) = table.seen.entry(hash) else {
                // Already expired/removed.
                return;
            };

            if entry.get().token != token {
                // Old timer for a newer admission.
                return;
            }

            entry.remove();

            debug_assert!(*live > 0);
            *live -= 1;

            table.seen.is_empty()
        };

        // Do not keep empty per-stream maps around.
        if remove_stream {
            stream_entry.remove();
        }
    }
}

/// Deduplication state owned by exactly one shard/core.
///
/// Scheduler ownership is intentionally hidden here:
/// callers admit ids; Dedup handles their lifetime.
pub(crate) struct Dedup {
    tables: Rc<RefCell<Tables>>,
    scheduler: Rc<RefCell<Scheduler>>,
    lane: Ns<Expiry>,
}

impl Dedup {
    /// Create the dedup domain and install its expiration listener.
    ///
    /// There is deliberately no separate `install()`.
    /// A constructed Dedup is always ready to use.
    pub(crate) fn new(scheduler: Rc<RefCell<Scheduler>>) -> Self {
        let lane = scheduler.borrow_mut().namespace("dedup");

        let tables = Rc::new(RefCell::new(Tables::default()));

        // Listener only needs the tables, not Dedup itself.
        {
            let tables = Rc::clone(&tables);

            scheduler.borrow_mut().listen(lane, move |event: &Expiry| {
                tables
                    .borrow_mut()
                    .expire(event.stream, event.hash, event.token);
            });
        }

        Self {
            tables,
            scheduler,
            lane,
        }
    }

    /// Admit one msg_id.
    ///
    /// true  = admitted
    /// false = duplicate inside the active window
    ///
    /// Fast path:
    ///
    /// ```text
    /// hash
    ///   ↓
    /// StreamId lookup
    ///   ↓
    /// hash lookup
    ///   ↓
    /// vacant / active / expired
    /// ```
    ///
    /// No contains()+get().
    /// No second hash-table lookup.
    /// No allocation for intermediate structures.
    #[inline]
    pub(crate) fn admit(
        &self,
        stream: StreamId,
        msg_id: &[u8],
        now_ms: TimestampMs,
        window_ms: u32,
    ) -> bool {
        // Nothing to dedup by: no window, or a message carrying no id.
        // Two empty ids are not each other's duplicate.
        if window_ms == 0 || msg_id.is_empty() {
            return true;
        }

        self.advance(now_ms);

        let hash = crate::transport::dispatch_v2::idempotency_hash(msg_id);

        let expires_at = now_ms.saturating_add(window_ms as TimestampMs);

        let token;

        {
            let mut tables = self.tables.borrow_mut();

            let Tables {
                streams,
                live,
                next_token,
            } = &mut *tables;

            let table = streams.entry(stream).or_default();

            match table.seen.entry(hash) {
                Entry::Vacant(entry) => {
                    token = Tables::alloc_token(next_token);

                    entry.insert(Seen { expires_at, token });

                    *live += 1;
                }

                Entry::Occupied(mut entry) => {
                    let seen = entry.get();

                    if seen.expires_at > now_ms {
                        // Duplicate.
                        //
                        // Do NOT renew the expiration:
                        // repeated publishes must not extend the window.
                        return false;
                    }

                    // Logically expired but its scheduler event may not
                    // have fired yet.
                    //
                    // Reuse the same HashMap slot. No remove+insert.
                    token = Tables::alloc_token(next_token);

                    *entry.get_mut() = Seen { expires_at, token };

                    // `live` does not change: same occupied slot.
                }
            }
        }

        // The tables borrow is deliberately gone before entering Scheduler.
        //
        // If Scheduler ever invokes listeners synchronously, there can be
        // no RefCell re-entrant borrow here.
        let queued = self.scheduler.borrow_mut().queue(
            self.lane,
            Expiry {
                stream,
                hash,
                token,
            },
            expires_at,
        );

        // `queue` stores nothing when the deadline is already behind the
        // scheduler's clock, and that clock is not the `now_ms` handed in
        // here. Dropping the result would admit an id whose expiration was
        // never scheduled: it would live forever, with no error to see.
        if let Some(expiry) = queued.elapsed() {
            self.tables
                .borrow_mut()
                .expire(expiry.stream, expiry.hash, expiry.token);
        }

        true
    }

    /// Move the scheduler to `now_ms`, retiring whatever ran out.
    ///
    /// Driven from admission rather than from the shard timer on purpose:
    /// the timer reads an `Instant` from the worker's own epoch, while a
    /// publish carries `SharedClock`'s UNIX ms. Two clocks on one wheel is
    /// how deadlines end up either always past or never reached, so the
    /// wheel is advanced by the same clock that sets the deadlines.
    ///
    /// A shard that stops publishing therefore stops retiring. That costs
    /// memory bounded by one window and nothing else — nothing reads an
    /// entry whose stream is idle, and the next publish catches up in full.
    ///
    /// No borrow of `tables` is held here: the listener takes it.
    #[inline]
    fn advance(&self, now_ms: TimestampMs) {
        self.scheduler.borrow_mut().tick(now_ms);
    }

    /// Admit a whole batch, or none of it.
    ///
    /// The wire gives a batch one `first_seq`, so its sequence numbers are
    /// contiguous and "I took 7 of your 10" cannot be expressed. The first
    /// duplicate therefore undoes what this same call inserted, restoring
    /// renewed slots to their previous value rather than deleting them.
    ///
    /// Nothing is queued until the whole batch is committed, so a rejected
    /// batch leaves no timer behind either.
    pub(crate) fn admit_all<'a, I>(
        &self,
        stream: StreamId,
        ids: I,
        now_ms: TimestampMs,
        window_ms: u32,
    ) -> bool
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        if window_ms == 0 {
            return true;
        }

        self.advance(now_ms);

        let expires_at = now_ms.saturating_add(window_ms as TimestampMs);
        let mut staged: smallvec::SmallVec<[(MsgHash, Token); 16]> = smallvec::SmallVec::new();

        {
            let mut tables = self.tables.borrow_mut();

            let Tables {
                streams,
                live,
                next_token,
            } = &mut *tables;

            let table = streams.entry(stream).or_default();

            // Previous value per staged id, so a renewal can be put back
            // exactly as it was instead of being dropped.
            let mut undo: smallvec::SmallVec<[(MsgHash, Option<Seen>); 16]> =
                smallvec::SmallVec::new();
            let mut duplicate = false;

            for msg_id in ids {
                let hash = crate::transport::dispatch_v2::idempotency_hash(msg_id);

                match table.seen.entry(hash) {
                    Entry::Vacant(entry) => {
                        let token = Tables::alloc_token(next_token);
                        entry.insert(Seen { expires_at, token });
                        *live += 1;
                        undo.push((hash, None));
                        staged.push((hash, token));
                    }

                    Entry::Occupied(mut entry) => {
                        let previous = *entry.get();

                        // Also catches an id repeated INSIDE this batch: the
                        // earlier copy was just written with this same
                        // `expires_at`.
                        if previous.expires_at > now_ms {
                            duplicate = true;
                            break;
                        }

                        let token = Tables::alloc_token(next_token);
                        *entry.get_mut() = Seen { expires_at, token };
                        undo.push((hash, Some(previous)));
                        staged.push((hash, token));
                    }
                }
            }

            if duplicate {
                for (hash, previous) in undo.into_iter().rev() {
                    match previous {
                        Some(seen) => {
                            table.seen.insert(hash, seen);
                        }
                        None => {
                            if table.seen.remove(&hash).is_some() {
                                *live -= 1;
                            }
                        }
                    }
                }
                return false;
            }
        }

        // Committed. Queue every staged id, then settle any whose deadline
        // the scheduler considers already past.
        let mut elapsed: smallvec::SmallVec<[Expiry; 4]> = smallvec::SmallVec::new();
        {
            let mut scheduler = self.scheduler.borrow_mut();
            for (hash, token) in staged {
                let queued = scheduler.queue(
                    self.lane,
                    Expiry {
                        stream,
                        hash,
                        token,
                    },
                    expires_at,
                );
                if let Some(e) = queued.elapsed() {
                    elapsed.push(e);
                }
            }
        }

        if !elapsed.is_empty() {
            let mut tables = self.tables.borrow_mut();
            for e in elapsed {
                tables.expire(e.stream, e.hash, e.token);
            }
        }

        true
    }

    /// Remove ALL dedup state belonging to one stream.
    ///
    /// No global scan.
    /// No scheduler scan.
    ///
    /// Existing scheduler events become harmless because their token can
    /// never match a future admission.
    #[inline]
    pub(crate) fn remove_stream(&self, stream: StreamId) -> bool {
        let mut tables = self.tables.borrow_mut();

        let Some(removed) = tables.streams.remove(&stream) else {
            return false;
        };

        let count = removed.seen.len();

        debug_assert!(tables.live >= count);
        tables.live -= count;

        true
    }

    /// Number of currently tracked IDs.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.tables.borrow().live
    }

    /// Fast check for whether Dedup currently tracks anything.
    #[inline]
    pub(crate) fn idle(&self) -> bool {
        self.tables.borrow().live == 0
    }

    /// Whether this stream currently owns dedup state.
    ///
    /// Do not call this before admit(); admit() already does the stream
    /// lookup itself.
    #[inline]
    pub(crate) fn contains_stream(&self, stream: StreamId) -> bool {
        self.tables.borrow().streams.contains_key(&stream)
    }

    /// Clear all dedup state.
    ///
    /// `next_token` intentionally survives so timers already inside the
    /// scheduler cannot collide with future admissions.
    #[inline]
    pub(crate) fn clear(&self) {
        let mut tables = self.tables.borrow_mut();

        tables.streams.clear();
        tables.live = 0;
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.tables.borrow().streams.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(n: u32) -> StreamId {
        StreamId(n)
    }

    fn setup() -> (Rc<RefCell<Scheduler>>, Dedup) {
        let sched = Rc::new(RefCell::new(Scheduler::new(1, 0)));
        let dedup = Dedup::new(Rc::clone(&sched));
        (sched, dedup)
    }

    fn tick(sched: &Rc<RefCell<Scheduler>>, now_ms: u64) {
        sched.borrow_mut().tick(now_ms);
    }

    #[test]
    fn repeat_inside_window_is_rejected() {
        let (_sched, d) = setup();
        assert!(d.admit(s(1), b"id-1", 0, 1000));
        assert!(!d.admit(s(1), b"id-1", 500, 1000));
    }

    #[test]
    fn the_listener_retires_the_entry_when_the_window_ends() {
        // Nobody polls: the scheduler fires and the listener does the work.
        //
        // The wheel's contract is never-early, within-one-tick-late -- an
        // intervening tick at 999 pushes this to 1001 -- so the assertion is
        // "not before the deadline, gone just after it", not an exact ms.
        let (sched, d) = setup();
        d.admit(s(1), b"a", 0, 1000);
        assert_eq!(d.len(), 1);
        tick(&sched, 999);
        assert_eq!(d.len(), 1, "must never fire early");
        tick(&sched, 1001);
        assert_eq!(d.len(), 0);
        assert!(d.admit(s(1), b"a", 1001, 1000), "free again");
    }

    #[test]
    fn duplicate_does_not_extend_window() {
        let (sched, d) = setup();
        assert!(d.admit(s(1), b"a", 0, 1000));
        assert!(!d.admit(s(1), b"a", 900, 1000));
        tick(&sched, 1000);
        assert_eq!(d.len(), 0, "must expire on the ORIGINAL schedule");
    }

    #[test]
    fn expiration_removes_empty_outer_stream() {
        let (sched, d) = setup();
        assert!(d.admit(s(1), b"a", 0, 1000));
        assert!(d.contains_stream(s(1)));
        tick(&sched, 1000);
        assert!(!d.contains_stream(s(1)));
        assert!(d.is_empty());
    }

    #[test]
    fn logically_expired_id_can_be_readmitted_before_old_event_runs() {
        let (sched, d) = setup();
        assert!(d.admit(s(1), b"a", 0, 1000));
        assert!(d.admit(s(1), b"a", 2000, 1000));
        assert_eq!(d.len(), 1, "same slot, not a second entry");

        tick(&sched, 2500);
        assert_eq!(d.len(), 1, "old token must be ignored");
        tick(&sched, 3000);
        assert_eq!(d.len(), 0);
    }

    #[test]
    fn same_id_is_independent_between_streams() {
        let (_sched, d) = setup();
        assert!(d.admit(s(1), b"same", 0, 1000));
        assert!(d.admit(s(2), b"same", 0, 1000));
        assert!(!d.admit(s(1), b"same", 0, 1000));
        assert!(!d.admit(s(2), b"same", 0, 1000));
        assert_eq!(d.len(), 2);
    }

    #[test]
    fn window_zero_creates_no_state() {
        let (_sched, d) = setup();
        assert!(d.admit(s(1), b"a", 0, 0));
        assert!(d.admit(s(1), b"a", 0, 0));
        assert!(d.idle());
        assert!(d.is_empty());
    }

    #[test]
    fn removing_stream_removes_all_its_ids() {
        let (_sched, d) = setup();
        assert!(d.admit(s(1), b"a", 0, 1000));
        assert!(d.admit(s(1), b"b", 0, 1000));
        assert!(d.admit(s(2), b"c", 0, 1000));
        assert_eq!(d.len(), 3);

        assert!(d.remove_stream(s(1)));
        assert_eq!(d.len(), 1);
        assert!(!d.remove_stream(s(1)), "twice must not underflow");
        assert_eq!(d.len(), 1);
    }

    #[test]
    fn late_event_for_removed_stream_is_ignored() {
        let (sched, d) = setup();
        assert!(d.admit(s(1), b"a", 0, 1000));
        assert!(d.remove_stream(s(1)));
        tick(&sched, 1000);
        assert_eq!(d.len(), 0);
    }

    #[test]
    fn old_event_cannot_delete_recreated_stream_entry() {
        let (sched, d) = setup();

        assert!(d.admit(s(1), b"a", 0, 1000));
        assert!(d.remove_stream(s(1)));

        // Same StreamId, same hash and deliberately the same expires_at, so
        // only the token can tell the two admissions apart.
        assert!(d.admit(s(1), b"a", 200, 800));
        assert_eq!(d.len(), 1);

        tick(&sched, 1000);
        assert_eq!(d.len(), 0);
    }

    #[test]
    fn an_already_elapsed_deadline_does_not_leak_the_entry() {
        // The scheduler's clock is not the `now_ms` a publish carries. When
        // it is ahead, `queue` stores nothing -- and an entry with no timer
        // would live forever.
        let (sched, d) = setup();
        tick(&sched, 10_000); // scheduler is now well ahead
        assert!(d.admit(s(1), b"a", 0, 1000)); // deadline 1000, already past
        assert_eq!(d.len(), 0, "nothing may be left untracked");
        assert!(d.is_empty());
        assert!(d.admit(s(1), b"a", 0, 1000), "and it is admittable again");
    }

    #[test]
    fn a_batch_is_all_or_nothing() {
        let (_sched, d) = setup();
        let ids: [&[u8]; 2] = [b"a", b"b"];
        assert!(d.admit_all(s(1), ids, 0, 1000));
        assert_eq!(d.len(), 2);

        // Third entry repeats: nothing from this call may survive.
        let ids: [&[u8]; 3] = [b"c", b"d", b"a"];
        assert!(!d.admit_all(s(1), ids, 0, 1000));
        assert_eq!(d.len(), 2, "c and d must not survive a rejected batch");

        // And the retry behaves exactly like a first attempt.
        let ids: [&[u8]; 2] = [b"c", b"d"];
        assert!(d.admit_all(s(1), ids, 0, 1000));
        assert_eq!(d.len(), 4);
    }

    #[test]
    fn a_rejected_batch_restores_a_renewed_slot_instead_of_dropping_it() {
        let (sched, d) = setup();
        assert!(d.admit(s(1), b"a", 0, 1000)); // expires at 1000

        // "a" is logically expired at 5000, so the batch renews it, then
        // hits a duplicate. The renewal must be put BACK, not deleted.
        assert!(d.admit(s(1), b"live", 5000, 1000));
        let ids: [&[u8]; 2] = [b"a", b"live"];
        assert!(!d.admit_all(s(1), ids, 5000, 1000));
        assert_eq!(d.len(), 2, "both entries still tracked");

        // The restored "a" carries its ORIGINAL deadline, so its original
        // timer still matches and retires it.
        tick(&sched, 1001);
        assert_eq!(d.len(), 1, "the restored slot kept its old token");
    }

    #[test]
    fn a_batch_repeating_an_id_inside_itself_is_rejected() {
        let (_sched, d) = setup();
        let ids: [&[u8]; 3] = [b"x", b"y", b"x"];
        assert!(!d.admit_all(s(1), ids, 0, 1000));
        assert_eq!(d.len(), 0);
    }

    #[test]
    fn a_rejected_batch_leaves_no_timer_behind() {
        let (sched, d) = setup();
        let ids: [&[u8]; 2] = [b"a", b"a"];
        assert!(!d.admit_all(s(1), ids, 0, 1000));
        assert_eq!(d.len(), 0);
        assert!(d.admit(s(1), b"a", 0, 5000));
        tick(&sched, 1001);
        assert_eq!(d.len(), 1, "no stray timer from the rejected batch");
    }

    #[test]
    fn batch_window_zero_creates_no_state() {
        let (_sched, d) = setup();
        let ids: [&[u8]; 2] = [b"a", b"a"];
        assert!(d.admit_all(s(1), ids, 0, 0));
        assert!(d.idle());
    }

    #[test]
    fn clear_resets_state() {
        let (_sched, d) = setup();
        assert!(d.admit(s(1), b"a", 0, 1000));
        assert!(d.admit(s(2), b"b", 0, 1000));
        d.clear();
        assert_eq!(d.len(), 0);
        assert!(d.is_empty());
    }

    #[test]
    fn a_cleared_table_is_not_disturbed_by_its_old_timers() {
        let (sched, d) = setup();
        assert!(d.admit(s(1), b"a", 0, 1000));
        d.clear();
        assert!(d.admit(s(1), b"a", 0, 5000));
        assert_eq!(d.len(), 1);
        tick(&sched, 1000); // the pre-clear timer fires here
        assert_eq!(d.len(), 1, "next_token survives clear, so it cannot match");
        tick(&sched, 5000);
        assert_eq!(d.len(), 0);
    }
}
