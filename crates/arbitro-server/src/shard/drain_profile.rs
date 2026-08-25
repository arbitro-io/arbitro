//! Where the drain cycle's time actually goes.
//!
//! Answers one question: of the wall time between "gate opened" and "frames
//! flushed", how much is the copy under the lock, how much is matching and
//! frame building, and how much is the write to TCP?
//!
//! **Per CYCLE, never per message.** A cycle carries up to `max_feed` (256)
//! entries, so six clock reads amortise to ~0.7 ns/entry. The instrumentation
//! this replaces timed each `push_entry` — four `Instant::now()` plus four
//! `fetch_max` per message, roughly 200 ns/entry against a ~234 ns/entry
//! budget. The instrument outweighed everything it measured.
//!
//! OFF by default: without the feature `Phase` is a unit struct with an empty
//! `Drop`, and every call site vanishes. Same discipline as `lifecycle_trace`.
//! Enable with `--features drain_profile`; call `report()` from a bench.

#[cfg(feature = "drain_profile")]
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

#[cfg(feature = "drain_profile")]
static CYCLES: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "drain_profile")]
static ENTRIES: AtomicU64 = AtomicU64::new(0);
/// Window copy — the only phase holding the store lock.
#[cfg(feature = "drain_profile")]
static FILL_NS: AtomicU64 = AtomicU64::new(0);
/// Matching, per-recipient checks, frame building. Lock released.
#[cfg(feature = "drain_profile")]
static DISPATCH_NS: AtomicU64 = AtomicU64::new(0);
/// Flush to TCP plus the delivery bookkeeping after it.
#[cfg(feature = "drain_profile")]
static FLUSH_NS: AtomicU64 = AtomicU64::new(0);

/// Entries handed to `process_drain_entry`. Counts RE-walks: an entry the
/// cursor could not advance past is walked again next cycle.
#[cfg(feature = "drain_profile")]
static WALKED: AtomicU64 = AtomicU64::new(0);
/// Wire entries actually appended to a frame. The useful output.
#[cfg(feature = "drain_profile")]
static EMITS: AtomicU64 = AtomicU64::new(0);

/// Loop turns that found no consumer with capacity. Does no work and
/// `cycle()` never sees it, so without this the profile cannot see a spin.
#[cfg(feature = "drain_profile")]
static NO_DEMAND: AtomicU64 = AtomicU64::new(0);
/// Loop turns with demand but nothing new in the store.
#[cfg(feature = "drain_profile")]
static UP_TO_DATE: AtomicU64 = AtomicU64::new(0);
/// Loop turns that took the 50µs backpressure sleep.
#[cfg(feature = "drain_profile")]
static STALL_SLEEPS: AtomicU64 = AtomicU64::new(0);

/// The write to the fd itself, wherever it happens. `flush` stops at the
/// channel, so without this the socket is outside everything measured.
#[cfg(feature = "drain_profile")]
static SOCKET_NS: AtomicU64 = AtomicU64::new(0);
/// Frames written straight from the drain — same shard, empty channel.
#[cfg(feature = "drain_profile")]
static SOCK_DIRECT: AtomicU64 = AtomicU64::new(0);
/// Frames written by the writer task that shares the shard's thread.
#[cfg(feature = "drain_profile")]
static SOCK_PINNED: AtomicU64 = AtomicU64::new(0);
/// Frames written by the writer task on the shared pool.
#[cfg(feature = "drain_profile")]
static SOCK_POOL: AtomicU64 = AtomicU64::new(0);

/// The direct door was refused because the write channel was not empty.
#[cfg(feature = "drain_profile")]
static NO_DIRECT_QUEUED: AtomicU64 = AtomicU64::new(0);
/// Refused because this shard does not hold the socket.
#[cfg(feature = "drain_profile")]
static NO_DIRECT_ELSEWHERE: AtomicU64 = AtomicU64::new(0);

/// One entry entered dispatch.
#[cfg(feature = "drain_profile")]
#[inline]
pub fn walked() {
    WALKED.fetch_add(1, Relaxed);
}

/// One wire entry was appended to a frame.
#[cfg(feature = "drain_profile")]
#[inline]
pub fn emit() {
    EMITS.fetch_add(1, Relaxed);
}

/// Scoped timer — folds its lifetime into one phase counter on drop.
#[cfg(feature = "drain_profile")]
pub struct Phase {
    start: std::time::Instant,
    slot: &'static AtomicU64,
}

#[cfg(feature = "drain_profile")]
impl Drop for Phase {
    #[inline]
    fn drop(&mut self) {
        self.slot
            .fetch_add(self.start.elapsed().as_nanos() as u64, Relaxed);
    }
}

#[cfg(feature = "drain_profile")]
macro_rules! phase_fn {
    ($name:ident, $slot:ident) => {
        #[inline]
        pub fn $name() -> Phase {
            Phase {
                start: std::time::Instant::now(),
                slot: &$slot,
            }
        }
    };
}

#[cfg(feature = "drain_profile")]
phase_fn!(fill, FILL_NS);
#[cfg(feature = "drain_profile")]
phase_fn!(dispatch, DISPATCH_NS);
#[cfg(feature = "drain_profile")]
phase_fn!(flush, FLUSH_NS);

#[cfg(feature = "drain_profile")]
#[inline]
pub fn cycle(entries: usize) {
    CYCLES.fetch_add(1, Relaxed);
    ENTRIES.fetch_add(entries as u64, Relaxed);
}

/// A loop turn ended in `Window::NoDemand`.
#[cfg(feature = "drain_profile")]
#[inline]
pub fn no_demand() {
    NO_DEMAND.fetch_add(1, Relaxed);
}

/// A loop turn ended in `Window::UpToDate`.
#[cfg(feature = "drain_profile")]
#[inline]
pub fn up_to_date() {
    UP_TO_DATE.fetch_add(1, Relaxed);
}

/// A loop turn took the backpressure sleep.
#[cfg(feature = "drain_profile")]
#[inline]
pub fn stall_sleep() {
    STALL_SLEEPS.fetch_add(1, Relaxed);
}

/// Why a frame could not take the direct door.
#[cfg(feature = "drain_profile")]
#[inline]
pub fn no_direct(queued: bool) {
    if queued {
        NO_DIRECT_QUEUED.fetch_add(1, Relaxed);
    } else {
        NO_DIRECT_ELSEWHERE.fetch_add(1, Relaxed);
    }
}

/// Which door a frame took to the fd.
#[cfg(feature = "drain_profile")]
#[derive(Clone, Copy)]
pub enum SocketDoor {
    Direct,
    Pinned,
    Pool,
}

/// Times the write to the fd and records which door it took.
#[cfg(feature = "drain_profile")]
pub fn socket(door: SocketDoor) -> Phase {
    match door {
        SocketDoor::Direct => &SOCK_DIRECT,
        SocketDoor::Pinned => &SOCK_PINNED,
        SocketDoor::Pool => &SOCK_POOL,
    }
    .fetch_add(1, Relaxed);
    Phase {
        start: std::time::Instant::now(),
        slot: &SOCKET_NS,
    }
}

#[cfg(feature = "drain_profile")]
pub fn report() {
    let cycles = CYCLES.load(Relaxed);
    if cycles == 0 {
        eprintln!("\n--- drain profile --- no cycles recorded");
        report_turns(0);
        return;
    }
    let entries = ENTRIES.load(Relaxed).max(1);
    let (fill, dispatch, flush) = (
        FILL_NS.load(Relaxed),
        DISPATCH_NS.load(Relaxed),
        FLUSH_NS.load(Relaxed),
    );
    let total = (fill + dispatch + flush).max(1);
    eprintln!("\n--- drain profile ---");
    eprintln!(
        "  {cycles} cycles, {entries} entries ({:.1} per cycle)",
        entries as f64 / cycles as f64
    );
    for (name, ns) in [
        ("fill", fill),
        ("dispatch", dispatch),
        ("flush", flush),
        ("total", total),
    ] {
        eprintln!(
            "  {name:<9} {:>9.1} ms  {:>8.1} ns/entry  {:>5.1}%",
            ns as f64 / 1e6,
            ns as f64 / entries as f64,
            ns as f64 * 100.0 / total as f64,
        );
    }
    eprintln!("  `fill` holds the store lock; the other two do not.");

    let (walked, emits) = (WALKED.load(Relaxed), EMITS.load(Relaxed));
    if walked > 0 {
        eprintln!(
            "  walked={walked} emits={emits}  ({:.2} wire entries per walk)",
            emits as f64 / walked as f64
        );
        eprintln!(
            "  a walk that emits nothing is pure waste: the entry was matched, \
             checked and dropped."
        );
    }
    report_turns(cycles);
}

/// Loop turns that produced no cycle. A large count here against a small
/// `cycles` is a spin: the loop ran and found nothing it could do.
#[cfg(feature = "drain_profile")]
fn report_turns(cycles: u64) {
    let (nd, utd, sleeps) = (
        NO_DEMAND.load(Relaxed),
        UP_TO_DATE.load(Relaxed),
        STALL_SLEEPS.load(Relaxed),
    );
    let turns = cycles + nd + utd;
    eprintln!(
        "  turns={turns}  fed={cycles}  no_demand={nd}  up_to_date={utd}  \
         stall_sleeps={sleeps}"
    );
    let (d, p, q) = (
        SOCK_DIRECT.load(Relaxed),
        SOCK_PINNED.load(Relaxed),
        SOCK_POOL.load(Relaxed),
    );
    let frames = d + p + q;
    if frames > 0 {
        eprintln!(
            "  socket    {:>9.1} ms  {:>8.1} ns/frame   direct={d} pinned={p} pool={q}",
            SOCKET_NS.load(Relaxed) as f64 / 1e6,
            SOCKET_NS.load(Relaxed) as f64 / frames as f64,
        );
        eprintln!("  `socket` is the write to the fd; `flush` stops at the channel.");
    }
    let (q, e) = (
        NO_DIRECT_QUEUED.load(Relaxed),
        NO_DIRECT_ELSEWHERE.load(Relaxed),
    );
    if q + e > 0 {
        eprintln!("  direct refused: channel_not_empty={q}  socket_elsewhere={e}");
    }
    if turns > 0 {
        eprintln!(
            "  {:.1}% of turns delivered nothing.",
            (nd + utd) as f64 * 100.0 / turns as f64
        );
    }
}

// ── Feature off: every one of these compiles away ────────────────────────

#[cfg(not(feature = "drain_profile"))]
pub struct Phase;

#[cfg(not(feature = "drain_profile"))]
#[inline(always)]
pub fn fill() -> Phase {
    Phase
}
#[cfg(not(feature = "drain_profile"))]
#[inline(always)]
pub fn dispatch() -> Phase {
    Phase
}
#[cfg(not(feature = "drain_profile"))]
#[inline(always)]
pub fn flush() -> Phase {
    Phase
}
#[cfg(not(feature = "drain_profile"))]
#[inline(always)]
pub fn cycle(_entries: usize) {}
#[cfg(not(feature = "drain_profile"))]
#[derive(Clone, Copy)]
pub enum SocketDoor {
    Direct,
    Pinned,
    Pool,
}
#[cfg(not(feature = "drain_profile"))]
#[inline(always)]
pub fn socket(_door: SocketDoor) -> Phase {
    Phase
}
#[cfg(not(feature = "drain_profile"))]
#[inline(always)]
pub fn no_direct(_queued: bool) {}
#[cfg(not(feature = "drain_profile"))]
#[inline(always)]
pub fn no_demand() {}
#[cfg(not(feature = "drain_profile"))]
#[inline(always)]
pub fn up_to_date() {}
#[cfg(not(feature = "drain_profile"))]
#[inline(always)]
pub fn stall_sleep() {}
#[cfg(not(feature = "drain_profile"))]
#[inline(always)]
pub fn walked() {}
#[cfg(not(feature = "drain_profile"))]
#[inline(always)]
pub fn emit() {}
#[cfg(not(feature = "drain_profile"))]
#[inline(always)]
pub fn report() {}
