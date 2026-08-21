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

#[cfg(feature = "drain_profile")]
pub fn report() {
    let cycles = CYCLES.load(Relaxed);
    if cycles == 0 {
        eprintln!("\n--- drain profile --- no cycles recorded");
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
#[inline(always)]
pub fn walked() {}
#[cfg(not(feature = "drain_profile"))]
#[inline(always)]
pub fn emit() {}
#[cfg(not(feature = "drain_profile"))]
#[inline(always)]
pub fn report() {}
