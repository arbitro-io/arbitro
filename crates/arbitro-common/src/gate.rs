//! Gate — drain delivery signal for the shard task.
//!
//! The shard calls `release()` after publish/ack/nack to signal new work.
//! The drain loop checks `is_open()` to decide whether to run drain_cycle.
//! When drain_cycle finds nothing, it calls `lock()` — the drain awaits.
//!
//! Implementation: `arbitro_kit::gate::SignalSet<NotifyWaiter>` with a
//! single signal (bit 0). `NotifyWaiter` → `tokio::sync::Notify`, so the
//! drain is a cooperative tokio task instead of an OS thread parked via
//! `thread::park`. This removes the impedance mismatch with the tokio
//! runtime (no more `spawn_blocking` for `h.join()`, no more
//! `parking_lot::Mutex<Vec<JoinHandle>>`) and makes shutdown deterministic
//! under parallel tests.

use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Notify;

/// Async Gate built directly on `tokio::sync::Notify` + `AtomicBool`.
///
/// We don't go through `arbitro_kit::gate::SignalSet<NotifyWaiter>::acquire_async`
/// because its `wait_until` closure captures `&self.chunks[0]` with a
/// borrow that triggers a known compiler bug (rust-lang#100013) when the
/// future is required to be `Send` from inside a generic async context.
/// Hand-rolling the `notified() → check → await` loop sidesteps the
/// inference failure and is equivalent in semantics: coalesced release,
/// 0% CPU idle, lost-notify-safe.
///
/// Semantics:
/// - `release()` sets the flag and notifies the (single) consumer task.
///   Coalescing: many releases collapse to one wake.
/// - `acquire().await` suspends the task until the flag is set, then
///   returns. Fast-paths if already open.
/// - `lock()` clears the flag.
/// - `is_open()` reads the flag.
pub struct Gate {
    open: AtomicBool,
    notify: Notify,
}

impl Default for Gate {
    fn default() -> Self {
        Self::new()
    }
}

impl Gate {
    pub fn new() -> Self {
        Self {
            open: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    /// Signal that work is available. Coalescing — multiple concurrent
    /// releases merge into a single wake.
    #[inline]
    pub fn release(&self) {
        // `swap` returns the previous value. We only need to fire the
        // notify on the 0 → 1 transition; further releases are no-ops
        // until the consumer calls `lock()`.
        if !self.open.swap(true, Ordering::Release) {
            self.notify.notify_one();
        } else {
            // Idempotent wake — covers the race where the consumer
            // raced past a stale `acquire()` and is about to await
            // again. Without this second `notify_one`, a producer that
            // sees the flag already set would never re-arm the wake.
            self.notify.notify_one();
        }
    }

    /// Clear the "work available" signal. Called by drain when a cycle
    /// found nothing to deliver.
    #[inline]
    pub fn lock(&self) {
        self.open.store(false, Ordering::Release);
    }

    /// `true` if there is pending work.
    #[inline]
    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    /// Suspend the calling tokio task until work is available. 0% CPU —
    /// awaits `tokio::sync::Notify::notified()`.
    pub async fn acquire(&self) {
        loop {
            // BUILD the notified() future BEFORE checking the flag.
            // Without this ordering, a producer firing `notify_one`
            // between the check and the await would be lost (Notify
            // only "remembers" a permit if a `notified()` was already
            // registered when it fired).
            let notified = self.notify.notified();
            if self.open.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    /// T17 — multiple `release()` calls coalesce into a single
    /// "work available" signal. The gate's contract is that N producers
    /// firing in quick succession do NOT each wake the drain N times —
    /// they OR into one bit and the drain decides what to do on its
    /// next pass. Without this, the shard worker would do N redundant
    /// drain cycles per burst publish.
    #[test]
    fn t17_n_releases_coalesce_into_one_open_state() {
        let g = Gate::new();

        // Burst of 1000 releases — must end in exactly one "open" state.
        for _ in 0..1000 {
            g.release();
        }
        assert!(g.is_open(), "gate must be open after any release");

        // One lock() collapses the whole burst — same as if a single
        // release had happened. This is the key invariant: burst of N
        // does not require N lock() calls to clear.
        g.lock();
        assert!(!g.is_open(), "single lock() clears the coalesced state");
    }

    /// T17 follow-up — release/lock interleavings race the right way:
    /// a release that fires AFTER lock() wins (gate ends open).
    #[test]
    fn t17_release_after_lock_keeps_gate_open() {
        let g = Gate::new();

        g.release();
        g.lock();
        assert!(!g.is_open());
        g.release();
        assert!(g.is_open(), "post-lock release must reopen the gate");
    }

    /// T17 follow-up — concurrent producers cannot lose updates.
    /// Tightens the coalescing invariant: even when producers fire
    /// from N threads simultaneously, the final state is "open" and
    /// a single lock() collapses it.
    #[test]
    fn t17_concurrent_releases_never_lose_open_state() {
        let g = Arc::new(Gate::new());

        let mut handles = Vec::new();
        for _ in 0..8 {
            let g2 = Arc::clone(&g);
            handles.push(std::thread::spawn(move || {
                for _ in 0..1_000 {
                    g2.release();
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // Give the notify machinery a beat to settle on slow CI runners.
        std::thread::sleep(Duration::from_millis(5));
        assert!(
            g.is_open(),
            "8 × 1000 concurrent releases must leave gate open"
        );
    }

    /// Drain-thread analog under tokio: a task awaits `acquire()`, a
    /// non-tokio thread fires `release()`, the task wakes. This is the
    /// cross-runtime wake path the migration exists for.
    #[tokio::test(flavor = "multi_thread")]
    async fn acquire_async_wakes_on_release_from_os_thread() {
        let g = Arc::new(Gate::new());
        let g2 = Arc::clone(&g);
        let h = tokio::spawn(async move {
            g2.acquire().await;
        });
        std::thread::sleep(Duration::from_millis(10));
        g.release();
        h.await.unwrap();
        assert!(g.is_open());
    }
}
