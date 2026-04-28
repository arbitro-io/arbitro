//! Gate — drain delivery signal for the shard thread.
//!
//! The shard calls `release()` after publish/ack/nack to signal new work.
//! The shard loop checks `is_open()` to decide whether to run drain_deliver.
//! When drain_deliver finds nothing, it calls `lock()` — the shard parks.
//!
//! Two implementations behind a feature flag:
//!
//! ## Default (park/unpark)
//! AtomicBool + spin-512 + std::thread::park().
//! - release() cost when shard parked: ~7 µs (unpark syscall)
//! - release() cost when shard busy:   ~0.1 µs (relaxed atomic store)
//! - fire → acquire latency (parked):  ~13 µs
//!
//! ## `gate_crossbeam` feature
//! crossbeam_channel::bounded(1) + latched AtomicBool mirror.
//! - release() posts a unit to the channel (if not already full)
//! - acquire() recv()s from the channel
//! - Both are 0% CPU when idle (crossbeam uses futex parking internally)
//!
//! Same public API either way. Used to A/B compare signalling primitives.

// ═══════════════════════════════════════════════════════════════════════════
// Default impl: AtomicBool + park/unpark
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(not(any(feature = "gate_crossbeam", feature = "gate_tokio_notify")))]
mod imp {
    use std::cell::UnsafeCell;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[repr(align(64))]
    pub struct Gate {
        locked: AtomicBool,
        parked: AtomicBool,
        worker: UnsafeCell<Option<std::thread::Thread>>,
    }

    unsafe impl Sync for Gate {}

    impl Default for Gate {
        fn default() -> Self { Self::new() }
    }

    impl Gate {
        pub fn new() -> Self {
            Self {
                locked: AtomicBool::new(true),
                parked: AtomicBool::new(false),
                worker: UnsafeCell::new(None),
            }
        }

        pub fn set_worker(&self, t: std::thread::Thread) {
            unsafe { *self.worker.get() = Some(t); }
        }

        #[inline]
        pub fn release(&self) {
            self.locked.store(false, Ordering::Relaxed);
            if self.parked.load(Ordering::Relaxed) {
                unsafe {
                    if let Some(t) = &*self.worker.get() {
                        t.unpark();
                    }
                }
            }
        }

        #[inline]
        pub fn lock(&self) {
            self.locked.store(true, Ordering::Relaxed);
        }

        #[inline]
        pub fn is_open(&self) -> bool {
            !self.locked.load(Ordering::Relaxed)
        }

        #[inline]
        pub fn acquire(&self) {
            if !self.locked.load(Ordering::Relaxed) { return; }
            for _ in 0..512 {
                if !self.locked.load(Ordering::Relaxed) { return; }
                std::hint::spin_loop();
            }
            self.parked.store(true, Ordering::Relaxed);
            std::thread::park();
            self.parked.store(false, Ordering::Relaxed);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Alternative impl: crossbeam_channel::bounded(1) + latched flag
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(feature = "gate_crossbeam")]
mod imp {
    use crossbeam_channel::{bounded, Receiver, Sender, TryRecvError, TrySendError};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// crossbeam_channel-backed Gate.
    ///
    /// Semantics:
    /// - `open` is a latched mirror (sticky until `lock()`), matching the
    ///   default impl's `locked` semantics exactly.
    /// - The channel carries at most one pending signal; extra releases are
    ///   coalesced (try_send returns Full, we ignore — the latched bool
    ///   already records "work pending").
    /// - `acquire()` fast-paths on the latched bool; only falls through to
    ///   `rx.recv()` (blocking, 0% CPU) when no pending signal.
    #[repr(align(64))]
    pub struct Gate {
        open: AtomicBool,
        tx: Sender<()>,
        rx: Receiver<()>,
    }

    impl Default for Gate {
        fn default() -> Self { Self::new() }
    }

    impl Gate {
        pub fn new() -> Self {
            let (tx, rx) = bounded(1);
            Self {
                open: AtomicBool::new(false),
                tx,
                rx,
            }
        }

        /// No-op for API parity with the park/unpark impl (no thread handle
        /// needed — crossbeam manages its own parking).
        pub fn set_worker(&self, _t: std::thread::Thread) {}

        #[inline]
        pub fn release(&self) {
            self.open.store(true, Ordering::Relaxed);
            // Coalesce: if channel is Full, a signal is already pending —
            // the latched bool is enough for acquire() to wake.
            match self.tx.try_send(()) {
                Ok(_) | Err(TrySendError::Full(_)) => {}
                Err(TrySendError::Disconnected(_)) => {}
            }
        }

        #[inline]
        pub fn lock(&self) {
            self.open.store(false, Ordering::Relaxed);
            // Drain any stale signal so the next acquire truly blocks
            // when no release() happens after this lock().
            while let Err(TryRecvError::Empty) | Ok(_) = self.rx.try_recv() {
                if self.rx.is_empty() { break; }
            }
        }

        #[inline]
        pub fn is_open(&self) -> bool {
            self.open.load(Ordering::Relaxed)
        }

        #[inline]
        pub fn acquire(&self) {
            if self.open.load(Ordering::Relaxed) { return; }
            // recv() blocks at 0% CPU via crossbeam's internal parking.
            // We ignore the value — the signal is the fact of arrival.
            // If the channel ever disconnects (shouldn't — Gate owns both
            // ends), recv returns Err and we bail.
            let _ = self.rx.recv();
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Alternative impl: tokio::sync::Notify + latched flag
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(feature = "gate_tokio_notify")]
mod imp {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::sync::Notify;

    /// tokio::sync::Notify-backed Gate.
    ///
    /// Notify itself is runtime-agnostic (only uses wakers; no reactor/timers).
    /// We drive the `notified()` future from the sync shard thread via
    /// `futures::executor::block_on`, which internally uses a park/unpark-based
    /// thread-local executor. 0% CPU while blocked.
    ///
    /// Semantics match the default impl: `open` is a latched mirror,
    /// extra notifications are coalesced (Notify permits one wakeup).
    #[repr(align(64))]
    pub struct Gate {
        open: AtomicBool,
        notify: Arc<Notify>,
    }

    impl Default for Gate {
        fn default() -> Self { Self::new() }
    }

    impl Gate {
        pub fn new() -> Self {
            Self {
                open: AtomicBool::new(false),
                notify: Arc::new(Notify::new()),
            }
        }

        pub fn set_worker(&self, _t: std::thread::Thread) {}

        #[inline]
        pub fn release(&self) {
            self.open.store(true, Ordering::Relaxed);
            // notify_one is sync, cheap, coalescing (extra notifies while
            // a permit is already pending are dropped).
            self.notify.notify_one();
        }

        #[inline]
        pub fn lock(&self) {
            self.open.store(false, Ordering::Relaxed);
            // Drain any stale permit so the next acquire truly blocks.
            // Notify has no "reset" API, but polling notified() once and
            // dropping it won't consume the permit unless we await it.
            // Safest path: consume via try-path using a dummy executor.
            // In practice, the shard only calls lock() after draining
            // everything, and a stale permit causes one extra wake which
            // immediately returns because open is already true on next
            // release. So we tolerate the stale permit.
        }

        #[inline]
        pub fn is_open(&self) -> bool {
            self.open.load(Ordering::Relaxed)
        }

        #[inline]
        pub fn acquire(&self) {
            if self.open.load(Ordering::Relaxed) { return; }
            // block_on from the sync shard thread. futures' LocalPool uses
            // a thread-local waker that parks this OS thread — same kernel
            // mechanism as park/unpark, but routed through the Notify waker.
            futures::executor::block_on(self.notify.notified());
        }
    }
}

pub use imp::Gate;
