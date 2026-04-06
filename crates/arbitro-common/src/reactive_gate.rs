//! ReactiveGate — inline dispatch signal with zero thread-wake overhead.
//!
//! Unlike `Gate` (futex), this never blocks. `signal()` calls the handler
//! directly on the calling thread if the gate is open. If locked, signal()
//! is a no-op (O(1) atomic load).
//!
//! ```text
//! OPEN:   signal() → handler(&ctrl) — inline, same thread, same stack frame
//! LOCKED: signal() → noop
//! ```
//!
//! The handler receives a `GateCtrl` reference to lock/unlock from inside.
//! Reactive: if the handler never locks, every signal() runs to completion.
//! Backpressure: handler calls `ctrl.lock()` — subsequent signal()s are dropped
//! until something calls `gate.unlock()`.
//!
//! No allocations. No threads. No futex. No tokio.

use std::sync::atomic::{AtomicBool, Ordering::*};

/// Control handle passed to the handler on each dispatch.
///
/// Provides lock/unlock/signal — the handler can re-trigger the gate
/// inline without holding an external reference to it.
pub struct GateCtrl<'a> {
    locked:  &'a AtomicBool,
    handler: &'a dyn Fn(&GateCtrl<'_>),
}

impl GateCtrl<'_> {
    /// Stop dispatching. Subsequent `signal()` calls are no-ops until `unlock()`.
    #[inline]
    pub fn lock(&self) {
        self.locked.store(true, Release);
    }

    /// Re-enable dispatching.
    #[inline]
    pub fn unlock(&self) {
        self.locked.store(false, Release);
    }

    /// Dispatch the handler again if the gate is open.
    /// Inline, same thread — zero cross-thread overhead.
    #[inline]
    pub fn signal(&self) {
        if !self.locked.load(Acquire) {
            let ctrl = GateCtrl { locked: self.locked, handler: self.handler };
            (self.handler)(&ctrl);
        }
    }
}

/// Inline-dispatch reactive gate.
///
/// The handler is stored as `Box<dyn Fn>` so that `GateCtrl` can hold a
/// trait-object reference back to it for self-re-triggering via `ctrl.signal()`.
pub struct ReactiveGate {
    locked:  AtomicBool,
    handler: Box<dyn Fn(&GateCtrl<'_>) + Send + Sync>,
}

impl std::fmt::Debug for ReactiveGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReactiveGate")
            .field("locked", &self.locked)
            .finish_non_exhaustive()
    }
}

impl ReactiveGate {
    /// Create a new gate in open state with the given handler.
    pub fn new(handler: impl Fn(&GateCtrl<'_>) + Send + Sync + 'static) -> Self {
        Self {
            locked:  AtomicBool::new(false),
            handler: Box::new(handler),
        }
    }

    /// Create a new gate in locked state.
    pub fn new_locked(handler: impl Fn(&GateCtrl<'_>) + Send + Sync + 'static) -> Self {
        Self {
            locked:  AtomicBool::new(true),
            handler: Box::new(handler),
        }
    }

    /// Dispatch the handler inline if the gate is open.
    ///
    /// Returns `true` if the handler was called, `false` if locked.
    /// O(1) atomic load on the fast (locked) path.
    #[inline]
    pub fn signal(&self) -> bool {
        if self.locked.load(Acquire) {
            return false;
        }
        let ctrl = GateCtrl { locked: &self.locked, handler: &*self.handler };
        (self.handler)(&ctrl);
        true
    }

    /// Lock from outside — subsequent `signal()` calls are no-ops.
    #[inline]
    pub fn lock(&self) {
        self.locked.store(true, Release);
    }

    /// Unlock from outside — re-enable dispatching.
    #[inline]
    pub fn unlock(&self) {
        self.locked.store(false, Release);
    }

    /// Check if the gate is currently locked.
    #[inline]
    pub fn is_locked(&self) -> bool {
        self.locked.load(Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;
    use std::sync::Arc;

    #[test]
    fn signal_dispatches_when_open() {
        let count = Arc::new(AtomicU32::new(0));
        let c = count.clone();
        let gate = ReactiveGate::new(move |_| { c.fetch_add(1, Relaxed); });
        assert!(gate.signal());
        assert!(gate.signal());
        assert_eq!(count.load(Relaxed), 2);
    }

    #[test]
    fn signal_noop_when_locked() {
        let count = Arc::new(AtomicU32::new(0));
        let c = count.clone();
        let gate = ReactiveGate::new_locked(move |_| { c.fetch_add(1, Relaxed); });
        assert!(!gate.signal());
        assert_eq!(count.load(Relaxed), 0);
    }

    #[test]
    fn handler_can_lock_gate() {
        let count = Arc::new(AtomicU32::new(0));
        let c = count.clone();
        let gate = ReactiveGate::new(move |ctrl| {
            let v = c.fetch_add(1, Relaxed);
            if v >= 2 {
                ctrl.lock();
            }
        });
        gate.signal(); // count=1, open
        gate.signal(); // count=2, open
        gate.signal(); // count=3, locks inside
        gate.signal(); // noop — locked
        gate.signal(); // noop — locked
        assert_eq!(count.load(Relaxed), 3);
    }

    #[test]
    fn unlock_re_enables_after_handler_lock() {
        let count = Arc::new(AtomicU32::new(0));
        let c = count.clone();
        let gate = ReactiveGate::new(move |ctrl| {
            c.fetch_add(1, Relaxed);
            ctrl.lock();
        });
        gate.signal(); // dispatches, then locks
        gate.signal(); // noop
        gate.unlock(); // re-enable
        gate.signal(); // dispatches again, locks
        assert_eq!(count.load(Relaxed), 2);
    }

    #[test]
    fn ctrl_signal_self_retriggers() {
        // Handler uses ctrl.signal() to keep dispatching until count reached.
        let count = Arc::new(AtomicU32::new(0));
        let c = count.clone();
        let gate = ReactiveGate::new(move |ctrl| {
            let v = c.fetch_add(1, Relaxed) + 1;
            if v < 10 {
                ctrl.signal(); // self re-trigger inline
            } else {
                ctrl.lock();   // stop at 10
            }
        });
        gate.signal(); // one kick drives all 10
        assert_eq!(count.load(Relaxed), 10);
    }
}
