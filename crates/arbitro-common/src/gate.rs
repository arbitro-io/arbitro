//! Gate — futex-based signal with progressive spin + atomic wait.
//!
//! Uses `atomic_wait` crate (OS futex on Linux, WaitOnAddress on Windows).
//! Zero tokio dependency, zero allocation, sub-microsecond wake.
//!
//! ```text
//! UNLOCKED=0:  acquire() returns instantly (one CAS).
//! LOCKED=1:    acquire() spins briefly [1, 5, 50, 200] iterations,
//!              then falls into futex wait.
//!              unlock() → wake_one() interrupts the futex immediately.
//! ```
//!
//! Auto-reset: acquire() consumes the signal (swaps UNLOCKED→LOCKED).
//! Multiple unlock() calls before acquire() coalesce into one wakeup.

use std::sync::atomic::{AtomicU32, Ordering};

use atomic_wait::{wait, wake_one};

const UNLOCKED: u32 = 0;
const LOCKED: u32 = 1;

/// Progressive spin counts before falling into OS wait.
/// Total spin: ~256 iterations ≈ 1-5µs on modern hardware.
const SPIN_ROUNDS: &[u32] = &[1, 5, 50, 200];

/// Futex-based gate for drain signaling.
///
/// Faster than `tokio::sync::Notify` because:
/// - No async runtime involvement — direct OS futex syscall
/// - No task scheduling overhead — thread wakes at kernel level
/// - Spin phase catches signals within ~1-5µs without syscall
pub struct Gate {
    state: AtomicU32,
}

impl Default for Gate {
    fn default() -> Self {
        Self::new()
    }
}

impl Gate {
    /// Create a new gate in locked state (drain starts waiting).
    pub fn new() -> Self {
        Self {
            state: AtomicU32::new(LOCKED),
        }
    }

    /// Block until the gate is unlocked.
    ///
    /// If already unlocked, returns immediately and re-locks the gate
    /// (auto-reset behavior — one signal = one wakeup).
    ///
    /// If locked, spins briefly then falls into OS futex wait.
    /// `unlock()` interrupts the wait immediately via `wake_one()`.
    #[inline]
    pub fn acquire(&self) {
        // Fast path: already unlocked → swap to locked, return.
        if self.state.compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed).is_ok() {
            return;
        }

        self.acquire_slow();
    }

    #[cold]
    fn acquire_slow(&self) {
        // Phase 1: Progressive spin — catch signals within ~1-5µs without syscall.
        for &rounds in SPIN_ROUNDS {
            for _ in 0..rounds {
                if self.state.compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                    return;
                }
                std::hint::spin_loop();
            }
        }

        // Phase 2: OS futex wait — zero CPU, woken instantly by wake_one().
        loop {
            // Check before sleeping
            if self.state.compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                return;
            }

            // Futex wait: blocks if state == LOCKED.
            // Returns immediately if state != LOCKED (spurious ok).
            // wake_one() on this atomic interrupts this instantly.
            wait(&self.state, LOCKED);
        }
    }

    /// Unlock the gate — wake the waiter immediately.
    ///
    /// Non-blocking, O(1). Safe to call from any thread.
    /// If the waiter is in spin phase or futex wait, it wakes instantly.
    #[inline]
    pub fn unlock(&self) {
        self.state.store(UNLOCKED, Ordering::Release);
        wake_one(&self.state);
    }

    /// Lock the gate — next `acquire()` will block.
    #[inline]
    pub fn lock(&self) {
        self.state.store(LOCKED, Ordering::Release);
    }

    /// Check if the gate is currently unlocked (non-blocking).
    #[inline]
    pub fn is_unlocked(&self) -> bool {
        self.state.load(Ordering::Acquire) == UNLOCKED
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    #[test]
    fn unlock_before_acquire_returns_immediately() {
        let gate = Gate::new();
        gate.unlock();
        let start = Instant::now();
        gate.acquire();
        assert!(start.elapsed() < Duration::from_millis(1));
    }

    #[test]
    fn acquire_blocks_then_unlock_wakes() {
        let gate = Arc::new(Gate::new());
        let g = gate.clone();

        let handle = std::thread::spawn(move || {
            let start = Instant::now();
            g.acquire();
            start.elapsed()
        });

        std::thread::sleep(Duration::from_millis(5));
        gate.unlock();

        let elapsed = handle.join().unwrap();
        assert!(elapsed < Duration::from_millis(50), "took {:?}", elapsed);
    }

    #[test]
    fn auto_reset_behavior() {
        let gate = Arc::new(Gate::new());

        // unlock → acquire consumes the signal
        gate.unlock();
        gate.acquire();

        // gate is now locked again — next acquire should block
        let g = gate.clone();
        let handle = std::thread::spawn(move || {
            let start = Instant::now();
            g.acquire();
            start.elapsed()
        });

        std::thread::sleep(Duration::from_millis(10));
        gate.unlock();

        let elapsed = handle.join().unwrap();
        assert!(elapsed >= Duration::from_millis(5));
    }

    #[test]
    fn multiple_unlock_coalesce() {
        let gate = Gate::new();
        gate.unlock();
        gate.unlock();
        gate.unlock();

        gate.acquire(); // consumes signal
        // gate is locked again
    }

    #[test]
    fn rapid_signal_loop() {
        // Producer waits for consumer to finish each iteration
        // via a second gate (ping-pong).
        let signal = Arc::new(Gate::new());
        let ack = Arc::new(Gate::new());

        let s = signal.clone();
        let a = ack.clone();

        let count = Arc::new(AtomicU32::new(0));
        let c = count.clone();

        let handle = std::thread::spawn(move || {
            for _ in 0..1000 {
                s.acquire();
                c.fetch_add(1, Ordering::Relaxed);
                a.unlock(); // ack back to producer
            }
        });

        for _ in 0..1000 {
            signal.unlock();
            ack.acquire(); // wait for consumer to process
        }

        handle.join().unwrap();
        assert_eq!(count.load(Ordering::Relaxed), 1000);
    }

    #[test]
    fn drain_pattern_max1() {
        // Exact drain pattern: max_inflight=1, 20 jobs
        let gate = Arc::new(Gate::new());
        let inflight = Arc::new(AtomicU32::new(0));
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let max_inflight = 1u32;
        let total = 20u32;

        let g_comp = gate.clone();
        let inf_comp = inflight.clone();
        let done_comp = done.clone();

        let completer = std::thread::spawn(move || {
            while !done_comp.load(Ordering::Relaxed) {
                let current = inf_comp.load(Ordering::Acquire);
                if current > 0 {
                    inf_comp.fetch_sub(current, Ordering::Release);
                    if current >= max_inflight {
                        g_comp.unlock();
                    }
                } else {
                    std::hint::spin_loop();
                }
            }
        });

        gate.unlock();
        let mut completed = 0u32;

        loop {
            gate.acquire();

            let cur = inflight.load(Ordering::Acquire);
            if cur >= max_inflight {
                gate.lock();
                continue;
            }

            inflight.fetch_add(1, Ordering::Release);
            completed += 1;

            if completed >= total {
                done.store(true, Ordering::Relaxed);
                gate.unlock();
                completer.join().unwrap();
                return;
            }

            if cur + 1 < max_inflight {
                gate.unlock();
            } else {
                gate.lock();
            }
        }
    }

    #[test]
    fn wake_latency_under_1ms() {
        let gate = Arc::new(Gate::new());
        let g = gate.clone();

        let latency = Arc::new(std::sync::Mutex::new(Vec::new()));
        let l = latency.clone();

        let handle = std::thread::spawn(move || {
            for _ in 0..100 {
                let start = Instant::now();
                g.acquire();
                l.lock().unwrap().push(start.elapsed());
            }
        });

        std::thread::sleep(Duration::from_millis(1));
        for _ in 0..100 {
            gate.unlock();
            std::thread::sleep(Duration::from_micros(200));
        }

        handle.join().unwrap();
        let samples = latency.lock().unwrap();
        let median = {
            let mut sorted: Vec<_> = samples.iter().copied().collect();
            sorted.sort();
            sorted[sorted.len() / 2]
        };
        assert!(median < Duration::from_millis(1), "median wake: {:?}", median);
    }
}
