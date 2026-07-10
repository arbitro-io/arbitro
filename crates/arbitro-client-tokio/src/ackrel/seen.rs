//! Bounded FIFO dedup of `(consumer_id, seq)` pairs — redelivery guard
//! for at-least-once ack semantics. A hit means the message already ran
//! the user callback once; the caller re-acks without invoking it again.

use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;

/// Default cap: ~1M entries × 12 B ≈ 12 MiB, comfortably bounded.
const DEFAULT_CAP: usize = 1_000_000;

struct SeenInner {
    set: HashSet<(u32, u64)>,
    fifo: VecDeque<(u32, u64)>,
}

pub(crate) struct SeenCache {
    cap: usize,
    inner: Mutex<SeenInner>,
}

impl SeenCache {
    pub fn new() -> Self {
        Self::with_cap(DEFAULT_CAP)
    }

    pub fn with_cap(cap: usize) -> Self {
        Self {
            cap,
            inner: Mutex::new(SeenInner {
                set: HashSet::new(),
                fifo: VecDeque::new(),
            }),
        }
    }

    /// Returns `true` if `(cid, seq)` was newly inserted (miss). Returns
    /// `false` if already present (hit — a redelivery).
    pub fn insert_check(&self, cid: u32, seq: u64) -> bool {
        let mut g = self.inner.lock().unwrap();
        let key = (cid, seq);
        if !g.set.insert(key) {
            return false;
        }
        g.fifo.push_back(key);
        while g.fifo.len() > self.cap {
            if let Some(old) = g.fifo.pop_front() {
                g.set.remove(&old);
            }
        }
        true
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().set.len()
    }
}
