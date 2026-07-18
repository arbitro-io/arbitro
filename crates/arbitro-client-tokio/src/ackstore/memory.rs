//! In-memory-only [`Store`] — no persistence, no recovery. The correct choice
//! when restart-durability isn't required (the broker redelivers unacked
//! messages on reconnect anyway) and the fast baseline for benchmarking the
//! WAL. Same `(stream, consumer, seq)` identity + bounds-gate hot path.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::store::{Metrics, SlotInfo, SlotRef, Store, StoreError};

const MAX_NAME_LEN: usize = 65535;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[derive(Default)]
struct Counters {
    recorded: AtomicU64,
    confirmed: AtomicU64,
    registered: AtomicU64,
    tombstoned: AtomicU64,
}

pub struct MemoryStore {
    cap: usize,
    by_name: RwLock<HashMap<String, Arc<MemSlot>>>,
    next_id: AtomicU32,
    counters: Arc<Counters>,
}

impl std::fmt::Debug for MemoryStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryStore")
            .field("cap", &self.cap)
            .finish_non_exhaustive()
    }
}

struct MemSlot {
    slot_id: u32,
    stream: String,
    consumer: String,
    registered_at: SystemTime,
    cap: usize,
    counters: Arc<Counters>,
    min_seq: AtomicU64,
    max_seq: AtomicU64,
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    set: HashMap<u64, i64>, // seq → ts_ms
    fifo: Vec<u64>,
    oldest_ts: i64,
    tombed: bool,
}

impl MemoryStore {
    /// `cap` bounds each slot's live set (FIFO eviction); `0` uses 1_000_000.
    pub fn new(cap: usize) -> Self {
        Self {
            cap: if cap == 0 { 1_000_000 } else { cap },
            by_name: RwLock::new(HashMap::new()),
            next_id: AtomicU32::new(0),
            counters: Arc::new(Counters::default()),
        }
    }
}

fn slot_key(stream: &str, consumer: &str) -> String {
    format!("{stream}\u{0}{consumer}")
}

impl Store for MemoryStore {
    fn slot(&self, stream: &str, consumer: &str) -> Result<Arc<dyn SlotRef>, StoreError> {
        if stream.len() > MAX_NAME_LEN || consumer.len() > MAX_NAME_LEN {
            return Err(StoreError::NameTooLong);
        }
        let key = slot_key(stream, consumer);
        if let Some(s) = self.by_name.read().unwrap().get(&key) {
            return Ok(s.clone() as Arc<dyn SlotRef>);
        }
        let mut w = self.by_name.write().unwrap();
        if let Some(s) = w.get(&key) {
            return Ok(s.clone() as Arc<dyn SlotRef>);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if id == u32::MAX {
            return Err(StoreError::SlotOverflow);
        }
        let slot = Arc::new(MemSlot {
            slot_id: id,
            stream: stream.to_string(),
            consumer: consumer.to_string(),
            registered_at: SystemTime::now(),
            cap: self.cap,
            counters: self.counters.clone(),
            min_seq: AtomicU64::new(0),
            max_seq: AtomicU64::new(0),
            inner: Mutex::new(Inner::default()),
        });
        w.insert(key, slot.clone());
        self.counters.registered.fetch_add(1, Ordering::Relaxed);
        Ok(slot as Arc<dyn SlotRef>)
    }

    fn sync(&self) -> Result<(), StoreError> {
        Ok(())
    }
    fn restore(&self) -> Result<(), StoreError> {
        Ok(())
    }
    fn close(&self) -> Result<(), StoreError> {
        Ok(())
    }

    fn list_slots(&self) -> Vec<SlotInfo> {
        let mut out: Vec<SlotInfo> = self
            .by_name
            .read()
            .unwrap()
            .values()
            .map(|s| s.info())
            .collect();
        out.sort_by_key(|i| i.slot_id);
        out
    }

    fn slot_info(&self, stream: &str, consumer: &str) -> Option<SlotInfo> {
        self.by_name
            .read()
            .unwrap()
            .get(&slot_key(stream, consumer))
            .map(|s| s.info())
    }

    fn delete_slot(&self, stream: &str, consumer: &str) -> Result<(), StoreError> {
        let key = slot_key(stream, consumer);
        let slot = self.by_name.write().unwrap().remove(&key);
        match slot {
            None => Err(StoreError::UnknownSlot),
            Some(s) => {
                let mut g = s.inner.lock().unwrap();
                g.tombed = true;
                g.set.clear();
                g.fifo.clear();
                g.oldest_ts = 0;
                s.min_seq.store(0, Ordering::Relaxed);
                s.max_seq.store(0, Ordering::Relaxed);
                self.counters.tombstoned.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
    }

    fn metrics(&self) -> Metrics {
        let by = self.by_name.read().unwrap();
        let mut live = 0u64;
        for s in by.values() {
            live += s.inner.lock().unwrap().set.len() as u64;
        }
        Metrics {
            slots: by.len(),
            live_entries: live,
            recorded: self.counters.recorded.load(Ordering::Relaxed),
            confirmed: self.counters.confirmed.load(Ordering::Relaxed),
            registered: self.counters.registered.load(Ordering::Relaxed),
            tombstoned: self.counters.tombstoned.load(Ordering::Relaxed),
            ..Default::default()
        }
    }
}

impl SlotRef for MemSlot {
    fn fresh(&self, seq: u64) -> bool {
        let max = self.max_seq.load(Ordering::Relaxed);
        if max == 0 || seq > max {
            return true;
        }
        seq < self.min_seq.load(Ordering::Relaxed)
    }

    fn seen(&self, seq: u64) -> bool {
        if self.fresh(seq) {
            return false;
        }
        self.inner.lock().unwrap().set.contains_key(&seq)
    }

    fn record(&self, seq: u64) -> Result<(), StoreError> {
        self.check_record(seq).map(|_| ())
    }

    fn check_record(&self, seq: u64) -> Result<bool, StoreError> {
        let mut g = self.inner.lock().unwrap();
        if g.tombed {
            return Ok(true);
        }
        if g.set.contains_key(&seq) {
            return Ok(false);
        }
        let now = now_ms();
        g.set.insert(seq, now);
        g.fifo.push(seq);
        if g.oldest_ts == 0 {
            g.oldest_ts = now;
        }
        while g.fifo.len() > self.cap {
            let old = g.fifo.remove(0);
            g.set.remove(&old);
        }
        if seq > self.max_seq.load(Ordering::Relaxed) {
            self.max_seq.store(seq, Ordering::Relaxed);
        }
        // True minimum, not the FIFO front — async acks arrive out of order, so
        // `fifo.first()` (insertion order) can exceed the smallest live seq and
        // make `fresh()` skip the exact-set check for a recorded seq. Only lower
        // it here; removal paths recompute via `compact_fifo`.
        let cur_min = self.min_seq.load(Ordering::Relaxed);
        if cur_min == 0 || seq < cur_min {
            self.min_seq.store(seq, Ordering::Relaxed);
        }
        self.counters.recorded.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    fn confirm(&self, seq: u64) -> Result<(), StoreError> {
        let mut g = self.inner.lock().unwrap();
        if g.set.remove(&seq).is_some() {
            if g.set.is_empty() {
                g.oldest_ts = 0;
            }
            self.counters.confirmed.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn confirm_up_to(&self, cursor: u64) -> Result<usize, StoreError> {
        let mut g = self.inner.lock().unwrap();
        let before = g.set.len();
        g.set.retain(|&seq, _| seq > cursor);
        let removed = before - g.set.len();
        if removed > 0 {
            let mut min = 0u64;
            for &seq in g.set.keys() {
                if min == 0 || seq < min {
                    min = seq;
                }
            }
            self.min_seq.store(min, Ordering::Relaxed);
            if g.set.is_empty() {
                g.oldest_ts = 0;
            }
            self.counters
                .confirmed
                .fetch_add(removed as u64, Ordering::Relaxed);
        }
        Ok(removed)
    }

    fn info(&self) -> SlotInfo {
        let g = self.inner.lock().unwrap();
        let (mut min, mut max) = (0u64, 0u64);
        for &seq in g.set.keys() {
            if min == 0 || seq < min {
                min = seq;
            }
            if seq > max {
                max = seq;
            }
        }
        SlotInfo {
            slot_id: self.slot_id,
            stream: self.stream.clone(),
            consumer: self.consumer.clone(),
            live: g.set.len(),
            min_seq: min,
            max_seq: max,
            oldest_ts_ms: g.oldest_ts as u64,
            registered_at: self.registered_at,
        }
    }
}
