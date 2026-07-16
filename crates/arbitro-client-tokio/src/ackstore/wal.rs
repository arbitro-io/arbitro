//! Persistent, append-only [`Store`] backed by a write-ahead log. The
//! in-memory state (per-slot bounds + live set) is the runtime source of
//! truth; the log is read only at open (replay). Records are length-framed and
//! CRC-guarded (see [`super::format`]). Replaces the old SQLite cold tier with
//! a zero-dependency WAL whose on-disk format matches the sibling clients.
//!
//! Durability: a record is durable once [`Store::sync`] (with `fsync`) returns.
//! Between record and sync a crash loses that record and the message is
//! reprocessed after restart — the at-least-once guarantee the broker already
//! provides. Dedup is a best-effort optimization on top; it is never wrong
//! (the exact set is always consulted), only occasionally missed after an
//! un-synced crash.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::format::*;
use super::store::{Metrics, SlotInfo, SlotRef, Store, StoreError};

const MAX_NAME_LEN: usize = 65535;

pub(crate) type Clock = Arc<dyn Fn() -> i64 + Send + Sync>;

fn system_now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// WAL configuration. Sensible defaults via [`WalConfig::new`].
#[derive(Clone)]
pub struct WalConfig {
    pub dir: PathBuf,
    /// Per-entry expiry; `None`/zero disables the TTL sweep.
    pub ttl: Option<Duration>,
    /// TTL sweep cadence; default 5s.
    pub sweep_interval: Duration,
    /// fsync on sync()/close().
    pub fsync: bool,
    /// Live seqs per slot before FIFO eviction; default 1_000_000.
    pub max_cap: usize,
    /// Records between auto-snapshots; 0 disables.
    pub snapshot_every_n: usize,
    /// File size that triggers auto-compaction on sync; 0 disables.
    pub compact_at_bytes: i64,
    pub(crate) now: Clock,
}

impl WalConfig {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            ttl: None,
            sweep_interval: Duration::from_secs(5),
            fsync: false,
            max_cap: 1_000_000,
            snapshot_every_n: 0,
            compact_at_bytes: 0,
            now: Arc::new(system_now_ms),
        }
    }
}

#[derive(Default)]
pub(crate) struct Counters {
    pub recorded: AtomicU64,
    pub confirmed: AtomicU64,
    pub expired: AtomicU64,
    pub registered: AtomicU64,
    pub tombstoned: AtomicU64,
    pub sync_errors: AtomicU64,
    pub last_sync_ms: AtomicU64,
    pub last_snap_ms: AtomicU64,
    pub recs_since_snap: AtomicU64,
}

pub(crate) struct Writer {
    pub file: BufWriter<File>,
    pub size: i64,
    pub closed: bool,
    pub scratch: Vec<u8>,
}

/// Shared state referenced by both the [`Wal`] and every [`WalSlot`]. Holds NO
/// slot references, so `Arc<WalCore>` never forms a cycle with the slots.
pub(crate) struct WalCore {
    pub cfg: WalConfig,
    pub writer: Mutex<Writer>,
    pub counters: Counters,
}

pub(crate) struct SymTable {
    pub by_name: HashMap<String, Arc<WalSlot>>,
    pub by_id: Vec<Option<Arc<WalSlot>>>,
    pub next_id: u32,
}

pub struct Wal {
    pub(crate) core: Arc<WalCore>,
    pub(crate) sym: Arc<RwLock<SymTable>>,
    stop: Arc<AtomicBool>,
    sweep: Mutex<Option<JoinHandle<()>>>,
}

pub(crate) struct WalSlot {
    pub slot_id: u32,
    pub stream: String,
    pub consumer: String,
    pub registered_at: SystemTime,
    pub core: Arc<WalCore>,
    pub min_seq: AtomicU64,
    pub max_seq: AtomicU64,
    pub inner: Mutex<SlotInner>,
}

#[derive(Default)]
pub(crate) struct SlotInner {
    pub set: HashMap<u64, i64>, // seq → ts_ms
    pub fifo: Vec<u64>,
    pub oldest_ts: i64,
    pub tombed: bool,
}

pub(crate) fn slot_key(stream: &str, consumer: &str) -> String {
    format!("{stream}\u{0}{consumer}")
}

impl Wal {
    /// Open (creating if needed) a WAL at `cfg.dir` and replay it.
    pub fn open(cfg: WalConfig) -> Result<Arc<Self>, StoreError> {
        std::fs::create_dir_all(&cfg.dir)?;
        let path = cfg.dir.join("ackstore.log");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        let core = Arc::new(WalCore {
            cfg: cfg.clone(),
            writer: Mutex::new(Writer {
                file: BufWriter::new(file),
                size: 0,
                closed: false,
                scratch: Vec::with_capacity(4096),
            }),
            counters: Counters::default(),
        });
        let sym = Arc::new(RwLock::new(SymTable {
            by_name: HashMap::new(),
            by_id: Vec::new(),
            next_id: 0,
        }));

        let wal = Arc::new(Wal {
            core: core.clone(),
            sym: sym.clone(),
            stop: Arc::new(AtomicBool::new(false)),
            sweep: Mutex::new(None),
        });

        wal.replay()?;
        wal.ensure_header()?;

        // TTL sweeper.
        if cfg.ttl.map(|t| !t.is_zero()).unwrap_or(false) {
            let stop = wal.stop.clone();
            let core2 = core.clone();
            let sym2 = sym.clone();
            let interval = cfg.sweep_interval;
            let handle = std::thread::spawn(move || {
                sweep_loop(stop, core2, sym2, interval);
            });
            *wal.sweep.lock().unwrap() = Some(handle);
        }
        Ok(wal)
    }

    fn ensure_header(&self) -> Result<(), StoreError> {
        let mut w = self.core.writer.lock().unwrap();
        // BufWriter wraps File; get the underlying length via the inner file.
        w.file.flush()?;
        let len = w.file.get_ref().metadata()?.len() as i64;
        if len == 0 {
            w.file.get_mut().seek(SeekFrom::Start(0))?;
            w.file.write_all(&FILE_MAGIC)?;
            w.file.flush()?;
            w.size = FILE_MAGIC.len() as i64;
        } else {
            w.size = len;
        }
        w.file.get_mut().seek(SeekFrom::End(0))?;
        Ok(())
    }

    fn get_slot(&self, cid_name: &str) -> Option<Arc<WalSlot>> {
        self.sym.read().unwrap().by_name.get(cid_name).cloned()
    }
}

// ── append helpers on WalCore (called while holding a slot lock) ────────────

impl WalCore {
    pub(crate) fn append_seq_op(
        &self,
        op: u8,
        slot_id: u32,
        ts_ms: i64,
        seq: u64,
    ) -> Result<(), StoreError> {
        let mut w = self.writer.lock().unwrap();
        if w.closed {
            return Err(StoreError::Closed);
        }
        let need = record_frame_size();
        if w.scratch.len() < need {
            w.scratch.resize(need, 0);
        }
        // Split-borrow so we can write the scratch slice directly — no per-ack
        // heap allocation on the hot path.
        let Writer {
            file, scratch, size, ..
        } = &mut *w;
        let n = encode_seq_op(&mut scratch[..need], op, slot_id, ts_ms as u64, seq);
        file.write_all(&scratch[..n])?;
        *size += n as i64;
        Ok(())
    }

    fn append_tombstone(&self, slot_id: u32, ts_ms: i64) -> Result<(), StoreError> {
        let mut w = self.writer.lock().unwrap();
        if w.closed {
            return Err(StoreError::Closed);
        }
        let need = tombstone_frame_size();
        if w.scratch.len() < need {
            w.scratch.resize(need, 0);
        }
        let Writer {
            file, scratch, size, ..
        } = &mut *w;
        let n = encode_tombstone(&mut scratch[..need], slot_id, ts_ms as u64);
        file.write_all(&scratch[..n])?;
        *size += n as i64;
        Ok(())
    }

    fn append_frame(&self, frame: &[u8]) -> Result<(), StoreError> {
        let mut w = self.writer.lock().unwrap();
        if w.closed {
            return Err(StoreError::Closed);
        }
        w.file.write_all(frame)?;
        w.size += frame.len() as i64;
        Ok(())
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        // If the Wal is dropped without an explicit `close()`, signal the TTL
        // sweep thread to stop so it can't outlive the store (it holds an
        // `Arc<WalCore>` + `Arc<RwLock<SymTable>>`, which would otherwise leak
        // a thread + memory per instance). We don't join here to keep `drop`
        // non-blocking; the thread observes the flag on its next tick and
        // exits. `close()` remains the deterministic stop+join+fsync path.
        self.stop.store(true, Ordering::Relaxed);
    }
}

// ── Store impl ─────────────────────────────────────────────────────────────

impl Store for Wal {
    fn slot(&self, stream: &str, consumer: &str) -> Result<Arc<dyn SlotRef>, StoreError> {
        if stream.len() > MAX_NAME_LEN || consumer.len() > MAX_NAME_LEN {
            return Err(StoreError::NameTooLong);
        }
        let key = slot_key(stream, consumer);
        if let Some(s) = self.get_slot(&key) {
            return Ok(s as Arc<dyn SlotRef>);
        }
        let (slot, id) = {
            let mut sym = self.sym.write().unwrap();
            if let Some(s) = sym.by_name.get(&key) {
                return Ok(s.clone() as Arc<dyn SlotRef>);
            }
            if sym.next_id == u32::MAX {
                return Err(StoreError::SlotOverflow);
            }
            let id = sym.next_id;
            sym.next_id += 1;
            let slot = Arc::new(WalSlot {
                slot_id: id,
                stream: stream.to_string(),
                consumer: consumer.to_string(),
                registered_at: SystemTime::now(),
                core: self.core.clone(),
                min_seq: AtomicU64::new(0),
                max_seq: AtomicU64::new(0),
                inner: Mutex::new(SlotInner::default()),
            });
            sym.by_name.insert(key, slot.clone());
            while sym.by_id.len() <= id as usize {
                sym.by_id.push(None);
            }
            sym.by_id[id as usize] = Some(slot.clone());
            (slot, id)
        };

        // Persist the Register record.
        let now = (self.core.cfg.now)();
        let mut buf = vec![0u8; register_frame_size(stream.len(), consumer.len())];
        let n = encode_register(&mut buf, id, now as u64, stream.as_bytes(), consumer.as_bytes());
        self.core.append_frame(&buf[..n])?;
        self.core.counters.registered.fetch_add(1, Ordering::Relaxed);
        Ok(slot as Arc<dyn SlotRef>)
    }

    fn sync(&self) -> Result<(), StoreError> {
        {
            let mut w = self.core.writer.lock().unwrap();
            if w.closed {
                return Err(StoreError::Closed);
            }
            if let Err(e) = w.file.flush() {
                self.core.counters.sync_errors.fetch_add(1, Ordering::Relaxed);
                return Err(e.into());
            }
            if self.core.cfg.fsync {
                if let Err(e) = w.file.get_ref().sync_all() {
                    self.core.counters.sync_errors.fetch_add(1, Ordering::Relaxed);
                    return Err(e.into());
                }
            }
        }
        self.core
            .counters
            .last_sync_ms
            .store((self.core.cfg.now)() as u64, Ordering::Relaxed);

        // Auto-compaction supersedes auto-snapshot when both would fire.
        if self.core.cfg.compact_at_bytes > 0 {
            let size = self.core.writer.lock().unwrap().size;
            if size >= self.core.cfg.compact_at_bytes {
                return self.compact();
            }
        }
        if self.core.cfg.snapshot_every_n > 0
            && self.core.counters.recs_since_snap.load(Ordering::Relaxed) as usize
                >= self.core.cfg.snapshot_every_n
        {
            self.snapshot()?;
        }
        Ok(())
    }

    fn restore(&self) -> Result<(), StoreError> {
        Ok(()) // replay happened at open
    }

    fn close(&self) -> Result<(), StoreError> {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.sweep.lock().unwrap().take() {
            let _ = h.join();
        }
        let mut w = self.core.writer.lock().unwrap();
        if w.closed {
            return Ok(());
        }
        w.file.flush()?;
        if self.core.cfg.fsync {
            w.file.get_ref().sync_all()?;
        }
        w.closed = true;
        Ok(())
    }

    fn list_slots(&self) -> Vec<SlotInfo> {
        let slots: Vec<Arc<WalSlot>> =
            self.sym.read().unwrap().by_name.values().cloned().collect();
        let mut out: Vec<SlotInfo> = slots.iter().map(|s| s.info()).collect();
        out.sort_by_key(|i| i.slot_id);
        out
    }

    fn slot_info(&self, stream: &str, consumer: &str) -> Option<SlotInfo> {
        self.get_slot(&slot_key(stream, consumer)).map(|s| s.info())
    }

    fn delete_slot(&self, stream: &str, consumer: &str) -> Result<(), StoreError> {
        let key = slot_key(stream, consumer);
        let slot = {
            let mut sym = self.sym.write().unwrap();
            let s = sym.by_name.remove(&key);
            if let Some(ref s) = s {
                let id = s.slot_id as usize;
                if id < sym.by_id.len() {
                    sym.by_id[id] = None;
                }
            }
            s
        };
        let slot = slot.ok_or(StoreError::UnknownSlot)?;
        {
            let mut g = slot.inner.lock().unwrap();
            g.tombed = true;
            g.set.clear();
            g.fifo.clear();
            g.oldest_ts = 0;
            slot.min_seq.store(0, Ordering::Relaxed);
            slot.max_seq.store(0, Ordering::Relaxed);
            let now = (self.core.cfg.now)();
            self.core.append_tombstone(slot.slot_id, now)?;
        }
        self.core.counters.tombstoned.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn metrics(&self) -> Metrics {
        let slots: Vec<Arc<WalSlot>> =
            self.sym.read().unwrap().by_name.values().cloned().collect();
        let mut live = 0u64;
        for s in &slots {
            live += s.inner.lock().unwrap().set.len() as u64;
        }
        let size = self.core.writer.lock().unwrap().size;
        let c = &self.core.counters;
        Metrics {
            slots: slots.len(),
            live_entries: live,
            recorded: c.recorded.load(Ordering::Relaxed),
            confirmed: c.confirmed.load(Ordering::Relaxed),
            expired: c.expired.load(Ordering::Relaxed),
            registered: c.registered.load(Ordering::Relaxed),
            tombstoned: c.tombstoned.load(Ordering::Relaxed),
            sync_errors: c.sync_errors.load(Ordering::Relaxed),
            file_size: size,
            last_sync_ms: c.last_sync_ms.load(Ordering::Relaxed) as i64,
            last_snapshot_ms: c.last_snap_ms.load(Ordering::Relaxed) as i64,
        }
    }
}

// ── SlotRef impl ───────────────────────────────────────────────────────────

impl SlotRef for WalSlot {
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
        let now = (self.core.cfg.now)();
        g.set.insert(seq, now);
        g.fifo.push(seq);
        if g.oldest_ts == 0 {
            g.oldest_ts = now;
        }
        let cap = self.core.cfg.max_cap;
        while g.fifo.len() > cap {
            let old = g.fifo.remove(0);
            g.set.remove(&old);
        }
        if seq > self.max_seq.load(Ordering::Relaxed) {
            self.max_seq.store(seq, Ordering::Relaxed);
        }
        // `min_seq` must be the true minimum live seq, NOT the FIFO front:
        // async handlers ack out of order (e.g. seq 101 before 100), so the
        // insertion-ordered `fifo.first()` is not the smallest seq. A `min_seq`
        // above the true minimum makes `fresh()` skip the exact-set check for a
        // recorded seq (`seq < min_seq`), returning a false "not seen" — a dedup
        // miss. Only ever lower it here; the removal paths (`confirm_up_to`) call
        // `compact_fifo`, which recomputes the true min from the set.
        let cur_min = self.min_seq.load(Ordering::Relaxed);
        if cur_min == 0 || seq < cur_min {
            self.min_seq.store(seq, Ordering::Relaxed);
        }
        // Append while holding slot lock → disk order == memory order.
        self.core.append_seq_op(OP_RECORD, self.slot_id, now, seq)?;
        self.core.counters.recorded.fetch_add(1, Ordering::Relaxed);
        self.core
            .counters
            .recs_since_snap
            .fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    fn confirm(&self, seq: u64) -> Result<(), StoreError> {
        let mut g = self.inner.lock().unwrap();
        if g.set.remove(&seq).is_some() && g.set.is_empty() {
            g.oldest_ts = 0;
        }
        let now = (self.core.cfg.now)();
        self.core.append_seq_op(OP_CONFIRM, self.slot_id, now, seq)?;
        self.core.counters.confirmed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn confirm_up_to(&self, cursor: u64) -> Result<usize, StoreError> {
        let mut g = self.inner.lock().unwrap();
        let before = g.set.len();
        g.set.retain(|&seq, _| seq > cursor);
        let removed = before - g.set.len();
        // No-op cursor: nothing live is `<= cursor`. Since seqs are monotonic
        // and the broker cursor only advances, no future record will be
        // `<= cursor` either, so the CONFIRM_UP_TO frame would be dead weight.
        // Skipping the append matters: the reader hot path calls this on EVERY
        // `AckBatchResp`, most of which remove nothing — appending each time
        // would take the writer lock per frame and grow the log unbounded.
        if removed == 0 {
            return Ok(0);
        }
        recompute_oldest(&mut g);
        compact_fifo(&mut g, &self.min_seq);
        let now = (self.core.cfg.now)();
        self.core
            .append_seq_op(OP_CONFIRM_UP_TO, self.slot_id, now, cursor)?;
        self.core
            .counters
            .confirmed
            .fetch_add(removed as u64, Ordering::Relaxed);
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

pub(crate) fn recompute_oldest(g: &mut SlotInner) {
    if g.set.is_empty() {
        g.oldest_ts = 0;
        return;
    }
    let mut oldest = 0i64;
    for &ts in g.set.values() {
        if oldest == 0 || ts < oldest {
            oldest = ts;
        }
    }
    g.oldest_ts = oldest;
}

pub(crate) fn compact_fifo(g: &mut SlotInner, min_seq: &AtomicU64) {
    if g.fifo.is_empty() {
        return;
    }
    let mut kept = Vec::with_capacity(g.set.len());
    let mut min = 0u64;
    for &seq in &g.fifo {
        if g.set.contains_key(&seq) {
            kept.push(seq);
            if min == 0 || seq < min {
                min = seq;
            }
        }
    }
    g.fifo = kept;
    min_seq.store(min, Ordering::Relaxed); // maxSeq stays monotonic
}

// ── TTL sweep ──────────────────────────────────────────────────────────────

fn sweep_loop(
    stop: Arc<AtomicBool>,
    core: Arc<WalCore>,
    sym: Arc<RwLock<SymTable>>,
    interval: Duration,
) {
    let ttl = match core.cfg.ttl {
        Some(t) if !t.is_zero() => t,
        _ => return,
    };
    loop {
        // Sleep in small chunks so close() is responsive.
        let mut slept = Duration::ZERO;
        while slept < interval {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            std::thread::sleep(Duration::from_millis(50));
            slept += Duration::from_millis(50);
        }
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let cutoff = (core.cfg.now)() - ttl.as_millis() as i64;
        let slots: Vec<Arc<WalSlot>> = sym.read().unwrap().by_name.values().cloned().collect();
        for slot in slots {
            let mut g = slot.inner.lock().unwrap();
            if g.set.is_empty() {
                continue;
            }
            let expired: Vec<u64> = g
                .set
                .iter()
                .filter(|(_, &ts)| ts < cutoff)
                .map(|(&seq, _)| seq)
                .collect();
            for &seq in &expired {
                g.set.remove(&seq);
                let now = (core.cfg.now)();
                let _ = core.append_seq_op(OP_EXPIRE, slot.slot_id, now, seq);
            }
            if !expired.is_empty() {
                core.counters
                    .expired
                    .fetch_add(expired.len() as u64, Ordering::Relaxed);
                recompute_oldest(&mut g);
                compact_fifo(&mut g, &slot.min_seq);
            }
        }
    }
}

// ── snapshot ───────────────────────────────────────────────────────────────

impl Wal {
    /// Write one Snapshot record per non-empty slot (its live set). Speeds
    /// Restore and is auto-triggered by [`Store::sync`] after
    /// `snapshot_every_n` records.
    pub fn snapshot(&self) -> Result<(), StoreError> {
        let slots: Vec<Arc<WalSlot>> =
            self.sym.read().unwrap().by_name.values().cloned().collect();
        let now = (self.core.cfg.now)() as u64;
        for slot in slots {
            let g = slot.inner.lock().unwrap();
            if g.set.is_empty() {
                continue;
            }
            let mut seqs: Vec<u64> = g.set.keys().copied().collect();
            seqs.sort_unstable();
            let mut buf = vec![0u8; snapshot_frame_size(seqs.len())];
            let n = encode_snapshot(
                &mut buf,
                slot.slot_id,
                now,
                seqs[0],
                seqs[seqs.len() - 1],
                &seqs,
            );
            drop(g);
            self.core.append_frame(&buf[..n])?;
        }
        self.core.counters.recs_since_snap.store(0, Ordering::Relaxed);
        self.core.counters.last_snap_ms.store(now, Ordering::Relaxed);
        Ok(())
    }

    // ── replay ────────────────────────────────────────────────────────────

    /// Rebuild in-memory state from the log. Robust against torn writes /
    /// corruption: reads frames until the tail cannot supply a full, CRC-valid
    /// frame, then TRUNCATES at the last good record boundary. Applies the TTL
    /// filter (an old Record is not resurrected). Called once from `open`.
    fn replay(&self) -> Result<(), StoreError> {
        let mut w = self.core.writer.lock().unwrap();
        w.file.flush()?;
        let f = w.file.get_mut();
        f.seek(SeekFrom::Start(0))?;
        let size = f.metadata()?.len() as i64;
        if size == 0 {
            return Ok(());
        }
        let mut data = Vec::with_capacity(size as usize);
        f.read_to_end(&mut data)?;

        let ttl_cutoff = self
            .core
            .cfg
            .ttl
            .filter(|t| !t.is_zero())
            .map(|t| (self.core.cfg.now)() - t.as_millis() as i64)
            .unwrap_or(0);

        let (slots, names, good_offset) = scan_bytes(&data, ttl_cutoff)?;

        // Materialize into the sym table.
        let mut sym = self.sym.write().unwrap();
        // `next_id` must clear EVERY id ever seen in the log — including any
        // orphaned RECORD-without-Register id we skip below — so a fresh slot
        // never reuses an id and cross-contaminates leftover records.
        let mut max_id = 0u32;
        for &id in slots.keys().chain(names.keys()) {
            if id >= max_id {
                max_id = id + 1;
            }
        }
        let mut ids: Vec<u32> = slots.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            let live = &slots[&id];
            let (stream, consumer) = match names.get(&id) {
                Some(n) => n.clone(),
                None => continue,
            };
            let mut seqs: Vec<u64> = live.keys().copied().collect();
            seqs.sort_unstable();
            let mut set = HashMap::with_capacity(seqs.len());
            let mut fifo = Vec::with_capacity(seqs.len());
            let mut oldest = 0i64;
            for &seq in &seqs {
                let ts = live[&seq];
                set.insert(seq, ts);
                fifo.push(seq);
                if oldest == 0 || ts < oldest {
                    oldest = ts;
                }
            }
            let slot = Arc::new(WalSlot {
                slot_id: id,
                stream: stream.clone(),
                consumer: consumer.clone(),
                registered_at: SystemTime::now(),
                core: self.core.clone(),
                min_seq: AtomicU64::new(seqs.first().copied().unwrap_or(0)),
                max_seq: AtomicU64::new(seqs.last().copied().unwrap_or(0)),
                inner: Mutex::new(SlotInner {
                    set,
                    fifo,
                    oldest_ts: oldest,
                    tombed: false,
                }),
            });
            sym.by_name.insert(slot_key(&stream, &consumer), slot.clone());
            while sym.by_id.len() <= id as usize {
                sym.by_id.push(None);
            }
            sym.by_id[id as usize] = Some(slot);
        }
        sym.next_id = max_id;
        drop(sym);

        // Truncate any torn/corrupt tail.
        if good_offset < size {
            let f = w.file.get_mut();
            f.set_len(good_offset as u64)?;
            f.seek(SeekFrom::End(0))?;
            w.size = good_offset;
        }
        Ok(())
    }
}

/// Live-set accumulator for one slot during replay/compaction.
pub(crate) type LiveMap = HashMap<u64, i64>;

/// Scan a whole file image into per-slot live sets + names. Returns the byte
/// offset just past the last fully-valid frame (for truncation). Shared by
/// replay and compaction.
pub(crate) fn scan_bytes(
    data: &[u8],
    ttl_cutoff: i64,
) -> Result<(HashMap<u32, LiveMap>, HashMap<u32, (String, String)>, i64), StoreError> {
    let mut slots: HashMap<u32, LiveMap> = HashMap::new();
    let mut names: HashMap<u32, (String, String)> = HashMap::new();

    if data.len() < FILE_MAGIC.len() || data[..FILE_MAGIC.len()] != FILE_MAGIC {
        // No/invalid magic → nothing recoverable; truncate to empty (magic
        // rewritten by ensure_header).
        return Ok((slots, names, 0));
    }
    let mut pos = FILE_MAGIC.len();
    let mut good = pos as i64;

    while pos + FRAME_LEN_SIZE <= data.len() {
        let plen = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        if plen == 0 || plen > 64 * 1024 * 1024 {
            break;
        }
        let payload_start = pos + FRAME_LEN_SIZE;
        let crc_start = payload_start + plen;
        let frame_end = crc_start + FRAME_CRC_SIZE;
        if frame_end > data.len() {
            break; // torn tail
        }
        let payload = &data[payload_start..crc_start];
        let want = u32::from_le_bytes(data[crc_start..frame_end].try_into().unwrap());
        if crc32c(payload) != want {
            break; // corrupt → stop
        }
        let d = match decode_payload(payload) {
            Some(d) => d,
            None => break,
        };
        apply_replay(&mut slots, &mut names, &d, ttl_cutoff);
        pos = frame_end;
        good = pos as i64;
    }
    Ok((slots, names, good))
}

fn apply_replay(
    slots: &mut HashMap<u32, LiveMap>,
    names: &mut HashMap<u32, (String, String)>,
    d: &Decoded,
    ttl_cutoff: i64,
) {
    match d.op {
        OP_REGISTER => {
            slots.entry(d.slot_id).or_default();
            names.insert(
                d.slot_id,
                (
                    String::from_utf8_lossy(&d.stream).into_owned(),
                    String::from_utf8_lossy(&d.consumer).into_owned(),
                ),
            );
        }
        OP_RECORD => {
            if ttl_cutoff > 0 && (d.ts_ms as i64) < ttl_cutoff {
                return;
            }
            slots.entry(d.slot_id).or_default().insert(d.seq, d.ts_ms as i64);
        }
        OP_CONFIRM | OP_EXPIRE => {
            slots.entry(d.slot_id).or_default().remove(&d.seq);
        }
        OP_CONFIRM_UP_TO => {
            let live = slots.entry(d.slot_id).or_default();
            live.retain(|&seq, _| seq > d.seq);
        }
        OP_TOMBSTONE => {
            slots.remove(&d.slot_id);
            names.remove(&d.slot_id);
        }
        OP_SNAPSHOT => {
            let live = slots.entry(d.slot_id).or_default();
            live.clear();
            if ttl_cutoff > 0 && (d.ts_ms as i64) < ttl_cutoff {
                return;
            }
            for &seq in &d.seqs {
                live.insert(seq, d.ts_ms as i64);
            }
        }
        _ => {}
    }
}
