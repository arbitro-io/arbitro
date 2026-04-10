//! Generic accumulate-and-flush service.
//!
//! Accumulates entries until `max_size`, `max_bytes`, or `interval_ms` of
//! silence is reached, then calls `on_flush(&[u32])` with the accumulated
//! env_seqs. Timer resets on every new entry — fires only after silence.

use std::time::Duration;

// ── Entry ────────────────────────────────────────────────────────

pub struct FlusherEntry {
    pub env_seq: u32,
    pub bytes: usize,
}

// ── Handle ───────────────────────────────────────────────────────

pub struct Flusher {
    tx: std::sync::mpsc::SyncSender<FlusherEntry>,
}

impl Flusher {
    pub fn new() -> FlusherBuilder<()> {
        FlusherBuilder::new()
    }

    /// Push a new entry to the accumulator (non-blocking, best-effort).
    #[inline(always)]
    pub fn push(&self, env_seq: u32, bytes: usize) {
        let _ = self.tx.send(FlusherEntry { env_seq, bytes });
    }
}

// ── Builder ──────────────────────────────────────────────────────

pub struct FlusherBuilder<F> {
    pub interval_ms: u64,
    pub max_bytes: usize,
    pub max_size: usize,
    on_flush: Option<F>,
}

impl FlusherBuilder<()> {
    pub fn new() -> Self {
        Self {
            interval_ms: 2,
            max_bytes: 1024 * 1024,
            max_size: 512,
            on_flush: None,
        }
    }
}

impl<F> FlusherBuilder<F> {
    pub fn interval(mut self, ms: u64) -> Self {
        self.interval_ms = ms;
        self
    }

    pub fn max_bytes(mut self, bytes: usize) -> Self {
        self.max_bytes = bytes;
        self
    }

    pub fn max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    pub fn on_flush<F2>(self, f: F2) -> FlusherBuilder<F2>
    where
        F2: FnMut(&[u32]) + Send + 'static,
    {
        FlusherBuilder {
            interval_ms: self.interval_ms,
            max_bytes: self.max_bytes,
            max_size: self.max_size,
            on_flush: Some(f),
        }
    }
}

impl<F> FlusherBuilder<F>
where
    F: FnMut(&[u32]) + Send + 'static,
{
    pub fn spawn(self) -> Flusher {
        let (tx, rx) = std::sync::mpsc::sync_channel::<FlusherEntry>(200_000);
        let max_size = self.max_size;
        let max_bytes = self.max_bytes;
        let timeout = Duration::from_millis(self.interval_ms);
        let mut on_flush = self.on_flush.unwrap();

        std::thread::spawn(move || {
            let mut pending: Vec<u32> = Vec::with_capacity(max_size);
            let mut total_bytes: usize = 0;

            loop {
                // Block indefinitely when empty (0% CPU idle)
                let msg = if pending.is_empty() {
                    rx.recv().ok()
                } else {
                    // Block with timeout — resets on every successful recv
                    match rx.recv_timeout(timeout) {
                        Ok(m) => Some(m),
                        Err(_) => {
                            on_flush(&pending);
                            pending.clear();
                            total_bytes = 0;
                            continue;
                        }
                    }
                };

                match msg {
                    Some(e) => {
                        pending.push(e.env_seq);
                        total_bytes += e.bytes;
                        if pending.len() >= max_size || total_bytes >= max_bytes {
                            on_flush(&pending);
                            pending.clear();
                            total_bytes = 0;
                        }
                    }
                    None => {
                        if !pending.is_empty() {
                            on_flush(&pending);
                        }
                        break;
                    }
                }
            }
        });

        Flusher { tx }
    }
}
