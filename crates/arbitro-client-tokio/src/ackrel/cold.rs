//! Optional persistent cold tier for deferred acks. Backs [`super::AckRelay`]
//! so outstanding (delivered, not-yet-broker-confirmed) sequence numbers
//! survive a client process restart. All calls here are synchronous
//! `rusqlite` — callers MUST run them on `spawn_blocking` / a dedicated
//! thread, never on the async hot path.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection};

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Snapshot of one consumer's outstanding-ack range, as stored in
/// `consumer_state`. Cheap to load in bulk at startup — the actual
/// `seq`s are only pulled per-consumer via [`ColdStore::load_seqs`] when
/// the reconnect flow needs them.
pub(crate) struct StateSummary {
    pub consumer_id: u32,
    pub generation: u32,
    pub count: u32,
    pub min_seq: u64,
    pub max_seq: u64,
}

/// SQLite-backed cold tier. One connection, WAL mode, per client.
pub(crate) struct ColdStore {
    conn: Connection,
}

impl ColdStore {
    pub fn open(path: &Path) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS pending_acks(
                consumer_id INTEGER,
                generation INTEGER,
                seq INTEGER,
                created_at_ms INTEGER,
                PRIMARY KEY(consumer_id, generation, seq)
             );
             CREATE TABLE IF NOT EXISTS consumer_state(
                consumer_id INTEGER PRIMARY KEY,
                generation INTEGER,
                count INTEGER,
                min_seq INTEGER,
                max_seq INTEGER,
                updated_at_ms INTEGER
             );",
        )?;
        Ok(Self { conn })
    }

    /// Inserts `seqs` and upserts the consumer's summary row in one
    /// transaction. INVARIANT: `pending_acks` rows never land without a
    /// matching `consumer_state` update in the same commit.
    pub fn write_batch(
        &mut self,
        cid: u32,
        generation: u32,
        seqs: &[(u64, u64)],
        summary: StateSummary,
    ) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO pending_acks(consumer_id, generation, seq, created_at_ms)
                 VALUES (?1, ?2, ?3, ?4)",
            )?;
            for &(seq, created_at_ms) in seqs {
                stmt.execute(params![cid, generation, seq, created_at_ms])?;
            }
        }
        tx.execute(
            "INSERT INTO consumer_state(consumer_id, generation, count, min_seq, max_seq, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(consumer_id) DO UPDATE SET
                generation=excluded.generation,
                count=excluded.count,
                min_seq=excluded.min_seq,
                max_seq=excluded.max_seq,
                updated_at_ms=excluded.updated_at_ms",
            params![
                summary.consumer_id,
                summary.generation,
                summary.count,
                summary.min_seq,
                summary.max_seq,
                now_ms(),
            ],
        )?;
        tx.commit()
    }

    /// Drops every `pending_acks` row `<= new_cursor` (broker-confirmed
    /// cumulative ack) and reconciles `consumer_state` in the same
    /// transaction. When `summary.count == 0` the consumer has no more
    /// outstanding acks, so we delete the state row instead of upserting
    /// a zeroed one — keeps `load_summaries` free of dead entries.
    pub fn purge_up_to(
        &mut self,
        cid: u32,
        generation: u32,
        new_cursor: u64,
        summary: StateSummary,
    ) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute(
            "DELETE FROM pending_acks WHERE consumer_id=?1 AND generation=?2 AND seq<=?3",
            params![cid, generation, new_cursor],
        )?;
        if summary.count == 0 {
            tx.execute("DELETE FROM consumer_state WHERE consumer_id=?1", params![cid])?;
        } else {
            tx.execute(
                "INSERT INTO consumer_state(consumer_id, generation, count, min_seq, max_seq, updated_at_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(consumer_id) DO UPDATE SET
                    generation=excluded.generation,
                    count=excluded.count,
                    min_seq=excluded.min_seq,
                    max_seq=excluded.max_seq,
                    updated_at_ms=excluded.updated_at_ms",
                params![
                    summary.consumer_id,
                    summary.generation,
                    summary.count,
                    summary.min_seq,
                    summary.max_seq,
                    now_ms(),
                ],
            )?;
        }
        tx.commit()
    }

    /// Reconnect generation-mismatch: wipes all rows for `cid` from both
    /// tables in one transaction, matching the hot-tier purge invariant
    /// that deferred acks don't survive a session change.
    pub fn purge_generation(&mut self, cid: u32) -> rusqlite::Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM pending_acks WHERE consumer_id=?1", params![cid])?;
        tx.execute("DELETE FROM consumer_state WHERE consumer_id=?1", params![cid])?;
        tx.commit()
    }

    /// STARTUP PRELOAD: tiny read of `consumer_state` only — no scan of
    /// `pending_acks`. Feeds the gate seeds for `AckRelay::ensure`.
    pub fn load_summaries(&self) -> rusqlite::Result<Vec<StateSummary>> {
        let mut stmt = self
            .conn
            .prepare("SELECT consumer_id, generation, count, min_seq, max_seq FROM consumer_state")?;
        let rows = stmt.query_map([], |row| {
            Ok(StateSummary {
                consumer_id: row.get(0)?,
                generation: row.get(1)?,
                count: row.get(2)?,
                min_seq: row.get(3)?,
                max_seq: row.get(4)?,
            })
        })?;
        rows.collect()
    }

    /// LAZY per-consumer load: only called by the reconnect flow (T4)
    /// once it needs the actual seqs, not at startup.
    pub fn load_seqs(&self, cid: u32, generation: u32) -> rusqlite::Result<Vec<u64>> {
        let mut stmt = self.conn.prepare(
            "SELECT seq FROM pending_acks WHERE consumer_id=?1 AND generation=?2 ORDER BY seq ASC",
        )?;
        let rows = stmt.query_map(params![cid, generation], |row| row.get(0))?;
        rows.collect()
    }

    /// TTL sweep. Returns rows deleted; caller reconciles `consumer_state`
    /// summaries afterward.
    pub fn expire_older_than(&mut self, cutoff_ms: u64) -> rusqlite::Result<usize> {
        self.conn
            .execute("DELETE FROM pending_acks WHERE created_at_ms < ?1", params![cutoff_ms])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_mem() -> ColdStore {
        let conn = Connection::open_in_memory().unwrap();
        conn.pragma_update(None, "journal_mode", "WAL").unwrap();
        conn.pragma_update(None, "synchronous", "NORMAL").unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS pending_acks(
                consumer_id INTEGER,
                generation INTEGER,
                seq INTEGER,
                created_at_ms INTEGER,
                PRIMARY KEY(consumer_id, generation, seq)
             );
             CREATE TABLE IF NOT EXISTS consumer_state(
                consumer_id INTEGER PRIMARY KEY,
                generation INTEGER,
                count INTEGER,
                min_seq INTEGER,
                max_seq INTEGER,
                updated_at_ms INTEGER
             );",
        )
        .unwrap();
        ColdStore { conn }
    }

    #[test]
    fn write_batch_then_load_summaries() {
        let mut store = open_mem();
        let summary = StateSummary { consumer_id: 1, generation: 1, count: 3, min_seq: 10, max_seq: 30 };
        store
            .write_batch(1, 1, &[(10, 1_000), (20, 1_001), (30, 1_002)], summary)
            .unwrap();
        let summaries = store.load_summaries().unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].consumer_id, 1);
        assert_eq!(summaries[0].count, 3);
        assert_eq!(summaries[0].min_seq, 10);
        assert_eq!(summaries[0].max_seq, 30);
    }

    #[test]
    fn purge_up_to_deletes_and_updates_summary() {
        let mut store = open_mem();
        let summary = StateSummary { consumer_id: 1, generation: 1, count: 3, min_seq: 10, max_seq: 30 };
        store
            .write_batch(1, 1, &[(10, 1_000), (20, 1_001), (30, 1_002)], summary)
            .unwrap();
        let remaining = StateSummary { consumer_id: 1, generation: 1, count: 1, min_seq: 30, max_seq: 30 };
        store.purge_up_to(1, 1, 20, remaining).unwrap();
        let seqs = store.load_seqs(1, 1).unwrap();
        assert_eq!(seqs, vec![30]);
        let summaries = store.load_summaries().unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].count, 1);
        assert_eq!(summaries[0].min_seq, 30);
    }

    #[test]
    fn load_seqs_returns_ascending() {
        let mut store = open_mem();
        let summary = StateSummary { consumer_id: 2, generation: 1, count: 3, min_seq: 5, max_seq: 15 };
        store
            .write_batch(2, 1, &[(15, 1_000), (5, 1_001), (10, 1_002)], summary)
            .unwrap();
        assert_eq!(store.load_seqs(2, 1).unwrap(), vec![5, 10, 15]);
    }

    #[test]
    fn single_transaction_keeps_state_in_sync() {
        let mut store = open_mem();
        let summary = StateSummary { consumer_id: 3, generation: 1, count: 4, min_seq: 1, max_seq: 4 };
        store
            .write_batch(3, 1, &[(1, 1_000), (2, 1_000), (3, 1_000), (4, 1_000)], summary)
            .unwrap();
        let pending_rows = store.load_seqs(3, 1).unwrap().len();
        let summaries = store.load_summaries().unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].count as usize, pending_rows);
    }
}
