//! FanoutQueue — fire-and-forget notification buffer for publish fanout.
//!
//! Level 1 — depends on `types` only.
//!
//! The engine pushes ONE notification per connection per message.
//! The client knows its own subscriptions and matches locally.
//! Each entry is just: "connection X, message (subject, seq) arrived".

use crate::types::*;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout};

/// A fanout notification: "connection X, message arrived".
/// The client matches locally against its subscriptions.
/// Zero-copy: cast `&[FanoutEntry]` ↔ `&[u8]` directly. 24 bytes.
#[derive(Debug, Clone, Copy, IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct FanoutEntry {
    pub connection_id: ConnectionId, // 8
    pub seq: u64,                    // 8
    pub subject_hash: u32,           // 4
    _pad: u32,                       // 4
}

impl FanoutEntry {
    /// Construct a new fanout notification.
    #[inline]
    pub fn new(connection_id: ConnectionId, subject_hash: u32, seq: u64) -> Self {
        Self {
            connection_id,
            seq,
            subject_hash,
            _pad: 0,
        }
    }
}

const _: () = assert!(std::mem::size_of::<FanoutEntry>() <= 24);

/// Pre-allocated buffer for fanout notifications.
///
/// The engine writes during publish (h ot path). The protocol layer
/// drains after each batch or on its own schedule. No locks —
/// single-threaded engine owns this buffer.
pub struct FanoutQueue {
    entries: Vec<FanoutEntry>,
    len: usize,
}

impl FanoutQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
            len: 0,
        }
    }

    /// Push a notification. O(1) amortized.
    #[inline]
    pub fn push(&mut self, entry: FanoutEntry) {
        if self.len < self.entries.len() {
            self.entries[self.len] = entry;
        } else {
            self.entries.push(entry);
        }
        self.len += 1;
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn reset(&mut self) {
        self.len = 0;
    }

    /// Drain and reset in one step. Auto-resets when dropped.
    #[inline]
    pub fn take(&mut self) -> FanoutDrain<'_> {
        FanoutDrain { queue: self }
    }
}

/// RAII drain guard — auto-resets the queue when dropped.
pub struct FanoutDrain<'a> {
    queue: &'a mut FanoutQueue,
}

impl<'a> FanoutDrain<'a> {
    #[inline]
    pub fn entries(&self) -> &[FanoutEntry] {
        &self.queue.entries[..self.queue.len]
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.queue.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.queue.len == 0
    }

    /// View entries as raw bytes. Zero-copy cast, no allocation.
    /// `&[FanoutEntry]` → `&[u8]` directly.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        zerocopy::IntoBytes::as_bytes(self.entries())
    }
}

impl<'a> Drop for FanoutDrain<'a> {
    fn drop(&mut self) {
        self.queue.len = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_and_drain() {
        let mut q = FanoutQueue::new(4);
        assert!(q.is_empty());

        q.push(FanoutEntry::new(ConnectionId(1), 0xBEEF, 100));
        q.push(FanoutEntry::new(ConnectionId(2), 0xDEAD, 101));

        assert_eq!(q.len(), 2);

        let drain = q.take();
        assert_eq!(drain.len(), 2);
        assert_eq!(drain.entries()[0].connection_id, ConnectionId(1));
        assert_eq!(drain.entries()[1].seq, 101);
        drop(drain);

        assert!(q.is_empty());
    }

    #[test]
    fn reuses_capacity() {
        let mut q = FanoutQueue::new(2);

        for round in 0..3 {
            q.push(FanoutEntry::new(ConnectionId(round), 0, round));
            let drain = q.take();
            assert_eq!(drain.len(), 1);
            assert_eq!(drain.entries()[0].seq, round);
            drop(drain);
        }
    }
}
