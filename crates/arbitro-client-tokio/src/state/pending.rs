//! Pending request map: `seq -> kit::OneShotAsyncSender<RequestResult>`.
//!
//! Insert before send, remove on `RepOk` / `RepError`. On disconnect,
//! [`Pending::drain_disconnected`] resolves every entry with
//! `ClientError::Disconnected` so callers awaiting `recv_async` wake.

use std::collections::HashMap;
use std::sync::Mutex;

use bytes::Bytes;

use arbitro_kit::route::{OneShotAsync, OneShotAsyncReceiver, OneShotAsyncSender};

use crate::error::{ClientError, RequestResult};

/// H19: hard cap on the number of in-flight requests one client can have
/// outstanding. Without this, a server that has stopped replying (or a
/// reply path bug) keeps growing the `Pending` map until the host runs
/// out of memory. 100k entries × ~80 B/entry ≈ 8 MiB, well below any
/// modern client's memory budget but large enough that benign bursts
/// never hit the limit.
const DEFAULT_MAX_INFLIGHT: usize = 100_000;

#[derive(Default)]
pub(crate) struct Pending {
    map: Mutex<HashMap<u64, OneShotAsyncSender<RequestResult>>>,
}

impl std::fmt::Debug for Pending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pending")
            .field("len", &self.len())
            .finish()
    }
}

impl Pending {
    pub fn new() -> Self {
        Self { map: Mutex::new(HashMap::new()) }
    }

    /// Reserve a slot for `seq`; the returned receiver yields the reply
    /// (or `Disconnected` on shutdown).
    ///
    /// H19: enforces a hard cap (`DEFAULT_MAX_INFLIGHT`) on the size of
    /// the map. When the cap is hit the receiver is returned already
    /// resolved with `ClientError::ChannelClosed`, and the slot is
    /// **not** inserted — so the wire frame the caller is about to send
    /// will never get a reply slot to fill, but the caller learns about
    /// it immediately on the next `.recv_async().await`. This bounds
    /// memory under a server that's stopped responding while keeping
    /// the call shape identical for every existing call site.
    pub fn register(&self, seq: u64) -> OneShotAsyncReceiver<RequestResult> {
        let (tx, rx) = OneShotAsync::<RequestResult>::new();
        let mut g = self.map.lock().unwrap();
        if g.len() >= DEFAULT_MAX_INFLIGHT {
            // Drop the lock before resolving so the receiver wake doesn't
            // run under it (defensive — the wake is cheap, but no point
            // holding contention).
            drop(g);
            tx.send(Err(ClientError::ChannelClosed));
            return rx;
        }
        // Replace if duplicate (shouldn't happen — seq is allocated atomically).
        g.insert(seq, tx);
        rx
    }

    /// Resolve `seq` with success.
    #[inline]
    pub fn complete_ok(&self, seq: u64, payload: Bytes) {
        if let Some(tx) = self.map.lock().unwrap().remove(&seq) {
            tx.send(Ok(payload));
        }
    }

    /// Resolve `seq` with a wire error code.
    #[inline]
    pub fn complete_err(&self, seq: u64, code: u16) {
        if let Some(tx) = self.map.lock().unwrap().remove(&seq) {
            tx.send(Err(ClientError::from_wire_code(code)));
        }
    }

    /// Resolve every pending entry with `Disconnected`. Called on session
    /// teardown so awaiting callers wake instead of hanging forever.
    pub fn drain_disconnected(&self) {
        let drained: Vec<_> = {
            let mut g = self.map.lock().unwrap();
            g.drain().map(|(_, tx)| tx).collect()
        };
        for tx in drained {
            tx.send(Err(ClientError::Disconnected));
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use bytes::Bytes;
    use crate::error::ClientError;

    #[tokio::test]
    async fn insert_remove_does_not_dangle() {
        let p = Pending::new();
        let rx = p.register(1);
        assert_eq!(p.len(), 1);
        p.complete_ok(1, Bytes::from_static(b"pong"));
        let val = rx.recv_async().await.unwrap().unwrap();
        assert_eq!(&val[..], b"pong");
        assert_eq!(p.len(), 0);
    }

    #[tokio::test]
    async fn drain_on_disconnect_wakes_all_with_disconnected() {
        let p = Arc::new(Pending::new());
        let rxs: Vec<_> = (1u64..=8).map(|seq| p.register(seq)).collect();
        assert_eq!(p.len(), 8);

        let p2 = Arc::clone(&p);
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            p2.drain_disconnected();
        });

        for rx in rxs {
            let result = rx.recv_async().await.unwrap();
            assert!(
                matches!(result, Err(ClientError::Disconnected)),
                "expected Disconnected, got {result:?}"
            );
        }
        assert_eq!(p.len(), 0);
    }
}
