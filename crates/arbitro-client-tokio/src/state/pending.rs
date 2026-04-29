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
    pub fn register(&self, seq: u64) -> OneShotAsyncReceiver<RequestResult> {
        let (tx, rx) = OneShotAsync::<RequestResult>::new();
        let mut g = self.map.lock().unwrap();
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
