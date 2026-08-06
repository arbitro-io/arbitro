//! Subscription registry: maps `consumer_id` → message delivery channel.
//!
//! The broker performs subject matching; the client only routes Deliver
//! frames by `consumer_id`.  No `SubjectTrie` is needed client-side.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::ackstore::SlotRef;
use crate::consume::message::Message;

/// Per-subscription state stored in the registry.
pub(crate) struct SubRecord {
    /// Stream the consumer belongs to (informational).
    pub stream_id: u32,
    /// Pre-encoded `SubFrame` replayed verbatim on every reconnect.
    pub sub_body: Bytes,
    /// Sender end of the message delivery channel.
    pub tx: mpsc::Sender<Message>,
    /// Durable dedup slot for this `(stream, consumer)`, resolved once at
    /// subscribe time. `None` when persistence is disabled. Cloned into each
    /// delivered `Message` (record-on-ack) and consulted on the delivery hot
    /// path (`seen`) and the broker-cursor path (`confirm_up_to`).
    pub slot: Option<Arc<dyn SlotRef>>,
}

impl std::fmt::Debug for SubRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubRecord")
            .field("stream_id", &self.stream_id)
            .finish()
    }
}

/// Registry that maps live `consumer_id`s to their delivery channels.
#[derive(Debug, Default)]
pub(crate) struct Subscriptions {
    inner: RwLock<HashMap<u32, SubRecord>>,
}

impl Subscriptions {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Register a subscription for `consumer_id`.
    ///
    /// Stores `sub_body` for reconnect replay.
    /// Returns the receiver end of the channel (capacity = 4096).
    pub fn register(
        &self,
        consumer_id: u32,
        stream_id: u32,
        sub_body: Bytes,
        slot: Option<Arc<dyn SlotRef>>,
    ) -> mpsc::Receiver<Message> {
        let (tx, rx) = mpsc::channel(4096);
        self.inner.write().unwrap().insert(
            consumer_id,
            SubRecord {
                stream_id,
                sub_body,
                tx,
                slot,
            },
        );
        rx
    }

    /// Remove a subscription.  Called when `SubscriptionHandle` is dropped.
    pub fn remove(&self, consumer_id: u32) {
        self.inner.write().unwrap().remove(&consumer_id);
    }

    /// Route a delivered `Message` to its subscriber (async, backpressure).
    ///
    /// Waits if the subscriber channel is full — this applies backpressure
    /// up through the reader task → TCP reads → server write buffer → drain.
    ///
    /// Returns `false` if `consumer_id` is not registered (frame silently
    /// dropped — safe during reconnect before resub is confirmed).
    pub async fn send(&self, consumer_id: u32, msg: Message) -> bool {
        // Clone the Sender under the lock, then drop the lock before awaiting.
        // This avoids holding the RwLock across an await point.
        let tx = {
            let guard = self.inner.read().unwrap();
            match guard.get(&consumer_id) {
                Some(rec) => rec.tx.clone(),
                None => return false,
            }
        };
        // Await — suspends the reader task if the subscriber is slow.
        let _ = tx.send(msg).await;
        true
    }

    /// Look up the `(stream_id, dedup slot)` for a registered consumer in a
    /// single lock acquisition. Returns `(0, None)` when unknown — matching
    /// the demux hot path's `unwrap_or` fallbacks.
    pub fn route_of(&self, consumer_id: u32) -> (u32, Option<Arc<dyn SlotRef>>) {
        match self.inner.read().unwrap().get(&consumer_id) {
            Some(r) => (r.stream_id, r.slot.clone()),
            None => (0, None),
        }
    }

    /// Look up the durable dedup slot for a registered consumer. Used by the
    /// broker-cursor cleanup path (`AckBatchResp` / `AckStateRep`).
    pub fn slot_of(&self, consumer_id: u32) -> Option<Arc<dyn SlotRef>> {
        self.inner
            .read()
            .unwrap()
            .get(&consumer_id)
            .and_then(|r| r.slot.clone())
    }

    /// Return all stored `sub_body` buffers for reconnect replay.
    /// Non-destructive — subscriptions remain registered.
    pub fn all_sub_bodies(&self) -> Vec<Bytes> {
        self.inner
            .read()
            .unwrap()
            .values()
            .map(|r| r.sub_body.clone())
            .collect()
    }

    /// Consumer ids that carry a durable dedup slot. Feeds the reconnect
    /// `AckStateReq` sweep (`conn::session::send_ack_state_reqs`): every
    /// slotted consumer asks the broker for its ack cursor once per
    /// (re)connect so stale WAL entries left by a dead session get purged
    /// even when no deferred acks are outstanding. Cold path.
    pub fn slotted_consumer_ids(&self) -> Vec<u32> {
        self.inner
            .read()
            .unwrap()
            .iter()
            .filter(|(_, r)| r.slot.is_some())
            .map(|(&cid, _)| cid)
            .collect()
    }
}
