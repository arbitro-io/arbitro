//! Local subscription — client-side demux target.

use bytes::Bytes;
use arbitro_common::subject::subject_matches;
use tokio::sync::mpsc;

use crate::message::Message;

/// A local subscription that receives messages matching a subject filter.
pub(crate) struct Subscription {
    pub stream_id: u32,
    pub consumer_id: u32,
    pub filter: Option<Box<[u8]>>,
    pub tx: mpsc::Sender<Message>,
    /// Subscribe body stored for automatic re-subscription after reconnect.
    pub subscribe_body: Bytes,
}

impl Subscription {
    /// Check if a subject matches this subscription's filter.
    pub fn matches(&self, subj: &[u8]) -> bool {
        match &self.filter {
            None => true, // no filter = match all
            Some(pattern) => subject_matches(pattern, subj),
        }
    }
}

/// Handle returned to the user for receiving messages.
pub struct SubscriptionHandle {
    pub(crate) rx: mpsc::Receiver<Message>,
    pub(crate) _id: u64,
}

impl SubscriptionHandle {
    /// Receive the next message. Returns None if subscription is closed.
    pub async fn next(&mut self) -> Option<Message> {
        self.rx.recv().await
    }
}

/// Handle for callback-based subscriptions.
/// Aborts the background task and removes the subscription on drop.
pub struct CallbackHandle {
    pub(crate) _handle: tokio::task::JoinHandle<()>,
    pub(crate) inner: std::sync::Arc<crate::inner::Inner>,
    pub(crate) sub_id: u64,
}

impl Drop for CallbackHandle {
    fn drop(&mut self) {
        self._handle.abort();
        self.inner.remove_subscription(self.sub_id);
    }
}
