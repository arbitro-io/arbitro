//! Consumer — push (subscribe) and pull (fetch) modes.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use tokio::sync::mpsc;
use zerocopy::IntoBytes;
use zerocopy::byteorder::little_endian::U32;

use arbitro_proto::action::Action;
use arbitro_proto::wire::subscribe::FetchFixed;

use crate::error::ClientError;
use crate::inner::Inner;
use crate::message::Message;
use crate::subscription::{Subscription, SubscriptionHandle};

/// A consumer bound to a stream.
pub struct Consumer {
    pub(crate) inner: Arc<Inner>,
    pub(crate) consumer_id: u32,
    pub(crate) stream_id: u32,
    next_sub_id: AtomicU64,
}

impl Consumer {
    pub(crate) fn new(inner: Arc<Inner>, consumer_id: u32, stream_id: u32) -> Self {
        Self {
            inner,
            consumer_id,
            stream_id,
            next_sub_id: AtomicU64::new(1),
        }
    }

    /// Push mode — subscribe with optional subject filter.
    /// Returns a handle to receive messages.
    pub async fn subscribe(
        &self,
        filter: Option<&[u8]>,
    ) -> Result<SubscriptionHandle, ClientError> {
        // Send Subscribe frame to server
        let subj = filter.unwrap_or(b">");
        let mut body = Vec::with_capacity(20 + subj.len());

        // SubscribeFixed: [4 consumer_id][2 subj_len][2 max_inflight][1 deliver_policy][1 deliver_mode][2 pad][8 start_seq]
        body.extend_from_slice(&self.consumer_id.to_le_bytes());
        body.extend_from_slice(&(subj.len() as u16).to_le_bytes());
        body.extend_from_slice(&0u16.to_le_bytes()); // max_inflight (server knows from consumer config)
        body.push(0); // deliver_policy
        body.push(0); // deliver_mode
        body.extend_from_slice(&[0u8; 2]); // pad
        body.extend_from_slice(&0u64.to_le_bytes()); // start_seq
        body.extend_from_slice(subj);

        self.inner.request(Action::Subscribe, self.stream_id, &body).await?;

        // Create local subscription for demux
        let sub_id = self.next_sub_id.fetch_add(1, Relaxed);
        let (tx, rx) = mpsc::channel(1024);

        let subscription = Subscription {
            stream_id: self.stream_id,
            consumer_id: self.consumer_id,
            filter: filter.map(Box::from),
            tx,
        };

        {
            let mut subs = self.inner.subscriptions.lock().unwrap();
            subs.insert(sub_id, subscription);
        }

        Ok(SubscriptionHandle { rx, _id: sub_id })
    }

    /// Pull mode — fetch up to N messages from the server.
    pub async fn fetch(&self, max_msgs: u32) -> Result<Vec<Message>, ClientError> {
        let body = FetchFixed {
            consumer_id: U32::new(self.consumer_id),
            max_msgs: U32::new(max_msgs),
        };

        self.inner.request(Action::Fetch, self.stream_id, body.as_bytes()).await?;

        // After fetch request, messages arrive as Deliver frames.
        // For now, we return empty — the messages will come via subscription.
        // A proper pull implementation would use a dedicated channel.
        Ok(Vec::new())
    }

    /// Get the consumer ID.
    pub fn id(&self) -> u32 {
        self.consumer_id
    }

    /// Get the stream ID.
    pub fn stream_id(&self) -> u32 {
        self.stream_id
    }
}
