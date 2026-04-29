//! Public `Client` facade.
//!
//! Cloning is cheap (`Arc` bump). All clones share the same underlying
//! transport, pending map, and seq allocator. Drop the last clone (or
//! call [`Client::close`]) to cancel every spawned task.

use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;

use crate::config::ClientConfig;
use crate::conn::session::{spawn_connection, WriteTx};
use crate::error::ClientError;
use crate::state::pending::Pending;
use crate::state::seq::SeqAllocator;

/// One entry of a batch publish: `{ subject: &[u8], payload: Bytes }`.
pub use crate::transport::encode::BatchEntry;

#[derive(Debug)]
pub(crate) struct Inner {
    pub(crate) cfg:       ClientConfig,
    pub(crate) tx:        WriteTx,
    pub(crate) pending:   Arc<Pending>,
    pub(crate) seq_alloc: SeqAllocator,
    pub(crate) cancel:    CancellationToken,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Last `Client` clone gone — tear down every spawned task.
        self.cancel.cancel();
        self.pending.drain_disconnected();
    }
}

/// Handle to a tokio-driven Arbitro connection.
#[derive(Debug, Clone)]
pub struct Client {
    inner: Arc<Inner>,
}

impl Client {
    /// Connect to the broker described by `cfg`. Resolves once the
    /// initial TCP dial + HELLO succeeds; subsequent reconnects (if
    /// enabled) are handled transparently in the background.
    pub async fn connect(cfg: ClientConfig) -> Result<Self, ClientError> {
        let pending = Arc::new(Pending::new());
        let cancel  = CancellationToken::new();
        let tx = spawn_connection(cfg.clone(), Arc::clone(&pending), cancel.clone()).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                cfg,
                tx,
                pending,
                seq_alloc: SeqAllocator::new(),
                cancel,
            }),
        })
    }

    /// Cancel every spawned task immediately. Idempotent.
    pub fn close(&self) {
        self.inner.cancel.cancel();
    }

    // ── publish ───────────────────────────────────────────────────────

    /// Fire-and-forget publish.
    #[inline]
    pub async fn publish(
        &self,
        stream_id: u32,
        subject: &[u8],
        payload: Bytes,
    ) -> Result<(), ClientError> {
        crate::publish::publish_async(
            &self.inner.tx,
            &self.inner.seq_alloc,
            stream_id,
            subject,
            payload,
        )
        .await
    }

    /// Fire-and-forget publish from a `&[u8]`.
    #[inline]
    pub async fn publish_bytes(
        &self,
        stream_id: u32,
        subject: &[u8],
        payload: &[u8],
    ) -> Result<(), ClientError> {
        self.publish(stream_id, subject, Bytes::copy_from_slice(payload)).await
    }

    /// Sync publish — awaits broker `RepOk` / `RepError`.
    #[inline]
    pub async fn publish_sync(
        &self,
        stream_id: u32,
        subject: &[u8],
        payload: Bytes,
    ) -> Result<Bytes, ClientError> {
        crate::publish::publish_sync_async(
            &self.inner.tx,
            &self.inner.pending,
            &self.inner.seq_alloc,
            stream_id,
            subject,
            payload,
        )
        .await
    }

    /// Fire-and-forget batch publish.
    pub async fn publish_batch(
        &self,
        stream_id: u32,
        entries: &[BatchEntry<'_>],
    ) -> Result<(), ClientError> {
        crate::publish::publish_batch_async(
            &self.inner.tx,
            &self.inner.seq_alloc,
            stream_id,
            entries,
        )
        .await
    }

    /// Sync batch publish.
    pub async fn publish_batch_sync(
        &self,
        stream_id: u32,
        entries: &[BatchEntry<'_>],
    ) -> Result<Bytes, ClientError> {
        crate::publish::publish_batch_sync_async(
            &self.inner.tx,
            &self.inner.pending,
            &self.inner.seq_alloc,
            stream_id,
            entries,
        )
        .await
    }

    // ── manage ────────────────────────────────────────────────────────

    /// CreateStream — returns the broker's raw `RepOk` payload.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_stream(
        &self,
        name: &[u8], filter: &[u8],
        max_msgs: u64, max_bytes: u64, max_age_secs: u64,
        replicas: u8, journal_kind: u8, retention: u8, discard: u8,
    ) -> Result<Bytes, ClientError> {
        crate::manage::create_stream(
            &self.inner.tx, &self.inner.pending, &self.inner.seq_alloc,
            name, filter, max_msgs, max_bytes, max_age_secs,
            replicas, journal_kind, retention, discard,
        ).await
    }

    pub async fn delete_stream(&self, name: &[u8]) -> Result<Bytes, ClientError> {
        crate::manage::delete_stream(
            &self.inner.tx, &self.inner.pending, &self.inner.seq_alloc, name,
        ).await
    }

    pub async fn get_stream(&self, name: &[u8]) -> Result<Bytes, ClientError> {
        crate::manage::get_stream(
            &self.inner.tx, &self.inner.pending, &self.inner.seq_alloc, name,
        ).await
    }

    pub async fn purge_stream(&self, name: &[u8]) -> Result<Bytes, ClientError> {
        crate::manage::purge_stream(
            &self.inner.tx, &self.inner.pending, &self.inner.seq_alloc, name,
        ).await
    }

    pub async fn drain_subject(
        &self, name: &[u8], subject: &[u8],
    ) -> Result<Bytes, ClientError> {
        crate::manage::drain_subject(
            &self.inner.tx, &self.inner.pending, &self.inner.seq_alloc, name, subject,
        ).await
    }

    pub async fn list_streams(
        &self, offset: u32, limit: u32,
    ) -> Result<Bytes, ClientError> {
        crate::manage::list_streams(
            &self.inner.tx, &self.inner.pending, &self.inner.seq_alloc, offset, limit,
        ).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_consumer(
        &self,
        stream_id: u32, name: &[u8], group: &[u8], subject: &[u8],
        max_inflight: u16, ack_policy: u8, deliver_policy: u8, deliver_mode: u8,
        ack_wait_ms: u32, start_seq: u64,
    ) -> Result<Bytes, ClientError> {
        crate::manage::create_consumer(
            &self.inner.tx, &self.inner.pending, &self.inner.seq_alloc,
            stream_id, name, group, subject, max_inflight,
            ack_policy, deliver_policy, deliver_mode, ack_wait_ms, start_seq,
        ).await
    }

    pub async fn delete_consumer(&self, consumer_id: u32) -> Result<Bytes, ClientError> {
        crate::manage::delete_consumer(
            &self.inner.tx, &self.inner.pending, &self.inner.seq_alloc, consumer_id,
        ).await
    }

    pub async fn get_consumer(
        &self, stream_id: u32, name: &[u8],
    ) -> Result<Bytes, ClientError> {
        crate::manage::get_consumer(
            &self.inner.tx, &self.inner.pending, &self.inner.seq_alloc, stream_id, name,
        ).await
    }

    pub async fn list_consumers(
        &self, stream_id: u32, offset: u32, limit: u32,
    ) -> Result<Bytes, ClientError> {
        crate::manage::list_consumers(
            &self.inner.tx, &self.inner.pending, &self.inner.seq_alloc,
            stream_id, offset, limit,
        ).await
    }
}
