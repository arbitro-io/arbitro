//! Publish helpers — fire-and-forget and request/reply variants.
//!
//! Public methods on [`crate::Client`] forward into these helpers so
//! the Client struct stays slim.

use bytes::Bytes;

use crate::error::ClientError;
use crate::state::pending::Pending;
use crate::state::seq::SeqAllocator;
use crate::transport::encode::{
    encode_pub_batch_v2, encode_pub_v2, BatchEntry,
};
use crate::transport::frame::WriteFrame;
use crate::conn::session::WriteTx;

/// Fire-and-forget publish (single subject, single payload).
#[inline]
pub(crate) async fn publish_async(
    tx: &WriteTx,
    seq_alloc: &SeqAllocator,
    stream_id: u32,
    subject: &[u8],
    payload: Bytes,
) -> Result<(), ClientError> {
    let seq = seq_alloc.next();
    let (prefix, subject_b, payload_b) = encode_pub_v2(seq, stream_id, 0, subject, &payload);
    tx.send(WriteFrame::PubSingle {
        prefix,
        subject: subject_b,
        payload: payload_b,
    })
    .await
}

/// Fire-and-forget batch publish.
#[inline]
pub(crate) async fn publish_batch_async(
    tx: &WriteTx,
    seq_alloc: &SeqAllocator,
    stream_id: u32,
    entries: &[BatchEntry<'_>],
) -> Result<(), ClientError> {
    let seq = seq_alloc.next();
    let buf = encode_pub_batch_v2(seq, stream_id, 0, entries);
    tx.send(WriteFrame::PubBatch(buf)).await
}

/// Sync publish — registers a Pending slot, sends, awaits the broker
/// `RepOk` / `RepError` reply.
pub(crate) async fn publish_sync_async(
    tx: &WriteTx,
    pending: &Pending,
    seq_alloc: &SeqAllocator,
    stream_id: u32,
    subject: &[u8],
    payload: Bytes,
) -> Result<Bytes, ClientError> {
    let seq = seq_alloc.next();
    let rx  = pending.register(seq);
    let (prefix, subject_b, payload_b) = encode_pub_v2(seq, stream_id, 0, subject, &payload);
    tx.send(WriteFrame::PubSingle {
        prefix,
        subject: subject_b,
        payload: payload_b,
    })
    .await?;
    rx.recv_async().await
        .map_err(|_| ClientError::ChannelClosed)
        .and_then(|r| r)
}

/// Sync batch publish.
pub(crate) async fn publish_batch_sync_async(
    tx: &WriteTx,
    pending: &Pending,
    seq_alloc: &SeqAllocator,
    stream_id: u32,
    entries: &[BatchEntry<'_>],
) -> Result<Bytes, ClientError> {
    let seq = seq_alloc.next();
    let rx  = pending.register(seq);
    let buf = encode_pub_batch_v2(seq, stream_id, 0, entries);
    tx.send(WriteFrame::PubBatch(buf)).await?;
    rx.recv_async().await
        .map_err(|_| ClientError::ChannelClosed)
        .and_then(|r| r)
}
