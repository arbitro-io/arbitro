//! Stream / consumer admin API.
//!
//! Each call:
//! 1. allocates a new `seq`,
//! 2. registers a `Pending` slot,
//! 3. encodes the v2 manager frame,
//! 4. enqueues it to the writer,
//! 5. awaits the broker reply.
//!
//! Reply payloads are returned as raw `Bytes`; higher-level decoders
//! (StreamInfo etc.) live in the engine crate and can be applied by
//! callers as needed.

use bytes::Bytes;

use crate::conn::session::WriteTx;
use crate::error::ClientError;
use crate::state::pending::Pending;
use crate::state::seq::SeqAllocator;
use crate::transport::encode::{
    encode_create_consumer_v2, encode_create_stream_v2, encode_delete_consumer_v2,
    encode_delete_stream_v2, encode_drain_subject_v2, encode_get_consumer_v2,
    encode_get_stream_v2, encode_list_consumers_v2, encode_list_streams_v2,
    encode_purge_stream_v2,
};
use crate::transport::frame::WriteFrame;

#[inline]
async fn request(
    tx: &WriteTx,
    pending: &Pending,
    seq: u64,
    body: Bytes,
) -> Result<Bytes, ClientError> {
    let rx = pending.register(seq);
    tx.send(WriteFrame::Mono(body)).await?;
    rx.recv_async().await
        .map_err(|_| ClientError::ChannelClosed)
        .and_then(|r| r)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_stream(
    tx: &WriteTx, pending: &Pending, seq_alloc: &SeqAllocator,
    name: &[u8], filter: &[u8],
    max_msgs: u64, max_bytes: u64, max_age_secs: u64,
    replicas: u8, journal_kind: u8, retention: u8, discard: u8,
) -> Result<Bytes, ClientError> {
    let seq = seq_alloc.next();
    let buf = encode_create_stream_v2(
        seq, name, filter, max_msgs, max_bytes, max_age_secs,
        replicas, journal_kind, retention, discard,
    );
    request(tx, pending, seq, buf).await
}

pub(crate) async fn delete_stream(
    tx: &WriteTx, pending: &Pending, seq_alloc: &SeqAllocator, name: &[u8],
) -> Result<Bytes, ClientError> {
    let seq = seq_alloc.next();
    request(tx, pending, seq, encode_delete_stream_v2(seq, name)).await
}

pub(crate) async fn get_stream(
    tx: &WriteTx, pending: &Pending, seq_alloc: &SeqAllocator, name: &[u8],
) -> Result<Bytes, ClientError> {
    let seq = seq_alloc.next();
    request(tx, pending, seq, encode_get_stream_v2(seq, name)).await
}

pub(crate) async fn purge_stream(
    tx: &WriteTx, pending: &Pending, seq_alloc: &SeqAllocator, name: &[u8],
) -> Result<Bytes, ClientError> {
    let seq = seq_alloc.next();
    request(tx, pending, seq, encode_purge_stream_v2(seq, name)).await
}

pub(crate) async fn drain_subject(
    tx: &WriteTx, pending: &Pending, seq_alloc: &SeqAllocator,
    name: &[u8], subject: &[u8],
) -> Result<Bytes, ClientError> {
    let seq = seq_alloc.next();
    request(tx, pending, seq, encode_drain_subject_v2(seq, name, subject)).await
}

pub(crate) async fn list_streams(
    tx: &WriteTx, pending: &Pending, seq_alloc: &SeqAllocator,
    offset: u32, limit: u32,
) -> Result<Bytes, ClientError> {
    let seq = seq_alloc.next();
    request(tx, pending, seq, encode_list_streams_v2(seq, offset, limit)).await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_consumer(
    tx: &WriteTx, pending: &Pending, seq_alloc: &SeqAllocator,
    stream_id: u32, name: &[u8], group: &[u8], subject: &[u8],
    max_inflight: u16, ack_policy: u8, deliver_policy: u8, deliver_mode: u8,
    ack_wait_ms: u32, start_seq: u64,
) -> Result<Bytes, ClientError> {
    let seq = seq_alloc.next();
    let buf = encode_create_consumer_v2(
        seq, stream_id, name, group, subject, max_inflight,
        ack_policy, deliver_policy, deliver_mode, ack_wait_ms, start_seq,
    );
    request(tx, pending, seq, buf).await
}

pub(crate) async fn delete_consumer(
    tx: &WriteTx, pending: &Pending, seq_alloc: &SeqAllocator, consumer_id: u32,
) -> Result<Bytes, ClientError> {
    let seq = seq_alloc.next();
    request(tx, pending, seq, encode_delete_consumer_v2(seq, consumer_id)).await
}

pub(crate) async fn get_consumer(
    tx: &WriteTx, pending: &Pending, seq_alloc: &SeqAllocator,
    stream_id: u32, name: &[u8],
) -> Result<Bytes, ClientError> {
    let seq = seq_alloc.next();
    request(tx, pending, seq, encode_get_consumer_v2(seq, stream_id, name)).await
}

pub(crate) async fn list_consumers(
    tx: &WriteTx, pending: &Pending, seq_alloc: &SeqAllocator,
    stream_id: u32, offset: u32, limit: u32,
) -> Result<Bytes, ClientError> {
    let seq = seq_alloc.next();
    request(tx, pending, seq, encode_list_consumers_v2(seq, stream_id, offset, limit)).await
}
