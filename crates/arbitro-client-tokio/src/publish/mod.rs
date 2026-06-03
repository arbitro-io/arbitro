//! Publish helpers — fire-and-forget and request/reply variants.

use std::future::Future;

use bytes::Bytes;
use arbitro_proto::v2::ingress::pub_frame::PubFrame;

use crate::error::ClientError;
use crate::state::pending::Pending;
use crate::state::seq::SeqAllocator;
use crate::transport::encode::{encode_pub_batch_v2, encode_pub_with_reply_v2, BatchEntry};
use crate::transport::frame::{WriteFrame, WriteProducer, INLINE_CAP};

/// Maximum number of entries in a single publish-batch frame.
/// Batches larger than this are automatically chunked by the client.
pub const PUBLISH_BATCH_MAX: usize = 256;

/// Enqueue a frame into the producer ring via `try_send`.
/// Synchronous — no await — so `&WriteProducer` never crosses an await
/// point and futures that call this remain `Send`.
#[inline]
pub(crate) fn enqueue(tx: &WriteProducer, frame: WriteFrame) -> Result<(), ClientError> {
    tx.try_send(frame).map_err(|_| ClientError::ChannelClosed)
}

/// Encode a single PubFrame into a `WriteFrame`.
///
/// Frames ≤ `INLINE_CAP` bytes are stored inline (zero heap allocation).
/// Larger frames fall back to a `Bytes` allocation.
///
/// Pass `msg_id = &[]` for the legacy / non-dedup case. A non-empty
/// `msg_id` opts the message into broker-side idempotency dedup on
/// streams that have a non-zero `idempotency_window_ms`.
#[inline]
fn encode_pub_frame(
    seq: u64,
    stream_id: u32,
    subject: &[u8],
    msg_id: &[u8],
    payload: &[u8],
) -> WriteFrame {
    let size = PubFrame::wire_size(subject.len(), msg_id.len(), payload.len());
    if size <= INLINE_CAP {
        let mut data = [0u8; INLINE_CAP];
        PubFrame::encode_into(
            &mut data[..size], seq, stream_id, 0, 0, subject, msg_id, payload,
        );
        WriteFrame::Inline(data, size as u16)
    } else {
        let mut buf = vec![0u8; size];
        PubFrame::encode_into(&mut buf, seq, stream_id, 0, 0, subject, msg_id, payload);
        WriteFrame::Mono(Bytes::from(buf))
    }
}

/// Fire-and-forget publish (single subject, single payload).
#[inline]
pub(crate) fn publish_async(
    tx: &WriteProducer,
    seq_alloc: &SeqAllocator,
    stream_id: u32,
    subject: &[u8],
    msg_id: &[u8],
    payload: Bytes,
) -> Result<(), ClientError> {
    let seq = seq_alloc.next();
    enqueue(tx, encode_pub_frame(seq, stream_id, subject, msg_id, &payload))
}

/// Fire-and-forget batch publish.
///
/// Automatically chunks into sub-batches of [`PUBLISH_BATCH_MAX`] entries
/// when the caller provides more than 256 entries.
pub(crate) fn publish_batch_async(
    tx: &WriteProducer,
    seq_alloc: &SeqAllocator,
    stream_id: u32,
    entries: &[BatchEntry<'_>],
) -> Result<(), ClientError> {
    for chunk in entries.chunks(PUBLISH_BATCH_MAX) {
        let seq = seq_alloc.next();
        enqueue(tx, WriteFrame::PubBatch(encode_pub_batch_v2(seq, stream_id, 0, chunk)))?;
    }
    Ok(())
}

/// Sync publish — registers a Pending slot, sends, returns a `Send` future
/// that awaits the broker reply.
///
/// All synchronous work (encode, register, enqueue) is done before the
/// `async move` block so that no `&WriteProducer` / `&Pending` borrow
/// crosses the await point. Only the owned `rx` receiver (which is `Send`)
/// lives inside the returned future.
pub(crate) fn publish_sync_async(
    tx: &WriteProducer,
    pending: &Pending,
    seq_alloc: &SeqAllocator,
    stream_id: u32,
    subject: &[u8],
    msg_id: &[u8],
    payload: Bytes,
) -> impl Future<Output = Result<Bytes, ClientError>> + Send {
    let seq   = seq_alloc.next();
    let frame = encode_pub_frame(seq, stream_id, subject, msg_id, &payload);
    let rx    = pending.register(seq);
    let enqueue_result = enqueue(tx, frame);
    async move {
        enqueue_result?;
        rx.recv_async().await
            .map_err(|_| ClientError::ChannelClosed)
            .and_then(|r| r)
    }
}

/// Delayed publish — parks the message in the broker's delayed journal.
/// The message will be delivered to consumers after `delay_ms` milliseconds.
/// Returns a future that resolves once the broker confirms receipt.
pub(crate) fn publish_delayed_async(
    tx: &WriteProducer,
    pending: &Pending,
    seq_alloc: &SeqAllocator,
    stream_id: u32,
    subject: &[u8],
    payload: Bytes,
    delay_ms: u64,
) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
    let seq   = seq_alloc.next();
    let frame = WriteFrame::Mono(
        crate::transport::encode::encode_pub_delayed_v2(seq, stream_id, subject, &payload, delay_ms)
    );
    let rx    = pending.register(seq);
    let enqueue_result = enqueue(tx, frame);
    async move {
        enqueue_result?;
        rx.recv_async().await
            .map_err(|_| ClientError::ChannelClosed)
            .and_then(|r| r)
    }
}

/// Publish with a reply-to subject (request/reply pattern).
///
/// The broker stores the entry with the reply_to subject and delivers it
/// to consumers. Consumers see `msg.reply_to()` and can publish a response
/// to that subject. The caller is responsible for subscribing to the
/// reply_to subject (typically an `_INBOX.<token>` pattern) before calling.
pub(crate) fn publish_with_reply_async(
    tx: &WriteProducer,
    pending: &Pending,
    seq_alloc: &SeqAllocator,
    stream_id: u32,
    subject: &[u8],
    reply_to: &[u8],
    msg_id: &[u8],
    payload: Bytes,
) -> impl Future<Output = Result<Bytes, ClientError>> + Send {
    let seq   = seq_alloc.next();
    let frame = WriteFrame::Mono(encode_pub_with_reply_v2(seq, stream_id, subject, reply_to, msg_id, &payload));
    let rx    = pending.register(seq);
    let enqueue_result = enqueue(tx, frame);
    async move {
        enqueue_result?;
        rx.recv_async().await
            .map_err(|_| ClientError::ChannelClosed)
            .and_then(|r| r)
    }
}

/// Sync batch publish — same pattern: sync work first, only `rx` in future.
///
/// Automatically chunks into sub-batches of [`PUBLISH_BATCH_MAX`] entries.
/// Returns the broker reply (`first_seq`) from the **first** chunk.
/// Subsequent chunks are sent fire-and-forget style (no pending slot)
/// because only the first seq is meaningful to the caller.
pub(crate) fn publish_batch_sync_async(
    tx: &WriteProducer,
    pending: &Pending,
    seq_alloc: &SeqAllocator,
    stream_id: u32,
    entries: &[BatchEntry<'_>],
) -> impl Future<Output = Result<Bytes, ClientError>> + Send {
    let mut chunks = entries.chunks(PUBLISH_BATCH_MAX);

    // First chunk — register a pending slot so we can await the reply.
    let first_chunk = chunks.next().unwrap_or(&[]);
    let seq = seq_alloc.next();
    let rx  = pending.register(seq);
    let mut enqueue_result = enqueue(
        tx,
        WriteFrame::PubBatch(encode_pub_batch_v2(seq, stream_id, 0, first_chunk)),
    );

    // Remaining chunks — fire-and-forget (each gets its own seq).
    if enqueue_result.is_ok() {
        for chunk in chunks {
            let seq = seq_alloc.next();
            if let Err(e) = enqueue(
                tx,
                WriteFrame::PubBatch(encode_pub_batch_v2(seq, stream_id, 0, chunk)),
            ) {
                enqueue_result = Err(e);
                break;
            }
        }
    }

    async move {
        enqueue_result?;
        rx.recv_async().await
            .map_err(|_| ClientError::ChannelClosed)
            .and_then(|r| r)
    }
}
