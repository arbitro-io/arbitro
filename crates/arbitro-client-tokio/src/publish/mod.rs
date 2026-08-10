//! Publish helpers — fire-and-forget and request/reply variants.

use std::future::Future;
use std::sync::Arc;

use arbitro_proto::v2::header::entry_flag;
use arbitro_proto::v2::ingress::pub_frame::PubFrame;
use arbitro_proto::wire::msg_headers::{encode_extended_payload_vec, HDR_MSG_ID};
use bytes::Bytes;

use crate::error::ClientError;
use crate::state::pending::Pending;
use crate::state::seq::SeqAllocator;
use crate::transport::encode::{encode_pub_batch_v2, encode_pub_with_reply_v2, BatchEntry};
use crate::transport::frame::{WriteFrame, WritePool, INLINE_CAP};
use arbitro_kit::route::AcquireError;
use arbitro_kit::stream::TrySendError;

/// Maximum number of entries in a single publish-batch frame.
/// Batches larger than this are automatically chunked by the client.
pub const PUBLISH_BATCH_MAX: usize = 256;

/// Default for [`ClientConfig::max_block`](crate::ClientConfig::max_block).
/// Same role as Kafka's `max.block.ms`: bounded so a genuinely dead peer
/// surfaces as an error instead of an unbounded hang.
pub const DEFAULT_MAX_BLOCK: std::time::Duration = std::time::Duration::from_secs(5);

/// Lease a producer slot from `pool` and `try_send` a single frame.
/// Synchronous — no await — so the borrow never crosses an await point and
/// futures that call this remain `Send`.
///
/// A full ring is [`ClientError::QueueFull`]: transient, and the caller may
/// retry or park. Reporting it as `ChannelClosed` is what silently dropped
/// tail batches — callers cannot tell "wait a moment" from "give up".
#[inline]
pub(crate) fn enqueue(pool: &Arc<WritePool>, frame: WriteFrame) -> Result<(), ClientError> {
    let mut lease = pool.acquire().map_err(|e| match e {
        // Terminal: the channel is shut down and no lease will ever work.
        AcquireError::Closed => ClientError::ChannelClosed,
        // Transient: every ring is leased right now. Retryable.
        AcquireError::Exhausted => ClientError::PoolExhausted,
    })?;
    match lease.try_send(frame) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => Err(ClientError::QueueFull),
        Err(TrySendError::Closed(_)) => Err(ClientError::ChannelClosed),
    }
}

/// Enqueue, parking on the producer's backpressure waiter while the ring is
/// full (0% CPU — no polling). For callers that already await a broker
/// reply, so blocking here costs nothing they had not already accepted.
pub(crate) async fn enqueue_wait(
    pool: &Arc<WritePool>,
    frame: WriteFrame,
    max_block: std::time::Duration,
) -> Result<(), ClientError> {
    let mut lease = pool.acquire().map_err(|e| match e {
        // Terminal: the channel is shut down and no lease will ever work.
        AcquireError::Closed => ClientError::ChannelClosed,
        // Transient: every ring is leased right now. Retryable.
        AcquireError::Exhausted => ClientError::PoolExhausted,
    })?;
    let frame = match lease.try_send(frame) {
        Ok(()) => return Ok(()),
        Err(TrySendError::Closed(_)) => return Err(ClientError::ChannelClosed),
        Err(TrySendError::Full(f)) => f,
    };
    match tokio::time::timeout(max_block, lease.send_async(frame)).await {
        Ok(Ok(())) => Ok(()),
        // The value comes back only when the channel closed under us.
        Ok(Err(_)) => Err(ClientError::ChannelClosed),
        Err(_) => Err(ClientError::QueueFull),
    }
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
            &mut data[..size],
            seq,
            stream_id,
            0,
            0,
            subject,
            msg_id,
            payload,
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
    pool: &Arc<WritePool>,
    seq_alloc: &SeqAllocator,
    stream_id: u32,
    subject: &[u8],
    msg_id: &[u8],
    payload: Bytes,
) -> Result<(), ClientError> {
    let seq = seq_alloc.next();
    enqueue(
        pool,
        encode_pub_frame(seq, stream_id, subject, msg_id, &payload),
    )
}

/// Fire-and-forget batch publish.
///
/// Automatically chunks into sub-batches of [`PUBLISH_BATCH_MAX`] entries
/// when the caller provides more than 256 entries.
pub(crate) fn publish_batch_async(
    pool: &Arc<WritePool>,
    seq_alloc: &SeqAllocator,
    stream_id: u32,
    entries: &[BatchEntry<'_>],
) -> Result<(), ClientError> {
    for chunk in entries.chunks(PUBLISH_BATCH_MAX) {
        let seq = seq_alloc.next();
        enqueue(
            pool,
            WriteFrame::PubBatch(encode_pub_batch_v2(seq, stream_id, 0, chunk)),
        )?;
    }
    Ok(())
}

/// Sync publish — registers a Pending slot, sends, returns a `Send` future
/// that awaits the broker reply.
///
/// All synchronous work (encode, register, enqueue) is done before the
/// `async move` block so that no `&WritePool` / `&Pending` borrow
/// crosses the await point. Only the owned `rx` receiver (which is `Send`)
/// lives inside the returned future.
pub(crate) fn publish_wait_async(
    pool: &Arc<WritePool>,
    pending: &Arc<Pending>,
    seq_alloc: &SeqAllocator,
    max_block: std::time::Duration,
    stream_id: u32,
    subject: &[u8],
    msg_id: &[u8],
    payload: Bytes,
) -> impl Future<Output = Result<Bytes, ClientError>> + Send {
    let seq = seq_alloc.next();
    let frame = encode_pub_frame(seq, stream_id, subject, msg_id, &payload);
    let rx = pending.register(seq);
    let pool = Arc::clone(pool);
    let pending = Arc::clone(pending);
    async move {
        if let Err(e) = enqueue_wait(&pool, frame, max_block).await {
            pending.cancel(seq); // leak-guard: see F3
            return Err(e);
        }
        rx.recv_async()
            .await
            .map_err(|_| ClientError::ChannelClosed)
            .and_then(|r| r)
    }
}

/// Delayed publish — parks the message in the broker's delayed journal.
/// The message will be delivered to consumers after `delay_ms` milliseconds.
/// Returns a future that resolves once the broker confirms receipt.
pub(crate) fn publish_delayed_async(
    pool: &Arc<WritePool>,
    pending: &Arc<Pending>,
    seq_alloc: &SeqAllocator,
    max_block: std::time::Duration,
    stream_id: u32,
    subject: &[u8],
    payload: Bytes,
    delay_ms: u64,
) -> impl std::future::Future<Output = Result<Bytes, ClientError>> + Send {
    let seq = seq_alloc.next();
    let frame = WriteFrame::Mono(crate::transport::encode::encode_pub_delayed_v2(
        seq, stream_id, subject, &payload, delay_ms,
    ));
    let rx = pending.register(seq);
    let pool = Arc::clone(pool);
    let pending = Arc::clone(pending);
    async move {
        if let Err(e) = enqueue_wait(&pool, frame, max_block).await {
            pending.cancel(seq); // leak-guard: see F3
            return Err(e);
        }
        rx.recv_async()
            .await
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
    pool: &Arc<WritePool>,
    pending: &Arc<Pending>,
    seq_alloc: &SeqAllocator,
    max_block: std::time::Duration,
    stream_id: u32,
    subject: &[u8],
    reply_to: &[u8],
    msg_id: &[u8],
    payload: Bytes,
) -> impl Future<Output = Result<Bytes, ClientError>> + Send {
    let seq = seq_alloc.next();
    let frame = WriteFrame::Mono(encode_pub_with_reply_v2(
        seq, stream_id, subject, reply_to, msg_id, &payload,
    ));
    let rx = pending.register(seq);
    let pool = Arc::clone(pool);
    let pending = Arc::clone(pending);
    async move {
        if let Err(e) = enqueue_wait(&pool, frame, max_block).await {
            pending.cancel(seq); // leak-guard: see F3
            return Err(e);
        }
        rx.recv_async()
            .await
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
pub(crate) fn publish_batch_wait_async(
    pool: &Arc<WritePool>,
    pending: &Arc<Pending>,
    seq_alloc: &SeqAllocator,
    max_block: std::time::Duration,
    stream_id: u32,
    entries: &[BatchEntry<'_>],
) -> impl Future<Output = Result<Bytes, ClientError>> + Send {
    // Encode every chunk up front: `entries` borrows caller data that must
    // not outlive this call, while the enqueue below now awaits.
    let mut chunks = entries.chunks(PUBLISH_BATCH_MAX);
    let first_chunk = chunks.next().unwrap_or(&[]);
    let seq = seq_alloc.next();
    let mut frames = vec![WriteFrame::PubBatch(encode_pub_batch_v2(
        seq,
        stream_id,
        0,
        first_chunk,
    ))];
    for chunk in chunks {
        let chunk_seq = seq_alloc.next();
        frames.push(WriteFrame::PubBatch(encode_pub_batch_v2(
            chunk_seq, stream_id, 0, chunk,
        )));
    }

    let rx = pending.register(seq);
    let pool = Arc::clone(pool);
    let pending = Arc::clone(pending);

    async move {
        // The caller already agreed to await a reply, so parking on a full
        // ring is cheaper than failing: backpressure is transient.
        for frame in frames {
            if let Err(e) = enqueue_wait(&pool, frame, max_block).await {
                pending.cancel(seq); // leak-guard: see F3
                return Err(e);
            }
        }
        rx.recv_async()
            .await
            .map_err(|_| ClientError::ChannelClosed)
            .and_then(|r| r)
    }
}

// ── Publish with headers ─────────────────────────────────────────────────

/// Encode a PubFrame whose payload is a pre-encoded ExtendedPayload (user
/// payload + headers TLV). Sets `entry_flags = HAS_HEADERS` so the broker
/// stores it directly without re-wrapping.
///
/// If `headers` contains an entry with key `HDR_MSG_ID`, its value is also
/// placed in the frame's dedicated `msg_id` field for runtime idempotency.
#[inline]
fn encode_pub_frame_with_headers(
    seq: u64,
    stream_id: u32,
    subject: &[u8],
    headers: &[(&[u8], &[u8])],
    payload: &[u8],
) -> WriteFrame {
    let msg_id: &[u8] = headers
        .iter()
        .find(|(k, _)| *k == HDR_MSG_ID)
        .map(|(_, v)| *v)
        .unwrap_or(&[]);

    let ext_payload = encode_extended_payload_vec(payload, headers);

    let size = PubFrame::wire_size(subject.len(), msg_id.len(), ext_payload.len());
    if size <= INLINE_CAP {
        let mut data = [0u8; INLINE_CAP];
        PubFrame::encode_into(
            &mut data[..size],
            seq,
            stream_id,
            0,
            entry_flag::HAS_HEADERS,
            subject,
            msg_id,
            &ext_payload,
        );
        WriteFrame::Inline(data, size as u16)
    } else {
        let mut buf = vec![0u8; size];
        PubFrame::encode_into(
            &mut buf,
            seq,
            stream_id,
            0,
            entry_flag::HAS_HEADERS,
            subject,
            msg_id,
            &ext_payload,
        );
        WriteFrame::Mono(Bytes::from(buf))
    }
}

/// Sync publish with arbitrary headers. Awaits broker confirmation.
pub(crate) fn publish_with_headers_sync_async(
    pool: &Arc<WritePool>,
    pending: &Arc<Pending>,
    seq_alloc: &SeqAllocator,
    max_block: std::time::Duration,
    stream_id: u32,
    subject: &[u8],
    headers: &[(&[u8], &[u8])],
    payload: Bytes,
) -> impl Future<Output = Result<Bytes, ClientError>> + Send {
    let seq = seq_alloc.next();
    let frame = encode_pub_frame_with_headers(seq, stream_id, subject, headers, &payload);
    let rx = pending.register(seq);
    let pool = Arc::clone(pool);
    let pending = Arc::clone(pending);
    async move {
        if let Err(e) = enqueue_wait(&pool, frame, max_block).await {
            pending.cancel(seq); // leak-guard: see F3
            return Err(e);
        }
        rx.recv_async()
            .await
            .map_err(|_| ClientError::ChannelClosed)
            .and_then(|r| r)
    }
}

/// Encode a plain PubFrame for reply delivery (used by Message::reply).
#[inline]
pub(crate) fn encode_pub_frame_for_reply(
    seq: u64,
    stream_id: u32,
    subject: &[u8],
    payload: &[u8],
) -> WriteFrame {
    encode_pub_frame(seq, stream_id, subject, &[], payload)
}

/// Fire-and-forget publish with arbitrary headers.
#[inline]
#[allow(dead_code)]
pub(crate) fn publish_with_headers_async(
    pool: &Arc<WritePool>,
    seq_alloc: &SeqAllocator,
    stream_id: u32,
    subject: &[u8],
    headers: &[(&[u8], &[u8])],
    payload: Bytes,
) -> Result<(), ClientError> {
    let seq = seq_alloc.next();
    enqueue(
        pool,
        encode_pub_frame_with_headers(seq, stream_id, subject, headers, &payload),
    )
}
