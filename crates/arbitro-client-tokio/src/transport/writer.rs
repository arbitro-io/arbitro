//! Single-writer transport task — owns `OwnedWriteHalf`, drains the
//! tokio mpsc receiver, uses `write_vectored` for `PubSingle`.
//!
//! ## Why tokio mpsc (not kit::Mpsc) for the writer queue
//!
//! kit::MpscProducer is `!Sync` (Cell-based ring index), which would
//! force every public publish call site through a `Mutex<MpscProducer>`.
//! tokio's `mpsc::Sender<T>` is `Send + Sync` and has the same single-
//! drain contract; bench previously showed mpsc contention is **not**
//! the latency bottleneck on the publish path. We still keep
//! `kit::OneShotAsync` for per-request reply correlation (the high-
//! contention slot), where kit is measurably faster.

use std::io::{self, IoSlice};

use bytes::Bytes;
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::error::ClientError;
use crate::transport::frame::WriteFrame;

/// Run the writer until the channel closes, the cancel token fires, or
/// IO fails. Returns `Ok(())` on graceful shutdown.
pub(crate) async fn writer_task(
    mut rx: mpsc::Receiver<WriteFrame>,
    mut w: OwnedWriteHalf,
    cancel: CancellationToken,
) -> Result<(), ClientError> {
    // Reuse a buffer for batch drain to avoid per-iteration allocs.
    let mut batch: Vec<WriteFrame> = Vec::with_capacity(64);
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            n = rx.recv_many(&mut batch, 64) => {
                if n == 0 {
                    // Channel closed.
                    let _ = w.shutdown().await;
                    return Ok(());
                }
                for frame in batch.drain(..) {
                    write_frame(&mut w, frame).await?;
                }
            }
        }
    }
}

#[inline]
async fn write_frame(w: &mut OwnedWriteHalf, f: WriteFrame) -> Result<(), ClientError> {
    match f {
        WriteFrame::Mono(b) => {
            w.write_all(&b).await?;
        }
        WriteFrame::PubSingle { prefix, subject, payload } => {
            write_vectored_3(w, &prefix, &subject, &payload).await?;
        }
        WriteFrame::PubBatch(b) => {
            w.write_all(&b).await?;
        }
    }
    Ok(())
}

/// Writev-style helper for the 3-iovec PUB single. Loops until every
/// byte is flushed, advancing across iovecs as the kernel partial-writes.
async fn write_vectored_3(
    w: &mut OwnedWriteHalf,
    a: &Bytes,
    b: &Bytes,
    c: &Bytes,
) -> io::Result<()> {
    // Snapshot lengths and stage iovecs. We slide a `consumed` cursor
    // forward; each pass rebuilds the iovec list from the cursor.
    let parts: [&[u8]; 3] = [a, b, c];
    let total: usize = parts.iter().map(|p| p.len()).sum();
    if total == 0 {
        return Ok(());
    }
    let mut consumed: usize = 0;
    while consumed < total {
        // Build iovecs starting from `consumed`.
        let mut bufs: [IoSlice<'_>; 3] = [IoSlice::new(&[]); 3];
        let mut n_bufs = 0usize;
        let mut offset = consumed;
        for p in parts.iter() {
            if offset >= p.len() {
                offset -= p.len();
                continue;
            }
            bufs[n_bufs] = IoSlice::new(&p[offset..]);
            n_bufs += 1;
            offset = 0;
        }
        // Wait for writability and try a vectored write. `try_write_vectored`
        // is non-blocking; we re-loop if the kernel wrote partially.
        w.writable().await?;
        match w.try_write_vectored(&bufs[..n_bufs]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "writev returned 0",
                ));
            }
            Ok(n) => consumed += n,
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
