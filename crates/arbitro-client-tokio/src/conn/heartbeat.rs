//! Heartbeat watchdog — sends v2 Ping frames and detects dead connections.
//!
//! v2 Ping/Pong are header-only (msg_len = 0).  Server replies immediately
//! with a Pong header.  If no Pong arrives within `cfg.timeout` the session
//! cancellation token is fired, which tears down writer and reader siblings.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio_util::sync::CancellationToken;

use arbitro_proto::action::Action;
use arbitro_proto::v2::header::HEADER_SIZE;

use crate::config::KeepAlive;
use crate::error::ClientError;
use crate::state::Inner;
use crate::transport::frame::{WriteFrame, INLINE_CAP};

#[inline]
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Heartbeat watchdog task.
///
/// - Sleeps `cfg.interval` (30 s by default).
/// - Checks whether the last received `Pong` is older than `cfg.timeout` (60 s).
/// - If stale → cancels the session → returns `Err(Disconnected)`.
/// - Otherwise → sends a `Ping` header (16 B inline frame, zero heap alloc).
pub(crate) async fn heartbeat_task(
    inner:  Arc<Inner>,
    cfg:    KeepAlive,
    cancel: CancellationToken,
) -> Result<(), ClientError> {
    // Initialise last_pong to now so we don't time-out during handshake.
    inner.last_pong_ns.store(now_ns(), Ordering::Relaxed);

    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            _ = tokio::time::sleep(cfg.interval) => {
                // ── 1. Check liveness ──────────────────────────────────
                let age_ns = now_ns()
                    .saturating_sub(inner.last_pong_ns.load(Ordering::Relaxed));
                let timeout_ns = cfg.timeout.as_nanos() as u64;

                if age_ns > timeout_ns {
                    cancel.cancel();   // kill writer + reader siblings
                    return Err(ClientError::Disconnected);
                }

                // ── 2. Send Ping (header-only, 16 B) ──────────────────
                // Ping = Header { action=0x0601, flags=0, entry_flags=0,
                //                 msg_len=0, seq=0 }
                // Write action as little-endian u16 at bytes [0..2];
                // all other header fields remain zero.
                let action_le = Action::Ping.as_u16().to_le_bytes();
                let mut data = [0u8; INLINE_CAP];
                data[0] = action_le[0];
                data[1] = action_le[1];
                let frame = WriteFrame::Inline(data, HEADER_SIZE as u16);

                let _ = inner.admin_producer.lock().unwrap().try_send(frame);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KeepAlive;

    /// Heartbeat exits cleanly when the cancel token fires.
    #[tokio::test]
    async fn heartbeat_exits_on_cancel() {
        // Use a very long interval so the sleep never fires in the test.
        let cfg = KeepAlive {
            interval: std::time::Duration::from_secs(3600),
            timeout:  std::time::Duration::from_secs(7200),
        };
        let cancel = CancellationToken::new();
        cancel.cancel();   // pre-cancel

        // Build a minimal Inner just to satisfy the signature.
        // We use a real ring so try_send doesn't panic.
        use arbitro_kit::route::MpscAsync;
        use crate::transport::frame::{WriteFrame, WRITE_QUEUE_CAP, MAX_WRITE_PRODUCERS};
        use crate::state::{pending::Pending, seq::SeqAllocator, subscriptions::Subscriptions};
        use crate::config::ClientConfig;
        use std::sync::Mutex;

        let (mut producers, _consumer, _shutdown) =
            MpscAsync::<WriteFrame, WRITE_QUEUE_CAP>::new(MAX_WRITE_PRODUCERS);
        let admin = producers.remove(0);
        let (ack_tx,  _ack_rx)  = tokio::sync::mpsc::channel(4);
        let (nack_tx, _nack_rx) = tokio::sync::mpsc::channel(4);
        let inner = Arc::new(Inner {
            cfg:            ClientConfig::default(),
            producer_pool:  Mutex::new(producers),
            pending:        Arc::new(Pending::new()),
            seq_alloc:      SeqAllocator::new(),
            cancel:         cancel.clone(),
            subscriptions:  Arc::new(Subscriptions::new()),
            admin_producer: Mutex::new(admin),
            ack_tx,
            nack_tx,
            last_pong_ns:   std::sync::atomic::AtomicU64::new(0),
        });

        // Should return immediately because cancel is already fired.
        let result = heartbeat_task(inner, cfg, cancel).await;
        assert!(result.is_ok());
    }
}
