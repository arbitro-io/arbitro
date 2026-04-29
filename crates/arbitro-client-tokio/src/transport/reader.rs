//! Reader task — `BytesMut + read_buf + split_to`. v2 framing.
//!
//! Decodes by `Header.action` and routes to the correct handler:
//! - `RepOk` / `RepError`           → resolve the matching `Pending` slot.
//! - `ListStreams` / `ListConsumers`→ resolve `Pending` with the body bytes
//!   (count + entries — caller parses).
//! - `Deliver`                       → push to the consume demux (TODO Step 5).
//! - everything else                 → log + drop.
//!
//! Pending is keyed by **request seq** (`Header.seq`). Reply bodies always
//! include the answer payload (e.g., RepOk body's `ref_seq` is the new
//! consumer/stream id, ListStreams body is `[count][entries…]`).

use bytes::{Bytes, BytesMut};
use tokio::io::AsyncReadExt;
use tokio::net::tcp::OwnedReadHalf;
use tokio_util::sync::CancellationToken;
use zerocopy::FromBytes;

use arbitro_proto::action::Action;
use arbitro_proto::v2::egress::rep_frame::RepErrFrame;
use arbitro_proto::v2::header::{Header, HEADER_SIZE};

use crate::error::ClientError;
use crate::state::pending::Pending;

/// Initial buffer capacity. Sized to hold a few small frames; grown on demand.
const READ_BUF_INITIAL: usize = 64 * 1024;

pub(crate) async fn reader_task(
    mut r: OwnedReadHalf,
    pending: std::sync::Arc<Pending>,
    cancel: CancellationToken,
) -> Result<(), ClientError> {
    let mut buf = BytesMut::with_capacity(READ_BUF_INITIAL);
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            res = r.read_buf(&mut buf) => {
                let n = res?;
                if n == 0 {
                    return Err(ClientError::Disconnected);
                }
                while buf.len() >= HEADER_SIZE {
                    let h = match Header::ref_from_bytes(&buf[..HEADER_SIZE]) {
                        Ok(h) => h,
                        Err(_) => return Err(ClientError::Disconnected),
                    };
                    let total = HEADER_SIZE + h.msg_len.get() as usize;
                    if buf.len() < total {
                        // Make sure we have room for the rest.
                        buf.reserve(total - buf.len());
                        break;
                    }
                    let frame = buf.split_to(total).freeze();
                    dispatch(&pending, frame);
                }
            }
        }
    }
}

#[inline]
fn dispatch(pending: &Pending, frame: Bytes) {
    // SAFETY: dispatch is called only after we verified `frame.len() >= HEADER_SIZE`.
    let h = match Header::ref_from_bytes(&frame[..HEADER_SIZE]) {
        Ok(h) => h,
        Err(_) => return,
    };
    let action = h.action.get();
    let req_seq = h.seq.get();
    let body = frame.slice(HEADER_SIZE..);

    // RepOk — wake pending with the body. Body's first 8 B are `ref_seq`
    // (the answer value: consumer_id, stream wire_id, etc.). Caller parses.
    if action == Action::RepOk.as_u16() {
        pending.complete_ok(req_seq, body);
        return;
    }

    // RepError — re-parse for the error code field.
    if action == Action::RepError.as_u16() {
        if let Ok(rep) = RepErrFrame::ref_from_bytes(&frame[..core::mem::size_of::<RepErrFrame>()]) {
            pending.complete_err(req_seq, rep.body.error_code.get());
        } else {
            pending.complete_err(req_seq, 0);
        }
        return;
    }

    // ListStreams / ListConsumers — server uses these action codes for the
    // *reply* (not RepOk) and the body is the variable-length payload.
    // Wake pending with the body so manage::list_* can decode it.
    if action == Action::ListStreams.as_u16() || action == Action::ListConsumers.as_u16() {
        pending.complete_ok(req_seq, body);
        return;
    }

    // Deliver / batch deliver / other actions: TODO consume demux (Step 5).
    let _ = action;
}
