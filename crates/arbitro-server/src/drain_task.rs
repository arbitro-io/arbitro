//! Drain task — async loop that delivers messages for a single stream.
//!
//! One task per stream. Waits on Gate (Notify), takes shard lock,
//! calls deliver_cycle, releases lock. Repeat.
//!
//! The engine never calls deliver_cycle — only signal.release().
//! This task is the ONLY caller of deliver_cycle in production.

use std::sync::Arc;

use arbitro_engine::transport::Transport;
use arbitro_engine::stream::StreamMap;

use crate::gate::Gate;

/// Spawn a drain task for a stream. Returns a JoinHandle.
pub fn spawn_drain_task(
    stream_id: u32,
    gate: Arc<Gate>,
    streams: Arc<StreamMap>,
    transport: Arc<dyn Transport>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            gate.wait().await;

            let transport_ref = transport.as_ref();
            let now_ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            // Drain until no progress. Release lock between cycles
            // so publish can append concurrently.
            loop {
                let progress = streams.with_mut(stream_id, |slot| {
                    slot.drain.deliver_cycle(&*slot.store, transport_ref, now_ts)
                });

                match progress {
                    None => return,        // Stream deleted, exit task
                    Some(false) => break,  // No more work
                    Some(true) => {}       // More work, loop
                }
            }
        }
    })
}
