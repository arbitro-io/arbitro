//! Drain task — async loop that delivers messages for a shard's pull consumers.
//!
//! One task per shard. Waits on Gate (Notify), sends DrainDeliver commands to
//! the shard via ShardHandle. The shard does the actual claim + store.get +
//! deliver via ConnectionRegistry — all on the shard thread (no async I/O).
//!
//! The shard signals `gate.release()` after publish queues messages.

use tokio::sync::watch;

use crate::gate::Gate;
use crate::handle::ShardHandle;

/// Spawn a drain task for a shard. Returns a JoinHandle.
pub fn spawn_drain_task(
    shard: ShardHandle,
    gate: Gate,
    shutdown_rx: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(drain_loop(shard, gate, shutdown_rx))
}

/// The drain loop — waits for gate signals, triggers delivery on the shard.
async fn drain_loop(
    shard: ShardHandle,
    gate: Gate,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = gate.wait() => {
                // Fire & forget — shard handles claim + store read + deliver
                if shard.drain_deliver().await.is_err() {
                    break; // shard worker exited
                }
            }
            _ = shutdown_rx.changed() => break,
        }
    }
}
