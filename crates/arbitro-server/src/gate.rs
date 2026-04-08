//! Gate — async drain signal backed by tokio::sync::Notify.
//!
//! The shard calls `release()` (sync, O(1)) after publish queues messages.
//! The drain task awaits `wait()` (async) to know when work is available.

use std::sync::Arc;
use tokio::sync::Notify;

/// Async drain gate — bridges sync shard signals to async drain tasks.
///
/// Clone-friendly (Arc<Notify> inside). Safe to call `release()` from any
/// thread — sync or async.
#[derive(Clone)]
pub struct Gate {
    notify: Arc<Notify>,
}

impl Default for Gate {
    fn default() -> Self {
        Self::new()
    }
}

impl Gate {
    pub fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
        }
    }

    /// Signal the drain task — wake it up. Non-blocking, O(1).
    /// Safe from sync shard thread or async context.
    #[inline]
    pub fn release(&self) {
        self.notify.notify_one();
    }

    /// Wait for a signal. Called by the drain task (async).
    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}
