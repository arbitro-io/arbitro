//! Gate — async DrainSignal backed by tokio::sync::Notify.
//!
//! The engine calls `release()` (sync, O(1)) after append/ack/nack.
//! The drain task awaits `wait()` (async) to know when work is available.

use std::sync::Arc;
use tokio::sync::Notify;

use arbitro_engine::DrainSignal;

/// Async drain gate — bridges sync engine signals to async drain tasks.
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

    /// Wait for a signal. Called by the drain task.
    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}

impl DrainSignal for Gate {
    #[inline]
    fn release(&self) {
        self.notify.notify_one();
    }
}
