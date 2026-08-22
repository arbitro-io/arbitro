//! One single-threaded runtime per shard, each on its own OS thread.
//!
//! ## Why, with the number
//!
//! A shard's store is reached today from three places at once: the drain
//! task, the command worker, and every connection thread that publishes.
//! Three concurrent owners means the `Mutex` is genuinely contended, and
//! the contention grows with the shard count.
//!
//! Measured on the faithful data path (`arbitro-experiment/shardbench`,
//! 24-core WSL, median of 3 × 1.2 s):
//!
//! | shards | share-nothing | shared mutex | channel hand-off |
//! |--------|---------------|--------------|------------------|
//! | 4      | 1.41 M ops/s  | 1.36 M       | 0.96 M           |
//! | 8      | 2.35 M        | 1.92 M       | 1.25 M           |
//! | 16     | 3.16 M        | 2.46 M       | 1.73 M           |
//!
//! Two things to read off that table, because both shaped this module.
//!
//! **The prize scales with shard count** — 4% at 4 shards, 22% at 8, 28%
//! at 16. At small shard counts there is barely any contention to remove,
//! which is why an earlier attempt at this concluded "no measurable gain":
//! it was measured where the gain does not exist.
//!
//! **Routing the publish through a channel is WORSE than the mutex** at
//! every size (−29%, −35%, −30%). So this module does NOT move publishes
//! across a channel. The store keeps its lock; what changes is that the
//! shard's own tasks stop fighting each other for it. The remaining win —
//! getting the publishing thread to BE this thread — comes from the
//! per-shard listeners, not from here.
//!
//! ## What this does not do
//!
//! It does not make the store single-owner. A connection accepted on the
//! bootstrap port still publishes from a runtime worker thread, and the
//! lock is what keeps that correct. Removing the lock would require every
//! publish to originate on the owning thread, which is a client-steering
//! property, not a server one.

use std::sync::Arc;

/// A shard's private runtime: a `current_thread` tokio runtime driven by
/// one dedicated OS thread.
pub(crate) struct ShardRuntime {
    handle: tokio::runtime::Handle,
    /// Dropping this stops the runtime's `block_on`, which lets the thread
    /// finish. Held so the runtime outlives the tasks spawned onto it.
    shutdown: Arc<tokio::sync::Notify>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl ShardRuntime {
    /// Start a runtime on its own thread, named `arbitro-shard-{id}` so it
    /// is identifiable in `top`/`perf` — an unnamed thread per shard makes
    /// a 16-shard broker unreadable in exactly the profile you would open
    /// to check whether this was worth it.
    pub(crate) fn start(id: usize) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let handle = rt.handle().clone();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let stop = Arc::clone(&shutdown);

        let thread = std::thread::Builder::new()
            .name(format!("arbitro-shard-{id}"))
            .spawn(move || {
                // `block_on` drives every task spawned through the handle,
                // not just this future. Parking on the notify is what keeps
                // the runtime alive and polling until shutdown.
                rt.block_on(async move { stop.notified().await });
            })?;

        Ok(Self {
            handle,
            shutdown,
            thread: Some(thread),
        })
    }

    pub(crate) fn handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }
}

impl Drop for ShardRuntime {
    fn drop(&mut self) {
        self.shutdown.notify_waiters();
        if let Some(t) = self.thread.take() {
            // Not joined: a shard task that refuses to finish would turn
            // shutdown into a hang, and the shard's own `running` flag plus
            // its awaited JoinHandles are what actually sequence teardown.
            // Dropping the handle detaches.
            drop(t);
        }
    }
}
