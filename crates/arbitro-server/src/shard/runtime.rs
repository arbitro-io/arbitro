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

use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

/// A task to be built and run on the shard's thread, handed the shard.
///
/// The closure is `Send` — it is only a recipe. What it builds is not, and
/// must not be: it belongs to the shard.
///
/// This channel crosses a REAL ownership boundary. The acceptor and the
/// shard are different owners, and a connection arriving later cannot be
/// handed over any other way. That is the one case message passing is for.
type LocalJob = Box<dyn FnOnce(Rc<super::shard::Shard>) -> Pin<Box<dyn Future<Output = ()>>> + Send>;

/// A shard's private runtime: a `current_thread` tokio runtime driven by
/// one dedicated OS thread.
pub(crate) struct ShardRuntime {
    handle: tokio::runtime::Handle,
    /// Hands later work to the shard's thread. See [`LocalJob`].
    local_tx: tokio::sync::mpsc::UnboundedSender<LocalJob>,
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
    /// `store` is MOVED onto the new thread and installed there. That move
    /// is the whole ownership story: after it, no other thread holds the
    /// journal, which is why nothing has to lock it.
    ///
    /// The caller blocks until the install has happened. Without that,
    /// tasks spawned onto the handle could run before the journal exists
    /// and see "this thread owns no shard" — indistinguishable from a
    /// misrouted connection, and intermittent.
    /// The `Shard` is created HERE, by the thing that owns the thread, and
    /// handed to `main` and to every task accepted later. Nothing it holds
    /// has to be `Send`, because nothing it holds ever leaves this thread.
    pub(crate) fn start<F, Fut>(
        id: usize,
        store: Box<dyn arbitro_store::Store>,
        main: F,
    ) -> std::io::Result<Self>
    where
        F: FnOnce(Rc<super::shard::Shard>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let handle = rt.handle().clone();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let stop = Arc::clone(&shutdown);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<()>(0);
        let (local_tx, mut local_rx) = tokio::sync::mpsc::unbounded_channel::<LocalJob>();

        let thread = std::thread::Builder::new()
            .name(format!("arbitro-shard-{id}"))
            .spawn(move || {
                let shard = super::shard::Shard::new(id, store);
                let _ = ready_tx.send(());
                // A `LocalSet` so this thread can also run tasks that hold
                // what the shard owns. `Handle::spawn` requires the whole
                // future to be `Send`, which would force every such field
                // into a lookup somewhere else.
                let local = tokio::task::LocalSet::new();
                local.block_on(&rt, async move {
                    tokio::task::spawn_local(main(Rc::clone(&shard)));
                    loop {
                        tokio::select! {
                            job = local_rx.recv() => match job {
                                Some(job) => {
                                    tokio::task::spawn_local(job(Rc::clone(&shard)));
                                }
                                None => break,
                            },
                            // Keeps the runtime alive and polling until
                            // shutdown.
                            _ = stop.notified() => break,
                        }
                    }
                });
            })?;
        // A dropped sender means the thread died before installing; treat
        // that as the fatal condition it is rather than returning a runtime
        // whose journal never arrives.
        ready_rx
            .recv()
            .expect("shard runtime thread died before installing its journal");

        Ok(Self {
            handle,
            local_tx,
            shutdown,
            thread: Some(thread),
        })
    }

    pub(crate) fn handle(&self) -> &tokio::runtime::Handle {
        &self.handle
    }

    /// Hand work to this shard, to be built and run on its thread with the
    /// shard in hand. For connections, which arrive after startup.
    pub(crate) fn spawn_local<F, Fut>(&self, build: F)
    where
        F: FnOnce(Rc<super::shard::Shard>) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + 'static,
    {
        let _ = self
            .local_tx
            .send(Box::new(move |shard| Box::pin(build(shard))));
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
