//! One shard. One owner.
//!
//! Everything a shard owns is a field here. Its tasks — the command loop,
//! the drain, every connection accepted on it — hold an `Rc<Shard>` and
//! reach those fields directly.
//!
//! That is not sharing between owners: it is one owner with several
//! entry points. `RefCell` orders them, which is sound because they are
//! tasks, and a task is not an ownership boundary.
//!
//! Message passing belongs at the boundary between shards, and nowhere
//! inside one.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use arbitro_store::Store;

use super::dedup::Dedup;

pub(crate) struct Shard {
    /// Which shard this is. A field, because the shard knows who it is —
    /// used to label logs and metrics and to answer a router, never to
    /// look up something it already owns.
    pub(crate) shard_id: usize,

    /// The journal. Moved here when the shard's thread starts and never
    /// leaves, which is why nothing locks it.
    store: RefCell<Box<dyn Store>>,

    /// Message-id admission.
    pub(crate) dedup: Dedup,

    /// Sockets this shard may write to directly, by connection id.
    ///
    /// A socket has two writers: the connection's own task, replying to a
    /// publish, and the drain, delivering messages. The `RefCell` is what
    /// orders them.
    egress: RefCell<HashMap<u64, crate::transport::egress::DirectEgress>>,

    /// A direct write happened and its socket has not been flushed yet.
    owes_flush: Cell<bool>,

    /// The command loop's state, reachable while the loop is parked.
    ///
    /// The loop puts it here around its `await` and takes it back on wake,
    /// so it is available exactly when a connection on this shard would
    /// want it — which is what lets an ack run straight through instead of
    /// queueing behind the loop.
    ///
    /// Ownership MOVES both ways. The worker holds an `Rc<Shard>`, so
    /// leaving it parked here forever would be a cycle and the journal
    /// would never close; taking it out on every wake is what breaks that.
    worker: RefCell<Option<Box<super::worker::CommandWorker>>>,
}

impl Shard {
    pub(crate) fn new(shard_id: usize, store: Box<dyn Store>) -> Rc<Self> {
        let scheduler = Rc::new(RefCell::new(arbitro_kit::scheduler::Scheduler::new(
            super::dedup::TICK_MS,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        )));
        Rc::new(Self {
            shard_id,
            store: RefCell::new(store),
            dedup: Dedup::new(scheduler),
            egress: RefCell::new(HashMap::new()),
            owes_flush: Cell::new(false),
            worker: RefCell::new(None),
        })
    }

    /// Park the command loop's state here while it awaits.
    ///
    /// Panics on a second park: two live workers on one shard would each
    /// think they own the engine.
    #[inline]
    pub(crate) fn park_worker(&self, w: Box<super::worker::CommandWorker>) {
        let mut slot = self.worker.borrow_mut();
        assert!(slot.is_none(), "this shard already has a parked worker");
        *slot = Some(w);
    }

    /// Take it back. `None` if it is not parked.
    #[inline]
    pub(crate) fn take_worker(&self) -> Option<Box<super::worker::CommandWorker>> {
        self.worker.borrow_mut().take()
    }

    /// Run `f` against the parked worker. `None` when the loop holds it —
    /// which is what makes the caller fall back to the queued path instead
    /// of waiting on state that is busy.
    ///
    /// **Never hold this across an `.await`.**
    #[inline]
    pub(crate) fn with_worker<R>(
        &self,
        f: impl FnOnce(&mut super::worker::CommandWorker) -> R,
    ) -> Option<R> {
        let mut slot = self.worker.borrow_mut();
        slot.as_mut().map(|w| f(w))
    }

    /// Drop whatever is parked, so a restarted shard starts clean.
    #[inline]
    pub(crate) fn clear_worker(&self) {
        *self.worker.borrow_mut() = None;
    }

    /// Run `f` against the journal.
    ///
    /// **Never hold this across an `.await`.** Tasks yield there, and a
    /// second one entering would panic — correctly, because it means two
    /// tasks interleaved inside the journal.
    #[inline]
    pub(crate) fn store<R>(&self, f: impl FnOnce(&mut dyn Store) -> R) -> R {
        let mut store = self.store.borrow_mut();
        f(&mut **store)
    }

    #[inline]
    pub(crate) fn install_egress(&self, conn_id: u64, e: crate::transport::egress::DirectEgress) {
        self.egress.borrow_mut().insert(conn_id, e);
    }

    #[inline]
    pub(crate) fn remove_egress(&self, conn_id: u64) {
        self.egress.borrow_mut().remove(&conn_id);
    }

    /// Run `f` against one connection's socket. `None` if this shard does
    /// not hold it — a connection accepted elsewhere.
    #[inline]
    pub(crate) fn with_egress<R>(
        &self,
        conn_id: u64,
        f: impl FnOnce(&mut crate::transport::egress::DirectEgress) -> R,
    ) -> Option<R> {
        let mut map = self.egress.borrow_mut();
        map.get_mut(&conn_id).map(f)
    }

    /// A direct write landed; its socket still owes a flush.
    #[inline]
    pub(crate) fn mark_owes(&self) {
        self.owes_flush.set(true);
    }

    /// Take the debt. `true` if there was one.
    #[inline]
    pub(crate) fn flush_owed(&self) -> bool {
        self.owes_flush.replace(false)
    }
}
