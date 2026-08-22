//! The shard's journal, owned by the shard's thread. No lock.
//!
//! ## Why there is no `Mutex` here
//!
//! A lock exists to make concurrent access safe. Under this model there is
//! no concurrent access to make safe: a shard's listener only accepts
//! connections that belong to that shard, those connections run on that
//! shard's `current_thread` runtime, and so do its drain and command
//! worker. Everything that touches the journal is a task on one thread,
//! and tasks on one thread do not run simultaneously.
//!
//! `RefCell` and not `Mutex` because the invariant being enforced is
//! single-threaded aliasing, and that is exactly what `RefCell` checks. It
//! is also the honest choice: if this is ever reached from a second thread
//! the code will not compile, whereas a `Mutex` would silently make it
//! "work" and quietly reintroduce the contention the model exists to
//! remove.
//!
//! ## The one rule
//!
//! **Never hold the borrow across an `.await`.** Tasks on one thread yield
//! at await points, so a borrow held across one lets a second task on the
//! same thread observe an outstanding borrow and panic. Every access here
//! is a straight-line call, and callers should keep it that way. A panic
//! from this `RefCell` is a real bug, not noise — it means two tasks on the
//! shard interleaved inside the journal.
//!
//! ## Ownership is exclusive, and decided at boot
//!
//! A shard's journal lives EITHER here or in the router's shared slot,
//! never both. Two live handles to one journal would be a far worse bug
//! than the lock this removes: appends through one would be invisible to
//! the cursor kept by the other, which is the "stored but never delivered"
//! shape all over again. `install` therefore refuses to overwrite.

use std::cell::RefCell;

use arbitro_store::Store;

thread_local! {
    /// The journal owned by THIS thread, if it is a shard runtime thread.
    /// `None` on the shared pool, on the accept loop, and in tests — which
    /// is what lets both models coexist while the shared path is retired.
    static LOCAL: RefCell<Option<LocalShard>> = const { RefCell::new(None) };
}

/// A shard's thread-owned state.
pub(crate) struct LocalShard {
    pub(crate) shard_id: usize,
    pub(crate) store: RefCell<Box<dyn Store>>,
}

/// Give this thread its shard's journal. Called once, from the shard's
/// runtime thread, before any task on it runs.
///
/// Panics on a second install: silently replacing a live journal would
/// orphan every cursor pointing at the first one.
pub(crate) fn install(shard_id: usize, store: Box<dyn Store>) {
    LOCAL.with(|l| {
        let mut slot = l.borrow_mut();
        assert!(
            slot.is_none(),
            "shard {shard_id}: a journal is already installed on this thread"
        );
        *slot = Some(LocalShard {
            shard_id,
            store: RefCell::new(store),
        });
    });
}

/// Run `f` against this thread's journal, if it owns one for `shard_id`.
///
/// Returns `None` when this thread owns no journal (shared mode, or a
/// connection on the bootstrap port) or owns a DIFFERENT shard's. The
/// second case is the important one: it means a connection reached for a
/// journal that is not its own, which under this model is a routing
/// mistake, not something to paper over by taking a lock.
pub(crate) fn with_store<R>(shard_id: usize, f: impl FnOnce(&mut dyn Store) -> R) -> Option<R> {
    LOCAL.with(|l| {
        let slot = l.borrow();
        let local = slot.as_ref()?;
        if local.shard_id != shard_id {
            return None;
        }
        let mut store = local.store.borrow_mut();
        Some(f(&mut **store))
    })
}

/// Whether this thread owns `shard_id`'s journal. For the publish path to
/// decide, once, which door it is going through.
pub(crate) fn owns(shard_id: usize) -> bool {
    LOCAL.with(|l| {
        l.borrow()
            .as_ref()
            .is_some_and(|local| local.shard_id == shard_id)
    })
}
