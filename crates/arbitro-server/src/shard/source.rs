//! The drain's half of the layering: hand it a window, without saying how
//! the window was obtained.
//!
//! Lives inside `shard` rather than next to `StreamSink` because `Staged`,
//! `Window` and `DrainConfig` are shard-internal. `StreamSink` crosses
//! layers — transport uses it — and this one does not.

use crate::shard::drain::{self, DrainConfig, Staged, Window};
use crate::shard::router::SharedStore;
use crate::shard::shared::SharedCounters;

/// The drain's side of the same idea: hand it a window, without saying how
/// the window was obtained.
///
/// This one is worth more than `StreamSink`, because hiding the lock lets
/// the WORK change, not just the wrapper. Today the window has to be copied
/// out — `Entry<'a>` borrows the store arena and dies with the guard, and
/// `evict_expired` can `munmap` a segment from the command thread at any
/// moment. A drain that OWNS its store has no such race: nothing can unmap
/// underneath it, so the same window can be held by reference instead of
/// copied. At 8KB payloads that copy is ~82us per cycle.
///
/// Both shapes leave the drain loop identical.
pub(in crate::shard) trait WindowSource {
    /// Decide this cycle's window and make it available through `staged`.
    ///
    /// Whether that means taking a lock, copying bytes, or neither is the
    /// implementation's business.
    fn take_window(
        &self,
        counters: &SharedCounters,
        cfg: &DrainConfig,
        staged: &mut Staged,
    ) -> Window;
}

/// Today's model: the store is shared, so the window is read under the lock
/// and copied out before releasing it.
pub(in crate::shard) struct SharedStoreSource<'a> {
    store: &'a SharedStore,
}

impl<'a> SharedStoreSource<'a> {
    #[inline]
    pub(in crate::shard) fn new(store: &'a SharedStore) -> Self {
        Self { store }
    }
}

impl WindowSource for SharedStoreSource<'_> {
    #[inline]
    fn take_window(
        &self,
        counters: &SharedCounters,
        cfg: &DrainConfig,
        staged: &mut Staged,
    ) -> Window {
        let g = self.store.lock();
        let w = drain::window(counters, &**g, cfg);
        if let Window::Range { start, end, .. } = w {
            let _p = crate::shard::drain_profile::fill();
            staged.fill(&**g, start, end);
        }
        w
    }
}
