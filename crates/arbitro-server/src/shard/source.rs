//! The drain's half of the layering: hand it a window, without saying how
//! the window was obtained.
//!
//! Lives inside `shard` rather than next to `StreamSink` because `Staged`,
//! `Window` and `DrainConfig` are shard-internal. `StreamSink` crosses
//! layers — transport uses it — and this one does not.

use crate::shard::drain::{self, DrainConfig, Staged, Window};

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

/// The journal owned by this thread. No lock — the drain, the command
/// worker and this shard's connections are all tasks on one thread.
///
/// The copy into `staged` is still here, and now for a weaker reason than
/// before. It used to be mandatory: `evict_expired` could `munmap` a
/// segment from another thread mid-walk. Eviction is a task on THIS thread
/// now, so it cannot run while this borrow is held, and the window could be
/// walked in place. Removing the copy is a separate change with its own
/// measurement — the doc above puts it at ~82us per cycle at 8KB payloads
/// — and doing it in the same pass as the ownership move would make a
/// regression in either impossible to attribute.
pub(in crate::shard) struct LocalSource {
    shard_id: usize,
}

impl LocalSource {
    #[inline]
    pub(in crate::shard) fn new(shard_id: usize) -> Self {
        Self { shard_id }
    }
}

impl WindowSource for LocalSource {
    #[inline]
    fn take_window(
        &self,
        counters: &SharedCounters,
        cfg: &DrainConfig,
        staged: &mut Staged,
    ) -> Window {
        crate::shard::local::store(self.shard_id, |s| {
            let w = drain::window(counters, s, cfg);
            if let Window::Range { start, end, .. } = w {
                let _p = crate::shard::drain_profile::fill();
                staged.fill(s, start, end);
            }
            w
        })
    }
}
