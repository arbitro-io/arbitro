//! Diagnostic event recorder — DISPOSABLE.
//!
//! One-shot lifecycle profiler used by `tests/lifecycle_timings.rs` to measure
//! every step a single message takes through the server, from TCP read to
//! TCP write (publish path), then from drainer wakeup to TCP write (deliver
//! path), and finally the ack flow.
//!
//! ## Cost when disabled
//! `record()` does a single `Relaxed` atomic load. ~1 ns. No allocations.
//!
//! ## Cost when enabled
//! `record()` takes a global `Mutex<Vec<Event>>` and pushes one `Event`.
//! Only used in tests — never in production benches.
//!
//! ## To remove
//! 1. Delete this file
//! 2. `git grep "lifecycle_trace::record" | xargs sed -i '/lifecycle_trace::record/d'`
//! 3. Remove `pub mod lifecycle_trace;` from `lib.rs`
//! 4. Delete `tests/lifecycle_timings.rs`

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

static ENABLED: AtomicBool = AtomicBool::new(false);
static EVENTS: Mutex<Vec<Event>> = Mutex::new(Vec::new());

#[derive(Clone, Debug)]
pub struct Event {
    pub label: &'static str,
    pub conn_id: u64,
    pub seq: u64,
    pub thread: &'static str,
    pub at: Instant,
}

/// Enable recording. After this call, every `record()` will push an event.
pub fn enable() {
    EVENTS.lock().unwrap().clear();
    ENABLED.store(true, Ordering::Release);
}

/// Disable recording. Subsequent `record()` calls are no-ops.
pub fn disable() {
    ENABLED.store(false, Ordering::Release);
}

/// Drain all recorded events. Returns them sorted by timestamp.
pub fn take() -> Vec<Event> {
    let mut events = std::mem::take(&mut *EVENTS.lock().unwrap());
    events.sort_by_key(|e| e.at);
    events
}

/// Record an event if recording is enabled.
///
/// `conn_id` and `seq` are correlation hints — pass `0` if unknown.
/// `thread` should be a `'static` name (`"transport"`, `"shard"`, `"writer"`).
#[inline]
pub fn record(label: &'static str, conn_id: u64, seq: u64, thread: &'static str) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    let event = Event {
        label,
        conn_id,
        seq,
        thread,
        at: Instant::now(),
    };
    if let Ok(mut events) = EVENTS.lock() {
        events.push(event);
    }
}
