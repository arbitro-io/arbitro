//! Diagnostic event recorder — compile-time gated via the `lifecycle_trace`
//! feature.
//!
//! One-shot lifecycle profiler used to measure every step a single message
//! takes through the server, from TCP read to TCP write (publish path), then
//! from drainer wakeup to TCP write (deliver path), and finally the ack flow.
//!
//! ## Enabling
//! Build or test with `--features lifecycle_trace`.
//!
//! ## Cost model
//! The public entry point is the `lifecycle_trace!` macro (not a function).
//! This is critical: **the macro guarantees that call-site argument
//! expressions are never evaluated when the feature is off**, because they
//! are stripped from the token stream at macro expansion. Contrast with a
//! plain function, where arguments are evaluated *before* the call and only
//! eliminated by the optimizer's dead-code elimination — which can fail for
//! opaque expressions (HashMap lookups, method calls with side effects,
//! cross-crate calls).
//!
//! With the feature **off**: `lifecycle_trace!(...)` expands to `()`.
//! Zero instructions in the final binary. The argument expressions literally
//! do not exist in the compiled code.
//!
//! With the feature **on**: `lifecycle_trace!(...)` expands to a call to
//! `__record`, which checks a runtime `AtomicBool` gate. Enable recording by
//! calling `lifecycle_trace::enable()` at the start of a diagnostic session.

// ── Feature ON: real implementation ────────────────────────────────────────
#[cfg(feature = "lifecycle_trace")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "lifecycle_trace")]
use std::sync::Mutex;
#[cfg(feature = "lifecycle_trace")]
use std::time::Instant;

#[cfg(feature = "lifecycle_trace")]
static ENABLED: AtomicBool = AtomicBool::new(false);

#[cfg(feature = "lifecycle_trace")]
static EVENTS: Mutex<Vec<Event>> = Mutex::new(Vec::new());

#[cfg(feature = "lifecycle_trace")]
#[derive(Clone, Debug)]
pub struct Event {
    pub label: &'static str,
    pub conn_id: u64,
    pub seq: u64,
    pub thread: &'static str,
    pub at: Instant,
}

/// Enable recording. After this call, every `lifecycle_trace!` will push an
/// event until `disable()` is called.
#[cfg(feature = "lifecycle_trace")]
pub fn enable() {
    EVENTS.lock().unwrap().clear();
    ENABLED.store(true, Ordering::Release);
}

/// Disable recording. Subsequent `lifecycle_trace!` calls still expand to a
/// function call, but the function returns immediately after the atomic
/// gate check (~1 ns).
#[cfg(feature = "lifecycle_trace")]
pub fn disable() {
    ENABLED.store(false, Ordering::Release);
}

/// Drain all recorded events. Returns them sorted by timestamp.
#[cfg(feature = "lifecycle_trace")]
pub fn take() -> Vec<Event> {
    let mut events = std::mem::take(&mut *EVENTS.lock().unwrap());
    events.sort_by_key(|e| e.at);
    events
}

/// Internal call target used by the `lifecycle_trace!` macro. Not meant to
/// be called directly — always go through the macro so the feature gate
/// works at expansion time.
#[cfg(feature = "lifecycle_trace")]
#[doc(hidden)]
#[inline]
pub fn __record(label: &'static str, conn_id: u64, seq: u64, thread: &'static str) {
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

// ── Feature ON: macro forwards to __record ─────────────────────────────────
/// Record a lifecycle event. With the `lifecycle_trace` feature:
/// - ON  → expands to a call to `__record` (real recording behind an
///         AtomicBool runtime gate).
/// - OFF → expands to `()`. Arguments are NOT evaluated — they disappear
///         from the token stream at macro expansion.
#[cfg(feature = "lifecycle_trace")]
#[macro_export]
macro_rules! lifecycle_trace {
    ($label:literal, $conn:expr, $seq:expr, $thread:literal $(,)?) => {
        $crate::lifecycle_trace::__record($label, $conn, $seq, $thread)
    };
}

// ── Feature OFF: macro expands to () ───────────────────────────────────────
#[cfg(not(feature = "lifecycle_trace"))]
#[macro_export]
macro_rules! lifecycle_trace {
    ($label:literal, $conn:expr, $seq:expr, $thread:literal $(,)?) => {
        ()
    };
}

// ── Feature OFF: stub API (so diagnostic code that references enable/take
// still type-checks without the feature, though nothing currently does) ────
#[cfg(not(feature = "lifecycle_trace"))]
#[inline(always)]
pub fn enable() {}

#[cfg(not(feature = "lifecycle_trace"))]
#[inline(always)]
pub fn disable() {}

#[cfg(not(feature = "lifecycle_trace"))]
#[inline(always)]
pub fn take() -> Vec<Event> {
    Vec::new()
}

#[cfg(not(feature = "lifecycle_trace"))]
#[derive(Clone, Debug)]
pub struct Event {
    pub label: &'static str,
    pub conn_id: u64,
    pub seq: u64,
    pub thread: &'static str,
    /// Present so that consumers (like the lifecycle_flow e2e test) compile
    /// without the feature; the field is never populated in stub mode because
    /// `take()` always returns an empty vec.
    pub at: std::time::Instant,
}
