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
use std::sync::{Arc, Mutex, OnceLock};
#[cfg(feature = "lifecycle_trace")]
use std::time::Instant;

#[cfg(feature = "lifecycle_trace")]
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Events kept PER THREAD. One shared ring does not work: a stalled thread
/// stops recording while every other thread keeps going, and the busy ones
/// overwrite the stalled one's tail — which is exactly the evidence a hang
/// investigation needs. Per-thread rings keep each thread's last moments.
#[cfg(feature = "lifecycle_trace")]
const RING_CAP: usize = 256;

#[cfg(feature = "lifecycle_trace")]
struct Ring {
    buf: Vec<Event>,
    /// Oldest slot once the ring is full.
    next: usize,
}

/// Every thread's ring, so `take` can collect across threads. Touched once
/// per thread at first record; the hot path only locks its own ring, which
/// is uncontended by construction.
#[cfg(feature = "lifecycle_trace")]
static RINGS: Mutex<Vec<Arc<Mutex<Ring>>>> = Mutex::new(Vec::new());

#[cfg(feature = "lifecycle_trace")]
thread_local! {
    static LOCAL: Arc<Mutex<Ring>> = {
        let ring = Arc::new(Mutex::new(Ring { buf: Vec::new(), next: 0 }));
        if let Ok(mut rings) = RINGS.lock() {
            rings.push(Arc::clone(&ring));
        }
        ring
    };
}

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
    if let Ok(rings) = RINGS.lock() {
        for ring in rings.iter() {
            if let Ok(mut r) = ring.lock() {
                r.buf.clear();
                r.next = 0;
            }
        }
    }
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
    let mut events = Vec::new();
    if let Ok(rings) = RINGS.lock() {
        for ring in rings.iter() {
            if let Ok(mut r) = ring.lock() {
                let n = r.next;
                let mut owned = std::mem::take(&mut r.buf);
                r.next = 0;
                if owned.len() == RING_CAP && n > 0 {
                    owned.rotate_left(n);
                }
                events.append(&mut owned);
            }
        }
    }
    events.sort_by_key(|e| e.at);
    events
}

/// Label prefixes to drop, from `ARBITRO_LIFECYCLE_SKIP` (comma separated).
/// Per-message call sites drown everything else out of a bounded ring; naming
/// them here keeps the rare events that a hang investigation actually needs.
#[cfg(feature = "lifecycle_trace")]
fn skipped(label: &str) -> bool {
    static SKIP: OnceLock<Vec<String>> = OnceLock::new();
    SKIP.get_or_init(|| {
        std::env::var("ARBITRO_LIFECYCLE_SKIP")
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect()
            })
            .unwrap_or_default()
    })
    .iter()
    .any(|p| label.starts_with(p.as_str()))
}

/// Internal call target used by the `lifecycle_trace!` macro. Not meant to
/// be called directly — always go through the macro so the feature gate
/// works at expansion time.
#[cfg(feature = "lifecycle_trace")]
#[doc(hidden)]
#[inline]
pub fn __record(label: &'static str, conn_id: u64, seq: u64, thread: &'static str) {
    if !ENABLED.load(Ordering::Relaxed) || skipped(label) {
        return;
    }
    let event = Event {
        label,
        conn_id,
        seq,
        thread,
        at: Instant::now(),
    };
    // try_with: a thread recording during its own TLS teardown must not panic.
    let _ = LOCAL.try_with(|ring| {
        if let Ok(mut r) = ring.lock() {
            if r.buf.len() < RING_CAP {
                r.buf.push(event);
            } else {
                let i = r.next;
                r.buf[i] = event;
                r.next = (i + 1) % RING_CAP;
            }
        }
    });
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
