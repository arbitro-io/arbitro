//! DrainSignal trait — abstraction for waking drain tasks.
//!
//! The engine signals after append/ack/nack. The server provides
//! an async Gate implementation (tokio::sync::Notify).
//! Tests use NullSignal.

/// Signal the drain task that new work is available.
/// Implementations must be non-blocking and cheap (O(1), zero alloc).
pub trait DrainSignal: Send + Sync {
    /// Wake the drain task. Non-blocking.
    fn release(&self);
}

/// No-op signal for tests and benchmarks.
pub struct NullSignal;

impl DrainSignal for NullSignal {
    #[inline(always)]
    fn release(&self) {}
}
