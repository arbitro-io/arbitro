//! `Flusher` — coalescing write buffer with configurable flush triggers.
//!
//! Flushes on the **first** condition met:
//! - `max_bytes` reached
//! - `max_count` frames reached
//! - `interval_ms` elapsed since last push (only when non-empty)
//!
//! The interval timer:
//! - Starts on first push into empty buffer
//! - Resets on every push
//! - Stops on flush/clear
//! - Dormant when buffer is empty (0% CPU)

use std::time::Instant;

/// Flush trigger configuration.
#[derive(Debug, Clone, Copy)]
pub struct FlushConfig {
    /// Flush when buffer reaches this size. 0 = no byte limit.
    pub max_bytes: usize,
    /// Flush when frame count reaches this. 0 = no count limit.
    pub max_count: u32,
    /// Flush after this many ms since last push. 0 = no interval.
    pub interval_ms: u32,
}

impl FlushConfig {
    pub fn new() -> Self {
        Self {
            max_bytes: 64 * 1024,
            max_count: 1000,
            interval_ms: 5,
        }
    }

    pub fn max_bytes(mut self, v: usize) -> Self { self.max_bytes = v; self }
    pub fn max_count(mut self, v: u32) -> Self { self.max_count = v; self }
    pub fn interval_ms(mut self, v: u32) -> Self { self.interval_ms = v; self }
}

/// Why the flusher decided to flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    Bytes,
    Count,
    Interval,
}

/// Pre-allocated coalescing buffer with flush triggers.
///
/// Capacity grows monotonically — never shrinks.
pub struct Flusher {
    buf: Vec<u8>,
    frame_count: u32,
    config: FlushConfig,
    last_push: Option<Instant>,
}

impl Flusher {
    pub fn new(config: FlushConfig) -> Self {
        let cap = if config.max_bytes > 0 { config.max_bytes } else { 64 * 1024 };
        Self {
            buf: Vec::with_capacity(cap),
            frame_count: 0,
            config,
            last_push: None,
        }
    }

    /// Append a frame. Returns `Some(reason)` if a flush trigger was hit.
    ///
    /// `now` is the caller's cached `Instant` — avoids a syscall per push.
    #[inline]
    pub fn push(&mut self, frame: &[u8], now: Instant) -> Option<FlushReason> {
        self.buf.extend_from_slice(frame);
        self.frame_count += 1;
        self.last_push = Some(now);
        self.check_limits()
    }

    /// Append multiple parts as a single logical frame.
    ///
    /// `now` is the caller's cached `Instant` — avoids a syscall per push.
    #[inline]
    pub fn push_parts(&mut self, parts: &[&[u8]], now: Instant) -> Option<FlushReason> {
        for part in parts {
            self.buf.extend_from_slice(part);
        }
        self.frame_count += 1;
        self.last_push = Some(now);
        self.check_limits()
    }

    /// Check if the interval has expired. Call this from your event loop.
    /// Returns `Some(Interval)` if flush is due, `None` otherwise.
    #[inline]
    pub fn check_interval(&self) -> Option<FlushReason> {
        if self.config.interval_ms == 0 || self.buf.is_empty() {
            return None;
        }
        if let Some(last) = self.last_push {
            if last.elapsed().as_millis() as u32 >= self.config.interval_ms {
                return Some(FlushReason::Interval);
            }
        }
        None
    }

    /// Milliseconds until interval expires. Used to set timer/sleep.
    /// Returns `None` if buffer is empty or interval is disabled.
    #[inline]
    pub fn ms_until_flush(&self) -> Option<u32> {
        if self.config.interval_ms == 0 || self.buf.is_empty() {
            return None;
        }
        if let Some(last) = self.last_push {
            let elapsed = last.elapsed().as_millis() as u32;
            if elapsed >= self.config.interval_ms {
                Some(0)
            } else {
                Some(self.config.interval_ms - elapsed)
            }
        } else {
            None
        }
    }

    /// The coalesced buffer ready for a single write().
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf
    }

    #[inline]
    pub fn frame_count(&self) -> u32 {
        self.frame_count
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Reset for next flush cycle. Capacity is retained. Timer stops.
    #[inline]
    pub fn clear(&mut self) {
        self.buf.clear();
        self.frame_count = 0;
        self.last_push = None;
    }

    #[inline]
    fn check_limits(&self) -> Option<FlushReason> {
        if self.config.max_bytes > 0 && self.buf.len() >= self.config.max_bytes {
            return Some(FlushReason::Bytes);
        }
        if self.config.max_count > 0 && self.frame_count >= self.config.max_count {
            return Some(FlushReason::Count);
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn trigger_on_bytes() {
        let cfg = FlushConfig::new().max_bytes(10).max_count(0).interval_ms(0);
        let mut f = Flusher::new(cfg);
        let now = Instant::now();
        assert_eq!(f.push(b"12345", now), None);
        assert_eq!(f.push(b"67890", now), Some(FlushReason::Bytes));
    }

    #[test]
    fn trigger_on_count() {
        let cfg = FlushConfig::new().max_bytes(0).max_count(3).interval_ms(0);
        let mut f = Flusher::new(cfg);
        let now = Instant::now();
        assert_eq!(f.push(b"a", now), None);
        assert_eq!(f.push(b"b", now), None);
        assert_eq!(f.push(b"c", now), Some(FlushReason::Count));
    }

    #[test]
    fn interval_only_when_non_empty() {
        let cfg = FlushConfig::new().max_bytes(0).max_count(0).interval_ms(10);
        let f = Flusher::new(cfg);
        assert_eq!(f.check_interval(), None);
        assert_eq!(f.ms_until_flush(), None);
    }

    #[test]
    fn interval_triggers_after_elapsed() {
        let cfg = FlushConfig::new().max_bytes(0).max_count(0).interval_ms(10);
        let mut f = Flusher::new(cfg);
        f.push(b"data", Instant::now());
        thread::sleep(Duration::from_millis(15));
        assert_eq!(f.check_interval(), Some(FlushReason::Interval));
    }

    #[test]
    fn interval_resets_on_push() {
        let cfg = FlushConfig::new().max_bytes(0).max_count(0).interval_ms(20);
        let mut f = Flusher::new(cfg);
        f.push(b"a", Instant::now());
        thread::sleep(Duration::from_millis(10));
        f.push(b"b", Instant::now()); // resets timer
        thread::sleep(Duration::from_millis(10));
        // Only 10ms since last push, should not trigger (interval=20ms)
        assert_eq!(f.check_interval(), None);
    }

    #[test]
    fn clear_stops_timer() {
        let cfg = FlushConfig::new().max_bytes(0).max_count(0).interval_ms(5);
        let mut f = Flusher::new(cfg);
        f.push(b"data", Instant::now());
        f.clear();
        thread::sleep(Duration::from_millis(10));
        assert_eq!(f.check_interval(), None);
        assert_eq!(f.ms_until_flush(), None);
    }

    #[test]
    fn push_parts_triggers() {
        let cfg = FlushConfig::new().max_bytes(10).max_count(0).interval_ms(0);
        let mut f = Flusher::new(cfg);
        assert_eq!(f.push_parts(&[b"12345", b"67890"], Instant::now()), Some(FlushReason::Bytes));
        assert_eq!(f.frame_count(), 1);
    }

    #[test]
    fn capacity_retained_after_clear() {
        let cfg = FlushConfig::new();
        let mut f = Flusher::new(cfg);
        f.push(&[0u8; 1024], Instant::now());
        let cap = f.buf.capacity();
        f.clear();
        assert_eq!(f.buf.capacity(), cap);
    }

    #[test]
    fn bytes_trigger_first() {
        let cfg = FlushConfig::new().max_bytes(5).max_count(1).interval_ms(0);
        let mut f = Flusher::new(cfg);
        assert_eq!(f.push(b"0123456789", Instant::now()), Some(FlushReason::Bytes));
    }
}
