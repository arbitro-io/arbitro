//! Decorrelated-jitter backoff (AWS algorithm):
//! `next = min(cap, rand(base, prev * 3))`.

use std::time::Duration;

use crate::config::ReconnectPolicy;

#[derive(Debug)]
pub(crate) struct Backoff {
    base: Duration,
    cap: Duration,
    max: Option<u32>,
    /// Previous delay; seeded with `base` so the first attempt yields
    /// something in `[base, base*3]`.
    prev: Duration,
    /// Number of attempts already taken (zero on first call to `next`).
    attempts: u32,
}

impl Backoff {
    pub fn new(p: &ReconnectPolicy) -> Self {
        Self {
            base: p.base,
            cap: p.cap,
            max: p.max_attempts,
            prev: p.base,
            attempts: 0,
        }
    }

    /// Reset after a successful connect — next backoff starts at `base` again.
    pub fn reset(&mut self) {
        self.prev = self.base;
        self.attempts = 0;
    }

    /// Returns `Some(delay)` to wait before the next attempt, or `None`
    /// if `max_attempts` has been reached.
    pub fn next(&mut self) -> Option<Duration> {
        if let Some(m) = self.max {
            if self.attempts >= m {
                return None;
            }
        }
        let lo = self.base.as_nanos() as u64;
        let hi = (self.prev.as_nanos() as u64).saturating_mul(3).max(lo + 1);
        let pick_ns = fastrand::u64(lo..hi);
        let cap_ns = self.cap.as_nanos() as u64;
        let chosen = pick_ns.min(cap_ns);
        let d = Duration::from_nanos(chosen);
        self.prev = d;
        self.attempts += 1;
        Some(d)
    }

    pub fn attempts(&self) -> u32 {
        self.attempts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn within_bounds_and_caps() {
        let p = ReconnectPolicy {
            base: Duration::from_millis(100),
            cap: Duration::from_secs(1),
            max_attempts: None,
        };
        let mut b = Backoff::new(&p);
        for _ in 0..200 {
            let d = b.next().unwrap();
            assert!(d >= Duration::from_millis(100));
            assert!(d <= Duration::from_secs(1));
        }
    }

    #[test]
    fn max_attempts_terminates() {
        let p = ReconnectPolicy {
            base: Duration::from_millis(1),
            cap: Duration::from_millis(10),
            max_attempts: Some(3),
        };
        let mut b = Backoff::new(&p);
        assert!(b.next().is_some());
        assert!(b.next().is_some());
        assert!(b.next().is_some());
        assert!(b.next().is_none());
    }

    #[test]
    fn reset_starts_over() {
        let p = ReconnectPolicy {
            base: Duration::from_millis(1),
            cap: Duration::from_millis(10),
            max_attempts: Some(2),
        };
        let mut b = Backoff::new(&p);
        b.next().unwrap();
        b.next().unwrap();
        assert!(b.next().is_none());
        b.reset();
        assert!(b.next().is_some());
    }
}
