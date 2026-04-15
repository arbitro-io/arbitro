//! Credit plugin — multi-scope credit counter arrays.
//!
//! Level 4 — depends on `types`, `plugin/mod`.
//!
//! Three credit scopes: Node, Connection, Subject.
//! Each scope is a HashMap<u32, CreditCounter>.
//! All operations O(1). No allocation on hot path.

use crate::types::CreditScope;
use std::collections::HashMap;

/// Per-scope credit counter.
#[derive(Debug, Clone, Copy)]
pub struct CreditCounter {
    pub limit: u32,
    pub used: u32,
}

impl CreditCounter {
    #[inline]
    pub fn new(limit: u32) -> Self { Self { limit, used: 0 } }

    #[inline]
    pub fn available(&self) -> u32 { self.limit.saturating_sub(self.used) }

    /// Try to acquire one credit. Returns true if successful.
    #[inline]
    pub fn try_acquire(&mut self) -> bool {
        if self.used < self.limit {
            self.used += 1;
            true
        } else {
            false
        }
    }

    /// Release one credit. Saturates at zero.
    #[inline]
    pub fn release(&mut self) {
        self.used = self.used.saturating_sub(1);
    }

    /// Reset used to zero (drain).
    #[inline]
    pub fn reset(&mut self) {
        self.used = 0;
    }
}

/// Multi-scope credit management plugin.
///
/// Tracks credit limits and usage across Node, Connection, and Subject scopes.
pub struct CreditPlugin {
    node: HashMap<u32, CreditCounter, ahash::RandomState>,
    connection: HashMap<u32, CreditCounter, ahash::RandomState>,
    subject: HashMap<u32, CreditCounter, ahash::RandomState>,
    /// Fast-path flags: the claim hot loop checks these to skip HashMap
    /// lookups entirely when no limits are configured for a scope. Set to
    /// `true` on the first `set_limit` for that scope; never reset to false
    /// (sticky — avoids re-scanning on removal; worst case is an unneeded
    /// lookup that returns `None`). Common case (no credits configured):
    /// ~15-25 ns/msg saved in the loop.
    has_connection_limits: bool,
    has_subject_limits: bool,
}

impl CreditPlugin {
    pub fn new() -> Self {
        Self {
            node: HashMap::with_hasher(ahash::RandomState::new()),
            connection: HashMap::with_hasher(ahash::RandomState::new()),
            subject: HashMap::with_hasher(ahash::RandomState::new()),
            has_connection_limits: false,
            has_subject_limits: false,
        }
    }

    /// Fast-path: is any Connection-scope limit configured?
    #[inline(always)]
    pub fn has_connection_limits(&self) -> bool { self.has_connection_limits }

    /// Fast-path: is any Subject-scope limit configured?
    #[inline(always)]
    pub fn has_subject_limits(&self) -> bool { self.has_subject_limits }

    #[inline]
    fn scope_map(&self, scope: CreditScope) -> &HashMap<u32, CreditCounter, ahash::RandomState> {
        match scope {
            CreditScope::Node => &self.node,
            CreditScope::Connection => &self.connection,
            CreditScope::Subject => &self.subject,
        }
    }

    #[inline]
    fn scope_map_mut(&mut self, scope: CreditScope) -> &mut HashMap<u32, CreditCounter, ahash::RandomState> {
        match scope {
            CreditScope::Node => &mut self.node,
            CreditScope::Connection => &mut self.connection,
            CreditScope::Subject => &mut self.subject,
        }
    }

    /// Set the credit limit for a key in a scope. Management path.
    pub fn set_limit(&mut self, scope: CreditScope, key: u32, limit: u32) {
        match scope {
            CreditScope::Connection => self.has_connection_limits = true,
            CreditScope::Subject => self.has_subject_limits = true,
            CreditScope::Node => {}
        }
        self.scope_map_mut(scope)
            .entry(key)
            .and_modify(|c| c.limit = limit)
            .or_insert(CreditCounter::new(limit));
    }

    /// Try to acquire a credit. O(1). Returns true if acquired.
    #[inline]
    pub fn try_acquire(&mut self, scope: CreditScope, key: u32) -> bool {
        if let Some(counter) = self.scope_map_mut(scope).get_mut(&key) {
            counter.try_acquire()
        } else {
            true // no limit set = unlimited
        }
    }

    /// Release a credit. O(1).
    #[inline]
    pub fn release(&mut self, scope: CreditScope, key: u32) {
        if let Some(counter) = self.scope_map_mut(scope).get_mut(&key) {
            counter.release();
        }
    }

    /// Get available credits. O(1).
    #[inline]
    pub fn available(&self, scope: CreditScope, key: u32) -> u32 {
        self.scope_map(scope)
            .get(&key)
            .map(|c| c.available())
            .unwrap_or(u32::MAX) // no limit = unlimited
    }

    /// Check if a key has credits available. O(1).
    #[inline]
    pub fn has_credit(&self, scope: CreditScope, key: u32) -> bool {
        self.scope_map(scope)
            .get(&key)
            .map(|c| c.used < c.limit)
            .unwrap_or(true)
    }

    /// Reset all credits for a key (drain). O(1).
    pub fn reset(&mut self, scope: CreditScope, key: u32) {
        if let Some(counter) = self.scope_map_mut(scope).get_mut(&key) {
            counter.reset();
        }
    }

    /// Remove a key entirely from a scope.
    pub fn remove(&mut self, scope: CreditScope, key: u32) {
        self.scope_map_mut(scope).remove(&key);
    }
}

impl Default for CreditPlugin {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquire_and_release() {
        let mut credit = CreditPlugin::new();
        credit.set_limit(CreditScope::Node, 1, 3);

        assert!(credit.try_acquire(CreditScope::Node, 1));
        assert!(credit.try_acquire(CreditScope::Node, 1));
        assert!(credit.try_acquire(CreditScope::Node, 1));
        assert!(!credit.try_acquire(CreditScope::Node, 1)); // exhausted

        credit.release(CreditScope::Node, 1);
        assert!(credit.try_acquire(CreditScope::Node, 1)); // freed one
    }

    #[test]
    fn no_limit_means_unlimited() {
        let mut credit = CreditPlugin::new();
        // No limit set — should always succeed
        assert!(credit.try_acquire(CreditScope::Connection, 99));
        assert!(credit.has_credit(CreditScope::Connection, 99));
        assert_eq!(credit.available(CreditScope::Connection, 99), u32::MAX);
    }

    #[test]
    fn available_tracking() {
        let mut credit = CreditPlugin::new();
        credit.set_limit(CreditScope::Subject, 10, 5);

        assert_eq!(credit.available(CreditScope::Subject, 10), 5);
        credit.try_acquire(CreditScope::Subject, 10);
        credit.try_acquire(CreditScope::Subject, 10);
        assert_eq!(credit.available(CreditScope::Subject, 10), 3);
    }

    #[test]
    fn reset_clears_usage() {
        let mut credit = CreditPlugin::new();
        credit.set_limit(CreditScope::Node, 1, 2);
        credit.try_acquire(CreditScope::Node, 1);
        credit.try_acquire(CreditScope::Node, 1);
        assert!(!credit.try_acquire(CreditScope::Node, 1));

        credit.reset(CreditScope::Node, 1);
        assert!(credit.try_acquire(CreditScope::Node, 1));
    }

    #[test]
    fn scopes_are_independent() {
        let mut credit = CreditPlugin::new();
        credit.set_limit(CreditScope::Node, 1, 1);
        credit.set_limit(CreditScope::Connection, 1, 1);

        credit.try_acquire(CreditScope::Node, 1);
        assert!(!credit.try_acquire(CreditScope::Node, 1));
        assert!(credit.try_acquire(CreditScope::Connection, 1)); // independent
    }

    #[test]
    fn remove_key() {
        let mut credit = CreditPlugin::new();
        credit.set_limit(CreditScope::Node, 1, 5);
        credit.try_acquire(CreditScope::Node, 1);

        credit.remove(CreditScope::Node, 1);
        // After removal, no limit = unlimited
        assert!(credit.has_credit(CreditScope::Node, 1));
        assert_eq!(credit.available(CreditScope::Node, 1), u32::MAX);
    }
}
