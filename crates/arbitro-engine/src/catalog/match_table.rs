//! Precomputed match table — subject → consumers resolved at subscription time.
//!
//! Level 5 — depends on `types`.
//!
//! At publish time, we need to know which consumers should receive a message
//! for a given subject. Instead of evaluating filters at publish time (O(N)
//! per subject × consumers), we precompute the mapping at subscription time.
//!
//! The match table maps subject_hash → Vec<MatchEntry>.
//! Wildcard patterns (*, >) are expanded at insert time using
//! pattern matching logic.

use crate::common::SubjectTrie;
use crate::types::*;
use std::collections::HashMap;

/// Sentinel value for `MatchEntry::binding_idx` meaning "unbound" — the
/// subscription exists in the match table but no active binding has been
/// stamped onto it yet (pull-model subscription, or the snapshot hasn't
/// been rebuilt since bind). Drain must skip entries with this value.
pub const BINDING_IDX_UNBOUND: u32 = u32::MAX;

/// A matched consumer for a subject.
///
/// `connection_id` is precomputed at bind time (management path).
/// Publish reads it directly — zero edge/graph lookups on hot path.
/// ConnectionId(0) means no active binding (pull model).
///
/// `binding_idx` is a server-layer index into `DrainSnapshot.bindings`,
/// stamped onto the match table during snapshot rebuild (NOT at engine
/// bind time — engine catalog is server-agnostic). Drain uses this to
/// fetch the binding with a direct Vec access instead of a HashMap
/// lookup on `(consumer_id, connection_id)`. Sentinel
/// `BINDING_IDX_UNBOUND` means no active binding — drain must skip.
///
/// **PartialEq / Eq explicitly EXCLUDE `binding_idx`** so that entries
/// for the same (consumer, connection, queue, subscription) tuple dedup
/// correctly even when `binding_idx` differs across snapshot rebuilds.
/// Violating this invariant breaks `add_exact::contains(&entry)` and
/// `resolve_patterns::contains(entry)` dedup.
#[derive(Debug, Clone, Copy, Eq)]
pub struct MatchEntry {
    pub consumer_id: ConsumerId,
    pub queue_id: QueueId,
    pub subscription_id: SubscriptionId,
    pub connection_id: ConnectionId,
    /// Server-layer index into snapshot bindings. `BINDING_IDX_UNBOUND`
    /// until stamped by `rebuild_and_swap_snapshot`.
    pub binding_idx: u32,
}

impl PartialEq for MatchEntry {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        // Intentionally excludes `binding_idx` — see struct docs. Two
        // entries that differ only in binding_idx describe the same
        // logical subscription and MUST dedup together.
        self.consumer_id == other.consumer_id
            && self.queue_id == other.queue_id
            && self.subscription_id == other.subscription_id
            && self.connection_id == other.connection_id
    }
}

// Hash isn't used in hot paths but derive would include binding_idx.
// If a future caller needs `MatchEntry: Hash` they MUST implement it
// manually with the same exclusion rule.
impl std::hash::Hash for MatchEntry {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.consumer_id.hash(state);
        self.queue_id.hash(state);
        self.subscription_id.hash(state);
        self.connection_id.hash(state);
    }
}

/// Precomputed subject → consumer mapping.
///
/// Built incrementally when subscriptions are added/removed.
/// Lookup at publish time is O(1) hash + iterate matched consumers (typically 1-3).
#[derive(Clone)]
pub struct MatchTable {
    /// Exact subject_hash → matched consumers.
    exact: HashMap<u32, Vec<MatchEntry>, ahash::RandomState>,

    /// Wildcard subscriptions that match all subjects on a stream.
    /// These are appended to every lookup result.
    catch_all: Vec<MatchEntry>,

    /// Pattern subscriptions: (pattern_bytes, entry).
    /// Kept for mutation tracking (add/remove). Trie is rebuilt on change.
    patterns: Vec<(Vec<u8>, MatchEntry)>,

    /// Arena trie for O(depth) pattern matching on cold resolve.
    /// Rebuilt from `patterns` whenever patterns change.
    pattern_trie: SubjectTrie,

    /// Trie index → MatchEntry mapping.
    pattern_entries: Vec<MatchEntry>,

    /// Cache of subjects we've already resolved patterns for.
    resolved_subjects: HashMap<u32, bool, ahash::RandomState>,

    /// Subject limit patterns: (pattern_bytes, max_inflight).
    /// Resolved to concrete hashes at publish time (same as match patterns).
    limit_patterns: Vec<(Vec<u8>, u32)>,

    /// Arena trie for O(depth) limit pattern matching.
    limit_trie: SubjectTrie,

    /// Trie index → max_inflight mapping.
    limit_values: Vec<u32>,

    /// Precomputed subject_hash → max inflight.
    /// Populated at publish time when patterns are resolved.
    max_subject_inflights: HashMap<u32, u32, ahash::RandomState>,
}

impl MatchTable {
    pub fn new() -> Self {
        Self {
            exact: HashMap::with_hasher(ahash::RandomState::new()),
            catch_all: Vec::new(),
            patterns: Vec::new(),
            pattern_trie: SubjectTrie::new(),
            pattern_entries: Vec::new(),
            resolved_subjects: HashMap::with_hasher(ahash::RandomState::new()),
            limit_patterns: Vec::new(),
            limit_trie: SubjectTrie::new(),
            limit_values: Vec::new(),
            max_subject_inflights: HashMap::with_hasher(ahash::RandomState::new()),
        }
    }

    /// Add a subscription with no filter (catch-all: receives everything).
    pub fn add_catch_all(&mut self, entry: MatchEntry) {
        if !self.catch_all.contains(&entry) {
            self.catch_all.push(entry);
        }
    }

    /// Add a subscription for an exact subject hash.
    pub fn add_exact(&mut self, subject_hash: u32, entry: MatchEntry) {
        let entries = self.exact.entry(subject_hash).or_default();
        if !entries.contains(&entry) {
            entries.push(entry);
        }
    }

    /// Add a subscription with a wildcard pattern.
    /// The pattern will be evaluated against new subjects as they appear.
    pub fn add_pattern(&mut self, pattern: Vec<u8>, entry: MatchEntry) {
        self.patterns.push((pattern, entry));
        self.rebuild_pattern_trie();
        // Invalidate resolved cache — new pattern may match cached subjects
        self.resolved_subjects.clear();
    }

    /// Remove all entries for a subscription.
    pub fn remove_subscription(&mut self, subscription_id: SubscriptionId) {
        self.catch_all.retain(|e| e.subscription_id != subscription_id);

        self.exact.retain(|_, entries| {
            entries.retain(|e| e.subscription_id != subscription_id);
            !entries.is_empty()
        });

        let had_patterns = self.patterns.iter().any(|(_, e)| e.subscription_id == subscription_id);
        self.patterns.retain(|(_, e)| e.subscription_id != subscription_id);
        if had_patterns {
            self.rebuild_pattern_trie();
        }
        self.resolved_subjects.clear();
    }

    /// Lookup matched consumers for a subject. O(1) for cached subjects.
    ///
    /// Returns exact matches + catch-all entries.
    /// For new subjects with patterns, resolves and caches.
    #[inline]
    pub fn lookup(&self, subject_hash: u32) -> MatchResult<'_> {
        let exact = self.exact.get(&subject_hash)
            .map(|v| v.as_slice())
            .unwrap_or(&[]);

        MatchResult {
            exact,
            catch_all: &self.catch_all,
        }
    }

    /// Resolve patterns for a subject that hasn't been seen before.
    /// Called on first publish to a new subject_hash.
    ///
    /// Uses trie for O(depth) matching instead of O(patterns) linear scan.
    /// The result is cached in `exact` for future O(1) lookups.
    pub fn resolve_patterns(&mut self, subject_hash: u32, subject: &[u8]) {
        if self.resolved_subjects.contains_key(&subject_hash) {
            return;
        }

        // O(depth) trie walk instead of O(patterns) linear scan
        let pattern_entries = &self.pattern_entries;
        let exact = &mut self.exact;
        self.pattern_trie.find_matches(subject, |idx| {
            let entry = &pattern_entries[idx as usize];
            let entries = exact.entry(subject_hash).or_default();
            if !entries.contains(entry) {
                entries.push(*entry);
            }
        });

        // Resolve subject limit patterns — pick the tightest (minimum) limit
        let limit_values = &self.limit_values;
        let max_subject_inflights = &mut self.max_subject_inflights;
        self.limit_trie.find_matches(subject, |idx| {
            let max_inflight = limit_values[idx as usize];
            let entry = max_subject_inflights.entry(subject_hash).or_insert(u32::MAX);
            *entry = (*entry).min(max_inflight);
        });

        self.resolved_subjects.insert(subject_hash, true);
    }

    /// Resolve patterns for a subject without mutating self.
    /// Used by the drain thread which reads a snapshot.
    /// Results are collected into `out` — caller should cache.
    pub fn resolve_patterns_readonly(
        &self,
        subject_hash: u32,
        subject: &[u8],
        out: &mut Vec<MatchEntry>,
    ) {
        if self.resolved_subjects.contains_key(&subject_hash) {
            return;
        }
        let pattern_entries = &self.pattern_entries;
        self.pattern_trie.find_matches(subject, |idx| {
            let entry = &pattern_entries[idx as usize];
            if !out.contains(entry) {
                out.push(*entry);
            }
        });
    }

    // ── Subject limits ────────────────────────────────────────────────────

    /// Add a subject inflight limit by pattern. Management path.
    ///
    /// The pattern is resolved to concrete subject hashes at publish time
    /// (same as match patterns). If the pattern is a literal (no wildcards),
    /// it's resolved immediately.
    pub fn add_max_subject_inflight(&mut self, pattern: &[u8], max_inflight: u32) {
        if pattern.contains(&b'*') || pattern.contains(&b'>') {
            self.limit_patterns.push((pattern.to_vec(), max_inflight));
            self.rebuild_limit_trie();
            // Invalidate resolved cache so limits get recomputed
            self.resolved_subjects.clear();
        } else {
            // Literal — resolve immediately
            let hash = crate::catalog::fnv1a_32(pattern);
            self.max_subject_inflights.insert(hash, max_inflight);
        }
    }

    /// Remove a subject inflight limit pattern.
    pub fn remove_max_subject_inflight(&mut self, pattern: &[u8]) {
        if pattern.contains(&b'*') || pattern.contains(&b'>') {
            self.limit_patterns.retain(|(p, _)| p != pattern);
            self.rebuild_limit_trie();
            self.resolved_subjects.clear();
        } else {
            let hash = crate::catalog::fnv1a_32(pattern);
            self.max_subject_inflights.remove(&hash);
        }
    }

    /// Lookup the max inflight for a subject. O(1).
    /// Returns None if no limit is set (unlimited).
    #[inline]
    pub fn max_subject_inflight(&self, subject_hash: u32) -> Option<u32> {
        self.max_subject_inflights.get(&subject_hash).copied()
    }

    /// Resolve wildcard subject limits without mutating self.
    /// Used by drain thread on a snapshot. Returns the min limit found,
    /// or None if no wildcard patterns match.
    pub fn resolve_subject_limit_readonly(
        &self,
        subject_hash: u32,
        subject: &[u8],
    ) -> Option<u32> {
        // Already resolved (literal or previous resolve_patterns call)?
        if let Some(&limit) = self.max_subject_inflights.get(&subject_hash) {
            return Some(limit);
        }
        // Walk limit trie for wildcard patterns.
        if self.limit_patterns.is_empty() {
            return None;
        }
        let limit_values = &self.limit_values;
        let mut min_limit: Option<u32> = None;
        self.limit_trie.find_matches(subject, |idx| {
            let max_inflight = limit_values[idx as usize];
            let entry = min_limit.get_or_insert(u32::MAX);
            *entry = (*entry).min(max_inflight);
        });
        min_limit
    }

    /// Fast-path: does ANY subject on this stream have an inflight limit?
    /// The claim hot loop checks this once per batch and skips
    /// `max_subject_inflight` HashMap lookups entirely when false.
    #[inline(always)]
    pub fn has_subject_limits(&self) -> bool {
        !self.max_subject_inflights.is_empty() || !self.limit_patterns.is_empty()
    }

    // ── Binding precomputation (management path) ──────────────────────────

    /// Set the connection_id on all match entries for a subscription.
    /// Called at bind time. O(S + C + P) where S = subjects, C = catch_all, P = patterns.
    pub fn bind_subscription(&mut self, subscription_id: SubscriptionId, connection_id: ConnectionId) {
        for entries in self.exact.values_mut() {
            for e in entries.iter_mut() {
                if e.subscription_id == subscription_id {
                    e.connection_id = connection_id;
                }
            }
        }
        for e in &mut self.catch_all {
            if e.subscription_id == subscription_id {
                e.connection_id = connection_id;
            }
        }
        for (_, e) in &mut self.patterns {
            if e.subscription_id == subscription_id {
                e.connection_id = connection_id;
            }
        }
        // pattern_entries must stay in sync with patterns (used by resolve_patterns)
        for e in &mut self.pattern_entries {
            if e.subscription_id == subscription_id {
                e.connection_id = connection_id;
            }
        }
    }

    /// Clear the connection_id on all match entries for a subscription.
    /// Called at unbind time.
    pub fn unbind_subscription(&mut self, subscription_id: SubscriptionId) {
        self.bind_subscription(subscription_id, ConnectionId(0));
    }

    /// Stamp `binding_idx` onto every match entry matching
    /// `(consumer_id, connection_id)`.
    ///
    /// Semantics: a binding is unique per `(consumer, connection)` pair;
    /// a consumer may have multiple subscriptions (different patterns)
    /// but they all reach the same underlying connection/writer. All
    /// match entries for that pair must point to the same binding slot
    /// in `DrainSnapshot.bindings`.
    ///
    /// Called by the server during `rebuild_and_swap_snapshot` on the
    /// **cloned** match table (not the engine's canonical copy) so the
    /// drain can fetch the binding with a direct `bindings[idx]` Vec
    /// access instead of a `(consumer_id, connection_id) → idx`
    /// HashMap lookup per match. O(S + C + P) per binding.
    pub fn set_binding_idx_for(
        &mut self,
        consumer_id: ConsumerId,
        connection_id: ConnectionId,
        binding_idx: u32,
    ) {
        let matches = |e: &MatchEntry| {
            e.consumer_id == consumer_id && e.connection_id == connection_id
        };
        for entries in self.exact.values_mut() {
            for e in entries.iter_mut() {
                if matches(e) {
                    e.binding_idx = binding_idx;
                }
            }
        }
        for e in &mut self.catch_all {
            if matches(e) {
                e.binding_idx = binding_idx;
            }
        }
        for (_, e) in &mut self.patterns {
            if matches(e) {
                e.binding_idx = binding_idx;
            }
        }
        for e in &mut self.pattern_entries {
            if matches(e) {
                e.binding_idx = binding_idx;
            }
        }
    }

    /// Number of exact subject mappings.
    pub fn exact_count(&self) -> usize { self.exact.len() }

    /// Number of catch-all subscriptions.
    pub fn catch_all_count(&self) -> usize { self.catch_all.len() }

    /// Number of pattern subscriptions.
    pub fn pattern_count(&self) -> usize { self.patterns.len() }

    // ── Trie rebuild (management path) ─────────────────────────────────────

    /// Rebuild the pattern trie from the patterns vec.
    fn rebuild_pattern_trie(&mut self) {
        self.pattern_trie.clear();
        self.pattern_entries.clear();
        for (i, (pattern, entry)) in self.patterns.iter().enumerate() {
            self.pattern_entries.push(*entry);
            self.pattern_trie.insert(pattern, i as u32);
        }
    }

    /// Rebuild the limit trie from the limit_patterns vec.
    fn rebuild_limit_trie(&mut self) {
        self.limit_trie.clear();
        self.limit_values.clear();
        for (i, (pattern, max_inflight)) in self.limit_patterns.iter().enumerate() {
            self.limit_values.push(*max_inflight);
            self.limit_trie.insert(pattern, i as u32);
        }
    }

    /// Clear everything.
    pub fn clear(&mut self) {
        self.exact.clear();
        self.catch_all.clear();
        self.patterns.clear();
        self.pattern_trie.clear();
        self.pattern_entries.clear();
        self.resolved_subjects.clear();
        self.limit_patterns.clear();
        self.limit_trie.clear();
        self.limit_values.clear();
        self.max_subject_inflights.clear();
    }
}

impl Default for MatchTable {
    fn default() -> Self { Self::new() }
}

/// Result of a match table lookup. Combines exact + catch-all.
pub struct MatchResult<'a> {
    pub exact: &'a [MatchEntry],
    pub catch_all: &'a [MatchEntry],
}

impl<'a> MatchResult<'a> {
    /// Total number of matched consumers.
    #[inline]
    pub fn count(&self) -> usize {
        self.exact.len() + self.catch_all.len()
    }

    /// Whether any consumer matched.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.exact.is_empty() && self.catch_all.is_empty()
    }

    /// Iterate over all matched entries (exact first, then catch-all).
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &MatchEntry> {
        self.exact.iter().chain(self.catch_all.iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::subject_matches;

    fn entry(consumer: u32, queue: u32, sub: u32) -> MatchEntry {
        MatchEntry {
            consumer_id: ConsumerId(consumer),
            queue_id: QueueId(queue),
            subscription_id: SubscriptionId(sub),
            connection_id: ConnectionId(0),
            binding_idx: BINDING_IDX_UNBOUND,
        }
    }

    #[test]
    fn partial_eq_excludes_binding_idx() {
        // CRITICAL INVARIANT: two entries differing only in binding_idx
        // must compare equal, otherwise `add_exact`'s dedup breaks and
        // rebuilds that re-stamp binding_idx would duplicate entries.
        let a = MatchEntry {
            consumer_id: ConsumerId(1),
            queue_id: QueueId(10),
            subscription_id: SubscriptionId(100),
            connection_id: ConnectionId(42),
            binding_idx: BINDING_IDX_UNBOUND,
        };
        let b = MatchEntry { binding_idx: 7, ..a };
        let c = MatchEntry { binding_idx: 99, ..a };
        assert_eq!(a, b, "binding_idx must NOT participate in PartialEq");
        assert_eq!(b, c, "binding_idx must NOT participate in PartialEq");

        // Other fields DO participate
        let d = MatchEntry { consumer_id: ConsumerId(2), ..a };
        assert_ne!(a, d);
    }

    #[test]
    fn add_exact_dedup_survives_binding_idx_restamping() {
        // Simulates the snapshot rebuild: same subscription inserted
        // twice with different binding_idx stamps must not duplicate.
        let mut mt = MatchTable::new();
        let e0 = MatchEntry { binding_idx: BINDING_IDX_UNBOUND, ..entry(1, 10, 100) };
        mt.add_exact(0xBEEF, e0);
        let e1 = MatchEntry { binding_idx: 42, ..entry(1, 10, 100) };
        mt.add_exact(0xBEEF, e1);

        let r = mt.lookup(0xBEEF);
        assert_eq!(r.count(), 1, "re-insert with different binding_idx must dedup");
    }

    #[test]
    fn set_binding_idx_for_stamps_all_locations() {
        let mut mt = MatchTable::new();
        let e_exact = MatchEntry { connection_id: ConnectionId(5), ..entry(1, 10, 100) };
        let e_catch = MatchEntry { connection_id: ConnectionId(5), ..entry(1, 10, 100) };
        mt.add_exact(0xBEEF, e_exact);
        mt.add_catch_all(e_catch);

        mt.set_binding_idx_for(
            ConsumerId(1),
            ConnectionId(5),
            777,
        );

        let r = mt.lookup(0xBEEF);
        assert!(r.exact.iter().all(|e| e.binding_idx == 777));
        assert!(r.catch_all.iter().all(|e| e.binding_idx == 777));
    }

    #[test]
    fn set_binding_idx_only_stamps_matching_pair() {
        let mut mt = MatchTable::new();
        // Two different (consumer, connection) pairs on the same subject.
        let e1 = MatchEntry { connection_id: ConnectionId(5), ..entry(1, 10, 100) };
        let e2 = MatchEntry { connection_id: ConnectionId(6), ..entry(2, 20, 200) };
        mt.add_exact(0xBEEF, e1);
        mt.add_exact(0xBEEF, e2);

        // Stamp only (consumer 1, conn 5)
        mt.set_binding_idx_for(
            ConsumerId(1),
            ConnectionId(5),
            777,
        );

        let r = mt.lookup(0xBEEF);
        let mut seen_777 = 0;
        let mut seen_unbound = 0;
        for e in r.exact {
            if e.binding_idx == 777 {
                seen_777 += 1;
            } else if e.binding_idx == BINDING_IDX_UNBOUND {
                seen_unbound += 1;
            }
        }
        assert_eq!(seen_777, 1, "only (consumer 1, conn 5) gets stamped");
        assert_eq!(seen_unbound, 1, "other pair stays unbound");
    }

    #[test]
    fn set_binding_idx_covers_all_subs_of_pair() {
        // A single binding (consumer 1, conn 5) might have MULTIPLE subscriptions
        // (different patterns). All must get the same binding_idx.
        let mut mt = MatchTable::new();
        let e_sub_a = MatchEntry { subscription_id: SubscriptionId(100), ..entry(1, 10, 100) };
        let e_sub_b = MatchEntry { subscription_id: SubscriptionId(101), ..entry(1, 10, 101) };
        let mut e_sub_a = e_sub_a;
        let mut e_sub_b = e_sub_b;
        e_sub_a.connection_id = ConnectionId(5);
        e_sub_b.connection_id = ConnectionId(5);
        mt.add_exact(0xBEEF, e_sub_a);
        mt.add_exact(0xDEAD, e_sub_b);

        mt.set_binding_idx_for(ConsumerId(1), ConnectionId(5), 42);

        assert!(mt.lookup(0xBEEF).exact.iter().all(|e| e.binding_idx == 42));
        assert!(mt.lookup(0xDEAD).exact.iter().all(|e| e.binding_idx == 42));
    }

    #[test]
    fn exact_match() {
        let mut mt = MatchTable::new();
        mt.add_exact(0xBEEF, entry(1, 10, 100));
        mt.add_exact(0xDEAD, entry(2, 20, 200));

        let r = mt.lookup(0xBEEF);
        assert_eq!(r.count(), 1);
        assert_eq!(r.exact[0].consumer_id, ConsumerId(1));

        let r = mt.lookup(0x0000);
        assert!(r.is_empty());
    }

    #[test]
    fn catch_all() {
        let mut mt = MatchTable::new();
        mt.add_catch_all(entry(1, 10, 100));
        mt.add_exact(0xBEEF, entry(2, 20, 200));

        let r = mt.lookup(0xBEEF);
        assert_eq!(r.count(), 2); // exact + catch-all

        let r = mt.lookup(0xDEAD);
        assert_eq!(r.count(), 1); // catch-all only
    }

    #[test]
    fn pattern_resolution() {
        let mut mt = MatchTable::new();
        mt.add_pattern(b"orders.*".to_vec(), entry(1, 10, 100));

        // Before resolution, exact lookup finds nothing
        let r = mt.lookup(0xBEEF);
        assert!(r.exact.is_empty());

        // Resolve
        mt.resolve_patterns(0xBEEF, b"orders.created");
        let r = mt.lookup(0xBEEF);
        assert_eq!(r.exact.len(), 1);

        // Non-matching subject
        mt.resolve_patterns(0xDEAD, b"users.created");
        let r = mt.lookup(0xDEAD);
        assert!(r.exact.is_empty());
    }

    #[test]
    fn remove_subscription() {
        let mut mt = MatchTable::new();
        let sub_id = SubscriptionId(100);
        mt.add_exact(0xBEEF, entry(1, 10, 100));
        mt.add_exact(0xBEEF, entry(2, 20, 200));
        mt.add_catch_all(entry(3, 30, 100));

        mt.remove_subscription(sub_id);

        let r = mt.lookup(0xBEEF);
        assert_eq!(r.exact.len(), 1);
        assert_eq!(r.exact[0].subscription_id, SubscriptionId(200));
        assert!(r.catch_all.is_empty());
    }

    #[test]
    fn no_duplicate_entries() {
        let mut mt = MatchTable::new();
        let e = entry(1, 10, 100);
        mt.add_exact(0xBEEF, e);
        mt.add_exact(0xBEEF, e); // duplicate

        assert_eq!(mt.lookup(0xBEEF).exact.len(), 1);
    }

    // ── subject_matches tests ────────────────────────────────────────────

    #[test]
    fn literal_match() {
        assert!(subject_matches(b"orders.created", b"orders.created"));
        assert!(!subject_matches(b"orders.created", b"orders.updated"));
    }

    #[test]
    fn star_wildcard() {
        assert!(subject_matches(b"orders.*", b"orders.created"));
        assert!(subject_matches(b"orders.*", b"orders.updated"));
        assert!(!subject_matches(b"orders.*", b"orders.us.created"));
        assert!(!subject_matches(b"orders.*", b"users.created"));
    }

    #[test]
    fn gt_wildcard() {
        assert!(subject_matches(b"orders.>", b"orders.created"));
        assert!(subject_matches(b"orders.>", b"orders.us.created"));
        assert!(!subject_matches(b"orders.>", b"users.created"));
    }

    #[test]
    fn bare_gt_matches_all() {
        assert!(subject_matches(b">", b"anything"));
        assert!(subject_matches(b">", b"a.b.c"));
    }

    #[test]
    fn mixed_wildcards() {
        assert!(subject_matches(b"*.orders.>", b"us.orders.created"));
        assert!(subject_matches(b"*.orders.>", b"eu.orders.updated.v2"));
        assert!(!subject_matches(b"*.orders.>", b"us.users.created"));
    }

}
