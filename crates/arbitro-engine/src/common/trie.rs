//! SubjectTrie — arena-based subject matching tree.
//!
//! Stores patterns like `orders.*`, `orders.premium.>`, `>` and matches them
//! against concrete subjects like `orders.premium.meta`.
//!
//! Optimized for hardware sympathy:
//! - Arena-based (`Vec<TrieNode>` with `u32` indices) for cache locality.
//! - Iterative matching using a stack-allocated buffer (no recursion).
//! - Linear scan for literal children (fast for branching < ~20).

use super::subject::next_token;
use smallvec::SmallVec;
use std::fmt;

/// Inline stack capacity before `find_matches` spills to the heap. Covers
/// all but pathologically deep/branchy subject trees without allocating.
const STACK_INLINE_CAP: usize = 16;

/// Error returned when a subject pattern fails validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatternError {
    /// Pattern is empty (zero bytes).
    Empty,
    /// Consecutive dots produce an empty token (e.g. `orders..created`).
    EmptyToken,
    /// Tokens appear after `>`, which must be the last token
    /// (e.g. `orders.>.eu`).
    TokensAfterGt,
}

impl fmt::Display for PatternError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PatternError::Empty => write!(f, "pattern is empty"),
            PatternError::EmptyToken => {
                write!(f, "pattern contains empty token (consecutive dots)")
            }
            PatternError::TokensAfterGt => {
                write!(f, "`>` must be the last token in a pattern")
            }
        }
    }
}

impl std::error::Error for PatternError {}

/// Validate a subject pattern before insertion into the trie.
///
/// Rules enforced:
/// 1. Pattern must not be empty.
/// 2. No empty tokens (consecutive dots like `orders..created`).
/// 3. `>` must be the last token — trailing tokens are rejected
///    (e.g. `orders.>.eu` is invalid because `>` is greedy).
/// 4. `*` is allowed in any position (single-level wildcard).
pub fn validate_pattern(pattern: &[u8]) -> Result<(), PatternError> {
    if pattern.is_empty() {
        return Err(PatternError::Empty);
    }

    // Leading or trailing dot means an empty first/last token.
    if pattern[0] == b'.' || pattern[pattern.len() - 1] == b'.' {
        return Err(PatternError::EmptyToken);
    }

    let mut p = pattern;
    while !p.is_empty() {
        let (token, rest) = next_token(p);

        // Empty token means consecutive dots (e.g. `orders..created`).
        if token.is_empty() {
            return Err(PatternError::EmptyToken);
        }

        // `>` must be the final token — no more tokens after it.
        if token == b">" && !rest.is_empty() {
            return Err(PatternError::TokensAfterGt);
        }

        p = rest;
    }

    Ok(())
}

/// Node in the subject trie. Stored contiguously in arena.
#[derive(Default, Clone)]
pub struct TrieNode {
    /// Literal segments → child node index. Linear scan.
    pub literals: Vec<(Box<[u8]>, u32)>,
    /// `*` matches exactly one token → child node index.
    pub wildcard_star: Option<u32>,
    /// `>` matches one or more tokens. Stores subscriber IDs.
    pub wildcard_gt: Vec<u32>,
    /// IDs of subscriptions terminating exactly at this node.
    pub subs: Vec<u32>,
}

/// Arena-based subject trie. All nodes in a contiguous `Vec`.
#[derive(Clone)]
pub struct SubjectTrie {
    nodes: Vec<TrieNode>,
}

impl SubjectTrie {
    pub fn new() -> Self {
        Self {
            nodes: vec![TrieNode::default()],
        }
    }

    /// Insert a pattern into the trie with a subscription ID.
    /// Management path — may allocate.
    pub fn insert(&mut self, pattern: &[u8], sub_id: u32) {
        let mut curr = 0usize;
        let mut p = pattern;

        while !p.is_empty() {
            let (token, rest) = next_token(p);

            match token {
                b">" => {
                    self.nodes[curr].wildcard_gt.push(sub_id);
                    return;
                }
                b"*" => {
                    let next = if let Some(idx) = self.nodes[curr].wildcard_star {
                        idx as usize
                    } else {
                        let idx = self.nodes.len();
                        self.nodes.push(TrieNode::default());
                        self.nodes[curr].wildcard_star = Some(idx as u32);
                        idx
                    };
                    curr = next;
                }
                lit => {
                    let next = if let Some((_, idx)) =
                        self.nodes[curr].literals.iter().find(|(t, _)| &**t == lit)
                    {
                        *idx as usize
                    } else {
                        let idx = self.nodes.len();
                        self.nodes.push(TrieNode::default());
                        self.nodes[curr].literals.push((Box::from(lit), idx as u32));
                        idx
                    };
                    curr = next;
                }
            }
            p = rest;
        }

        self.nodes[curr].subs.push(sub_id);
    }

    /// Match a subject against the trie. Calls `on_match` for each hit.
    ///
    /// Hot path — iterative, stack-allocated for the common case. Spills
    /// to the heap only for pathologically deep/branchy subjects
    /// (> `STACK_INLINE_CAP` simultaneous frontier nodes) instead of
    /// silently dropping matches like a fixed-size array would (ROB-14).
    #[inline]
    pub fn find_matches<F>(&self, subject: &[u8], mut on_match: F)
    where
        F: FnMut(u32),
    {
        let mut stack: SmallVec<[(u32, &[u8]); STACK_INLINE_CAP]> = SmallVec::new();
        stack.push((0u32, subject));

        while let Some((node_idx, sub)) = stack.pop() {
            let node = &self.nodes[node_idx as usize];

            // `>` at this level matches everything remaining
            if !sub.is_empty() && !node.wildcard_gt.is_empty() {
                for &id in &node.wildcard_gt {
                    on_match(id);
                }
            }

            // Terminal: all tokens consumed
            if sub.is_empty() {
                for &id in &node.subs {
                    on_match(id);
                }
                continue;
            }

            let (token, rest) = next_token(sub);

            // Exact literal child
            if let Some((_, idx)) = node.literals.iter().find(|(t, _)| &**t == token) {
                stack.push((*idx, rest));
            }

            // `*` wildcard child
            if let Some(idx) = node.wildcard_star {
                stack.push((idx, rest));
            }
        }
    }

    /// Number of nodes in the arena.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Clear the trie (reset to root only).
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.nodes.push(TrieNode::default());
    }
}

impl Default for SubjectTrie {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(trie: &SubjectTrie, subject: &[u8]) -> Vec<u32> {
        let mut out = Vec::new();
        trie.find_matches(subject, |id| out.push(id));
        out.sort();
        out
    }

    #[test]
    fn exact_match() {
        let mut t = SubjectTrie::new();
        t.insert(b"orders.created", 1);
        t.insert(b"orders.updated", 2);
        assert_eq!(collect(&t, b"orders.created"), vec![1]);
    }

    #[test]
    fn star_wildcard() {
        let mut t = SubjectTrie::new();
        t.insert(b"orders.*", 10);
        assert_eq!(collect(&t, b"orders.created"), vec![10]);
        assert!(collect(&t, b"orders.a.b").is_empty());
    }

    #[test]
    fn gt_wildcard() {
        let mut t = SubjectTrie::new();
        t.insert(b"orders.>", 100);
        assert_eq!(collect(&t, b"orders.created"), vec![100]);
        assert_eq!(collect(&t, b"orders.a.b.c"), vec![100]);
    }

    #[test]
    fn multiple_matches() {
        let mut t = SubjectTrie::new();
        t.insert(b"orders.>", 1);
        t.insert(b"orders.*", 2);
        t.insert(b"orders.created", 3);
        assert_eq!(collect(&t, b"orders.created"), vec![1, 2, 3]);
    }

    #[test]
    fn four_level_with_wildcards() {
        let mut t = SubjectTrie::new();
        t.insert(b"message.meta.premium.*", 1);
        t.insert(b"message.>", 2);
        t.insert(b"*.meta.>", 3);
        assert_eq!(collect(&t, b"message.meta.premium.user_42"), vec![1, 2, 3]);
    }

    // ── validate_pattern tests ──────────────────────────────────────────

    #[test]
    fn validate_empty_pattern() {
        assert_eq!(validate_pattern(b""), Err(PatternError::Empty));
    }

    #[test]
    fn validate_empty_token_consecutive_dots() {
        assert_eq!(
            validate_pattern(b"orders..created"),
            Err(PatternError::EmptyToken)
        );
    }

    #[test]
    fn validate_empty_token_leading_dot() {
        assert_eq!(validate_pattern(b".orders"), Err(PatternError::EmptyToken));
    }

    #[test]
    fn validate_empty_token_trailing_dot() {
        assert_eq!(validate_pattern(b"orders."), Err(PatternError::EmptyToken));
    }

    #[test]
    fn validate_tokens_after_gt() {
        assert_eq!(
            validate_pattern(b"orders.>.eu"),
            Err(PatternError::TokensAfterGt)
        );
    }

    #[test]
    fn validate_gt_not_last() {
        assert_eq!(validate_pattern(b">.foo"), Err(PatternError::TokensAfterGt));
    }

    #[test]
    fn validate_valid_patterns() {
        assert!(validate_pattern(b"orders.created").is_ok());
        assert!(validate_pattern(b"orders.*").is_ok());
        assert!(validate_pattern(b"orders.>").is_ok());
        assert!(validate_pattern(b">").is_ok());
        assert!(validate_pattern(b"*.orders.>").is_ok());
        assert!(validate_pattern(b"orders.*.created").is_ok());
        assert!(validate_pattern(b"a.b.c.d").is_ok());
    }
}
