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
                    let next = if let Some((_, idx)) = self.nodes[curr]
                        .literals
                        .iter()
                        .find(|(t, _)| &**t == lit)
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
    /// Hot path — iterative, stack-allocated, zero heap during traversal.
    #[inline]
    pub fn find_matches<F>(&self, subject: &[u8], mut on_match: F)
    where
        F: FnMut(u32),
    {
        let mut stack = [(0u32, subject); 16];
        let mut sp = 1;

        while sp > 0 {
            sp -= 1;
            let (node_idx, sub) = stack[sp];
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
                if sp < 16 {
                    stack[sp] = (*idx, rest);
                    sp += 1;
                }
            }

            // `*` wildcard child
            if let Some(idx) = node.wildcard_star {
                if sp < 16 {
                    stack[sp] = (idx, rest);
                    sp += 1;
                }
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
    fn default() -> Self { Self::new() }
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
        assert_eq!(
            collect(&t, b"message.meta.premium.user_42"),
            vec![1, 2, 3]
        );
    }
}
