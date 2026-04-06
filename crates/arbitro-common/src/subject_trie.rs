//! SubjectTrie — High-density subject matching tree for client-side fanout.
//!
//! Stores patterns like `orders.*`, `orders.premium.>`, `>` and matches them 
//! against specific subjects like `orders.premium.meta`.
//!
//! Optimized for Hardware Sympathy:
//! - Arena-based (Vec<Node> with u32 indices) for cache locality.
//! - Iterative matching using a stack-allocated buffer (no recursion).
//! - Linear-scan for literal children (fast for low-medium branching).

/// Node in the Subject Trie.
#[derive(Default, Clone)]
pub struct Node {
    /// Literal segments -> child node index.
    /// Contiguous Vec for cache-friendly linear scan.
    pub literals: Vec<(Box<[u8]>, u32)>,
    
    /// `*` matches exactly one token.
    pub wildcard_star: Option<u32>,
    
    /// `>` matches one or more tokens (must be at the end).
    /// Stores the IDs of subscriptions that match `>`.
    pub wildcard_gt: Vec<u32>,
    
    /// IDs of subscriptions that terminate exactly at this node.
    pub subs: Vec<u32>,
}

pub struct SubjectTrie {
    nodes: Vec<Node>,
}

impl SubjectTrie {
    /// Create a new, empty SubjectTrie.
    pub fn new() -> Self {
        Self {
            nodes: vec![Node::default()],
        }
    }

    /// Insert a pattern into the Trie and associate it with a subscription ID.
    pub fn insert(&mut self, pattern: &[u8], sub_id: u32) {
        let mut curr_idx = 0;
        let mut p = pattern;

        while !p.is_empty() {
            let (token, rest) = next_token(p);
            
            match token {
                b">" => {
                    self.nodes[curr_idx].wildcard_gt.push(sub_id);
                    return; // `>` is always terminal
                }
                b"*" => {
                    let next_idx = if let Some(idx) = self.nodes[curr_idx].wildcard_star {
                        idx
                    } else {
                        let new_idx = self.nodes.len() as u32;
                        self.nodes.push(Node::default());
                        self.nodes[curr_idx as usize].wildcard_star = Some(new_idx);
                        new_idx
                    };
                    curr_idx = next_idx as usize;
                }
                _ => {
                    let next_idx = if let Some((_, idx)) = self.nodes[curr_idx].literals.iter().find(|(t, _)| &**t == token) {
                        *idx
                    } else {
                        let new_idx = self.nodes.len() as u32;
                        self.nodes.push(Node::default());
                        self.nodes[curr_idx as usize].literals.push((Box::from(token), new_idx));
                        new_idx
                    };
                    curr_idx = next_idx as usize;
                }
            }
            p = rest;
        }

        // Exact match terminal
        self.nodes[curr_idx].subs.push(sub_id);
    }

    /// Match a subject against the Trie and find all matching subscription IDs.
    ///
    /// Hot path — iterative, stack-allocated, zero heap access during traversal.
    #[inline]
    pub fn find_matches<F>(&self, subject: &[u8], mut on_match: F) 
    where
        F: FnMut(u32),
    {
        // Traversal stack: [(node_idx, remaining_subject)]
        // Using a fixed-size array to avoid heap allocation.
        // Depth 16 is plenty for subjects (e.g. a.b.c is depth 3).
        let mut stack = [(0u32, subject); 16];
        let mut stack_ptr = 1; // Start with root

        while stack_ptr > 0 {
            stack_ptr -= 1;
            let (node_idx, sub) = stack[stack_ptr];
            let node = &self.nodes[node_idx as usize];

            // 1. Check for `>` wildcards at this level
            if !sub.is_empty() && !node.wildcard_gt.is_empty() {
                for &id in &node.wildcard_gt {
                    on_match(id);
                }
            }

            // 2. Terminal?
            if sub.is_empty() {
                if !node.subs.is_empty() {
                    for &id in &node.subs {
                        on_match(id);
                    }
                }
                continue;
            }

            // 3. Extract next token and recurse
            let (token, rest) = next_token(sub);

            // Literal children
            if let Some((_, idx)) = node.literals.iter().find(|(t, _)| &**t == token) {
                if stack_ptr < 16 {
                    stack[stack_ptr] = (*idx, rest);
                    stack_ptr += 1;
                }
            }

            // `*` child
            if let Some(idx) = node.wildcard_star {
                if stack_ptr < 16 {
                    stack[stack_ptr] = (idx, rest);
                    stack_ptr += 1;
                }
            }
        }
    }

    /// Clear the Trie (reset to root only).
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.nodes.push(Node::default());
    }
}

// Internal tokenization logic (shared with subject.rs)
#[inline(always)]
fn next_token(s: &[u8]) -> (&[u8], &[u8]) {
    match s.iter().position(|&b| b == b'.') {
        Some(i) => (&s[..i], &s[i + 1..]),
        None => (s, &[]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trie_exact_match() {
        let mut trie = SubjectTrie::new();
        trie.insert(b"orders.created", 1);
        trie.insert(b"orders.updated", 2);
        
        let mut matches = Vec::new();
        trie.find_matches(b"orders.created", |id| matches.push(id));
        assert_eq!(matches, vec![1]);
    }

    #[test]
    fn trie_wildcard_star() {
        let mut trie = SubjectTrie::new();
        trie.insert(b"orders.*", 10);
        
        let mut matches = Vec::new();
        trie.find_matches(b"orders.created", |id| matches.push(id));
        assert_eq!(matches, vec![10]);
        
        matches.clear();
        trie.find_matches(b"orders.created.meta", |_| matches.push(0));
        assert!(matches.is_empty());
    }

    #[test]
    fn trie_wildcard_gt() {
        let mut trie = SubjectTrie::new();
        trie.insert(b"orders.>", 100);
        
        let mut matches = Vec::new();
        trie.find_matches(b"orders.created", |id| matches.push(id));
        assert_eq!(matches, vec![100]);
        
        matches.clear();
        trie.find_matches(b"orders.a.b.c", |id| matches.push(id));
        assert_eq!(matches, vec![100]);
    }

    #[test]
    fn multiple_matches() {
        let mut trie = SubjectTrie::new();
        trie.insert(b"orders.>", 1);
        trie.insert(b"orders.*", 2);
        trie.insert(b"orders.created", 3);
        
        let mut matches = Vec::new();
        trie.find_matches(b"orders.created", |id| matches.push(id));
        matches.sort();
        assert_eq!(matches, vec![1, 2, 3]);
    }
}
