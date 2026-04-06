//! Subject Index — Trie-based subscriber matching with Bitsets.
//!
//! ## Architecture
//!
//! - Each node in the Trie represents a token (segment).
//! - Special nodes for `*` and `>` wildcards.
//! - Each node stores a `BitSet` of consumer IDs matching that pattern.

#[derive(Debug, Clone, Default)]
pub struct BitSet(Vec<u64>);

impl BitSet {
    pub fn new(size: usize) -> Self {
        Self(vec![0; (size + 63) / 64])
    }

    #[inline]
    pub fn insert(&mut self, bit: u32) {
        let idx = (bit / 64) as usize;
        if idx >= self.0.len() { 
            self.0.resize(idx + 1, 0);
        }
        self.0[idx] |= 1 << (bit % 64);
    }

    #[inline]
    pub fn remove(&mut self, bit: u32) {
        let idx = (bit / 64) as usize;
        if idx < self.0.len() {
            self.0[idx] &= !(1 << (bit % 64));
        }
    }

    #[inline]
    pub fn contains(&self, bit: u32) -> bool {
        let idx = (bit / 64) as usize;
        idx < self.0.len() && (self.0[idx] & (1 << (bit % 64))) != 0
    }

    #[inline]
    pub fn intersect(&mut self, other: &BitSet) {
        let common = self.0.len().min(other.0.len());
        for i in 0..common {
            self.0[i] &= other.0[i];
        }
        // Bitsets are treated as 0s beyond their length
        if self.0.len() > common {
            for i in common..self.0.len() {
                self.0[i] = 0;
            }
        }
    }

    #[inline]
    pub fn union(&mut self, other: &BitSet) {
        if other.0.len() > self.0.len() {
            self.0.resize(other.0.len(), 0);
        }
        for (a, b) in self.0.iter_mut().zip(other.0.iter()) {
            *a |= b;
        }
    }

    pub fn clear(&mut self) {
        for val in &mut self.0 { *val = 0; }
    }

    pub fn iter(&self) -> BitSetIter<'_> {
        BitSetIter { set: self, current_idx: 0, current_val: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.0.iter().all(|&v| v == 0)
    }
}

pub struct BitSetIter<'a> {
    set: &'a BitSet,
    current_idx: usize,
    current_val: u64,
}

impl<'a> Iterator for BitSetIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_val == 0 {
            if self.current_idx >= self.set.0.len() { return None; }
            self.current_val = self.set.0[self.current_idx];
            if self.current_val == 0 {
                self.current_idx += 1;
            } else {
                break;
            }
        }

        let bit_idx = self.current_val.trailing_zeros();
        let res = (self.current_idx * 64) as u32 + bit_idx;
        self.current_val &= !(1 << bit_idx);
        if self.current_val == 0 { self.current_idx += 1; }
        Some(res)
    }
}

// ── SubjectIndex ───────────────────────────────────────────────────────────

use std::collections::HashMap;

#[derive(Default)]
struct Node {
    children: HashMap<Vec<u8>, Node>,
    star: Option<Box<Node>>,
    gt_bitset: BitSet,
    exact_bitset: BitSet,
}

pub struct SubjectIndex {
    root: Node,
}

impl SubjectIndex {
    pub fn new() -> Self {
        Self { root: Node::default() }
    }

    pub fn insert(&mut self, pattern: &[u8], consumer_id: u32) {
        let mut current = &mut self.root;
        let tokens = pattern.split(|&b| b == b'.');

        for token in tokens {
            match token {
                b">" => {
                    current.gt_bitset.insert(consumer_id);
                    return;
                }
                b"*" => {
                    if current.star.is_none() { current.star = Some(Box::default()); }
                    current = current.star.as_mut().unwrap();
                }
                literal => {
                    current = current.children.entry(literal.to_vec()).or_default();
                }
            }
        }
        current.exact_bitset.insert(consumer_id);
    }

    pub fn remove(&mut self, pattern: &[u8], consumer_id: u32) {
        let mut current = &mut self.root;
        let tokens = pattern.split(|&b| b == b'.');

        for token in tokens {
            match token {
                b">" => {
                    current.gt_bitset.remove(consumer_id);
                    return;
                }
                b"*" => {
                    if let Some(ref mut star) = current.star {
                        current = star;
                    } else { return; }
                }
                literal => {
                    if let Some(node) = current.children.get_mut(literal) {
                        current = node;
                    } else { return; }
                }
            }
        }
        current.exact_bitset.remove(consumer_id);
    }

    pub fn matches(&self, subject: &[u8], result: &mut BitSet) {
        result.clear();
        let tokens: Vec<&[u8]> = subject.split(|&b| b == b'.').collect();
        self.match_recursive(&self.root, &tokens, 0, result);
    }

    fn match_recursive(&self, node: &Node, tokens: &[&[u8]], idx: usize, result: &mut BitSet) {
        // > (Greedy wildcard) matches one OR more remaining tokens
        if !node.gt_bitset.is_empty() && idx < tokens.len() {
            result.union(&node.gt_bitset);
        }

        if idx == tokens.len() {
            result.union(&node.exact_bitset);
            return;
        }

        let token = tokens[idx];

        // 1. Exact match
        if let Some(child) = node.children.get(token) {
            self.match_recursive(child, tokens, idx + 1, result);
        }

        // 2. * Wildcard
        if let Some(ref star) = node.star {
            self.match_recursive(star, tokens, idx + 1, result);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_basic_matching() {
        let mut idx = SubjectIndex::new();
        idx.insert(b"orders.v1.created", 1);
        idx.insert(b"orders.v1.*", 2);
        idx.insert(b"orders.>", 3);
        idx.insert(b"shipping.*", 4);

        let mut res = BitSet::new(10);
        
        idx.matches(b"orders.v1.created", &mut res);
        let matches: Vec<u32> = res.iter().collect();
        assert!(matches.contains(&1));
        assert!(matches.contains(&2));
        assert!(matches.contains(&3));
        assert!(!matches.contains(&4));
    }

    #[test]
    fn bitset_intersection() {
        let mut a = BitSet::new(10);
        let mut b = BitSet::new(10);
        a.insert(1); a.insert(2); a.insert(3);
        b.insert(2); b.insert(3); b.insert(4);

        a.intersect(&b);
        let res: Vec<u32> = a.iter().collect();
        assert_eq!(res, vec![2, 3]);
    }
}
