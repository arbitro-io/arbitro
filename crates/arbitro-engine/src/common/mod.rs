//! Common utilities — shared helpers for subject matching, hashing, and trie.
//!
//! Level 0 — no internal deps.

mod subject;
mod trie;

pub use subject::{subject_matches, next_token};
pub use trie::{SubjectTrie, TrieNode};

/// FNV-1a 32-bit hash. Deterministic, fast, no randomness.
#[inline]
pub fn fnv1a_32(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &b in data {
        hash ^= b as u32;
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}
