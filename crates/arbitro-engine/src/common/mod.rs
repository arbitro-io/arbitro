//! Common utilities — shared helpers for subject matching, hashing, and trie.
//!
//! Level 0 — no internal deps.

mod subject;
mod trie;

pub use subject::{subject_matches, next_token};
pub use trie::{SubjectTrie, TrieNode};

/// 32-bit wire hash. Foldhash with fixed seed → deterministic across
/// processes and versions (within a given foldhash release).
#[inline]
pub fn wire_hash_32(data: &[u8]) -> u32 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = foldhash::fast::FixedState::default().build_hasher();
    h.write(data);
    h.finish() as u32
}
