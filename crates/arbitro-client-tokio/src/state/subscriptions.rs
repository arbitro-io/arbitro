//! Subscription registry — placeholder.
//!
//! Publish-only MVP. The consume path will reuse
//! `arbitro_engine_v2::common::subject_trie::SubjectTrie` for wildcard
//! matching (no duplication).

#![allow(dead_code)]

#[derive(Debug, Default)]
pub(crate) struct Subscriptions;
