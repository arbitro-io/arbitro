//! Subscription — ephemeral binding between a connection and a consumer.
//!
//! A consumer can have multiple subscriptions (one per connected client).
//! The optional sub-filter narrows the consumer's filters for this specific
//! connection (e.g., consumer has "orders.>" but this client only wants "orders.created").

use arbitro_common::subject::subject_matches;
use arbitro_proto::ids::ConnId;

/// Ephemeral binding: connection ↔ consumer.
pub struct Subscription {
    pub conn_id: ConnId,
    /// Optional sub-filter that narrows the consumer's filters.
    /// None = consumer's filters apply as-is.
    pub filter: Option<Box<[u8]>>,
}

impl Subscription {
    pub fn new(conn_id: ConnId) -> Self {
        Self { conn_id, filter: None }
    }

    pub fn with_filter(conn_id: ConnId, filter: Box<[u8]>) -> Self {
        Self { conn_id, filter: Some(filter) }
    }

    /// Does this subscription accept the subject?
    /// If no sub-filter, always true (consumer-level match already done).
    #[inline]
    pub fn matches(&self, subject: &[u8]) -> bool {
        match &self.filter {
            Some(f) => subject_matches(f, subject),
            None => true,
        }
    }
}
