//! EngineError, ErrorCode — domain error types.
//!
//! Level 0 — depends only on `types`.

use std::fmt;

/// Error codes for engine operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum ErrorCode {
    // ── Graph errors ─────────────────────────────────────────────────────
    /// Slab key has stale generation — entity was removed and slot reused.
    StaleGeneration = 1,
    /// Slab slot is vacant — entity was already removed.
    SlotVacant = 2,
    /// Slab is at maximum capacity.
    SlabFull = 3,

    // ── Catalog errors ───────────────────────────────────────────────────
    /// Stream not found.
    StreamNotFound = 100,
    /// Consumer not found.
    ConsumerNotFound = 101,
    /// Subscription not found.
    SubscriptionNotFound = 102,
    /// Queue not found.
    QueueNotFound = 103,
    /// Duplicate entity — already exists.
    DuplicateEntity = 104,

    // ── Runtime errors ───────────────────────────────────────────────────
    /// Connection not found.
    ConnectionNotFound = 200,
    /// Pending not found — already acked or timed out.
    PendingNotFound = 201,
    /// Credit exhausted — cannot deliver.
    CreditExhausted = 202,
    /// Idempotency duplicate — message already processed.
    IdempotencyDuplicate = 203,
    /// Consumer is paused.
    ConsumerPaused = 204,
    /// Queue is paused.
    QueuePaused = 205,

    // ── Plugin errors ────────────────────────────────────────────────────
    /// Plugin not registered.
    PluginNotFound = 300,
    /// Plugin init failed.
    PluginInitFailed = 301,
    /// Edge type not registered.
    EdgeNotFound = 302,

    // ── Config errors ────────────────────────────────────────────────────
    /// Config log I/O error.
    ConfigIoError = 400,
    /// Config command invalid.
    ConfigInvalid = 401,
}

/// The primary error type for all engine operations.
#[derive(Debug)]
pub enum EngineError {
    /// Slab lookup failed — stale generation or vacant slot.
    StaleKey {
        code: ErrorCode,
        entity: &'static str,
        index: u32,
        expected_gen: u32,
        actual_gen: u32,
    },

    /// Entity not found by domain ID.
    NotFound {
        code: ErrorCode,
        entity: &'static str,
    },

    /// A limit was exceeded.
    LimitExceeded {
        code: ErrorCode,
        detail: &'static str,
    },

    /// Duplicate entity.
    Duplicate {
        code: ErrorCode,
        entity: &'static str,
    },

    /// Plugin-related error.
    Plugin {
        code: ErrorCode,
        plugin: &'static str,
    },

    /// Config/IO error.
    Config {
        code: ErrorCode,
        detail: String,
    },
}

impl fmt::Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EngineError::StaleKey { entity, index, expected_gen, actual_gen, .. } => {
                write!(f, "stale key for {entity}[{index}]: expected gen {expected_gen}, got {actual_gen}")
            }
            EngineError::NotFound { entity, .. } => {
                write!(f, "{entity} not found")
            }
            EngineError::LimitExceeded { detail, .. } => {
                write!(f, "limit exceeded: {detail}")
            }
            EngineError::Duplicate { entity, .. } => {
                write!(f, "duplicate {entity}")
            }
            EngineError::Plugin { plugin, .. } => {
                write!(f, "plugin error: {plugin}")
            }
            EngineError::Config { detail, .. } => {
                write!(f, "config error: {detail}")
            }
        }
    }
}

impl std::error::Error for EngineError {}

impl EngineError {
    #[inline]
    pub fn code(&self) -> ErrorCode {
        match self {
            EngineError::StaleKey { code, .. }
            | EngineError::NotFound { code, .. }
            | EngineError::LimitExceeded { code, .. }
            | EngineError::Duplicate { code, .. }
            | EngineError::Plugin { code, .. }
            | EngineError::Config { code, .. } => *code,
        }
    }

    pub fn stream_not_found() -> Self {
        EngineError::NotFound { code: ErrorCode::StreamNotFound, entity: "stream" }
    }

    pub fn consumer_not_found() -> Self {
        EngineError::NotFound { code: ErrorCode::ConsumerNotFound, entity: "consumer" }
    }

    pub fn subscription_not_found() -> Self {
        EngineError::NotFound { code: ErrorCode::SubscriptionNotFound, entity: "subscription" }
    }

    pub fn queue_not_found() -> Self {
        EngineError::NotFound { code: ErrorCode::QueueNotFound, entity: "queue" }
    }

    pub fn connection_not_found() -> Self {
        EngineError::NotFound { code: ErrorCode::ConnectionNotFound, entity: "connection" }
    }

    pub fn pending_not_found() -> Self {
        EngineError::NotFound { code: ErrorCode::PendingNotFound, entity: "pending" }
    }

    pub fn credit_exhausted() -> Self {
        EngineError::LimitExceeded { code: ErrorCode::CreditExhausted, detail: "credit exhausted" }
    }

    pub fn idempotency_duplicate() -> Self {
        EngineError::Duplicate { code: ErrorCode::IdempotencyDuplicate, entity: "idempotency key" }
    }

    pub fn plugin_not_found(name: &'static str) -> Self {
        EngineError::Plugin { code: ErrorCode::PluginNotFound, plugin: name }
    }

    pub fn edge_not_found(name: &'static str) -> Self {
        EngineError::Plugin { code: ErrorCode::EdgeNotFound, plugin: name }
    }
}

/// Convenience alias.
pub type EngineResult<T> = Result<T, EngineError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_codes_are_distinct() {
        assert_ne!(ErrorCode::StaleGeneration as u16, ErrorCode::SlotVacant as u16);
        assert_ne!(ErrorCode::StreamNotFound as u16, ErrorCode::ConsumerNotFound as u16);
    }

    #[test]
    fn error_display() {
        let e = EngineError::stream_not_found();
        assert_eq!(e.to_string(), "stream not found");
        assert_eq!(e.code(), ErrorCode::StreamNotFound);
    }

    #[test]
    fn stale_key_display() {
        let e = EngineError::StaleKey {
            code: ErrorCode::StaleGeneration,
            entity: "pending",
            index: 42,
            expected_gen: 5,
            actual_gen: 3,
        };
        assert!(e.to_string().contains("pending[42]"));
        assert!(e.to_string().contains("gen 5"));
    }
}
