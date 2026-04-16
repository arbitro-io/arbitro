//! Runtime operations — hot-path + cold-path processing.
//!
//! Level 7 — depends on everything below.
//!
//! Simplified from the legacy 8-module runtime:
//! - `execute`: Command dispatch → DeltaEvents (hot path).
//! - `retire`: Shared binding retirement primitive (cold path).

pub mod execute;
pub mod retire;
