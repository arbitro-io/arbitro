//! arbitro-common — primitives shared between `arbitro-server`,
//! `arbitro-engine` (kernel migration), and any future driver crates.
//!
//! Contents are deliberately minimal: only pieces that (1) need to be
//! shared across crate boundaries and (2) have no heavy dependencies
//! (no tokio, no server-internal types). This crate MUST stay safe for
//! the engine to depend on — see the engine's dependency bans in
//! `.agent/rules/code-anti-patterns.md`.
//!
//! Currently extracted:
//! - `gate::Gate` — drain-delivery doorbell (AtomicBool + spin + park).
//! - `name_registry::NameRegistry` — wire ID ↔ sequential engine ID map.
//!
//! Intentionally **not** extracted (still in `arbitro-server/src/common/`):
//! - `reply` — depends on `crate::transport::ConnectionRegistry`.
//! - `session` — depends on `tokio::sync::mpsc` (banned in engine).

pub mod gate;
pub mod name_registry;

pub use gate::Gate;
pub use name_registry::NameRegistry;
