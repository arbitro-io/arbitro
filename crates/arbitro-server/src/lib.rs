//! arbitro-server — sharded, single-writer message broker.
//!
//! Module layout:
//!
//! * `common/`      — `Gate`, reply builders, per-connection session.
//! * `transport/`   — `ConnectionRegistry` and wire-frame dispatch.
//! * `persistence/` — metadata `CommandLog` and replay.
//! * `shard/`       — `ShardRouter`, `ShardHandle`, `ShardWorker` and the
//!   per-role handler files under `shard/roles/`.
//! * `config`, `server` — top-level configuration and the `ArbitroServer`
//!   accept loop.

pub mod common;
pub mod config;
pub mod lifecycle_trace;
pub mod persistence;
pub mod server;
pub mod shard;
pub mod transport;

// ── Compat re-exports ─────────────────────────────────────────────────────
// Keep the old `arbitro_server::{router,command,command_log}::*` paths alive
// for tests, benches and `main.rs` without forcing a rename cascade.
pub mod router {
    pub use crate::shard::router::ShardRouter;
    /// Back-compat alias for the old `Server` name.
    pub use crate::shard::router::ShardRouter as Server;
}
pub mod command {
    pub use crate::shard::command::*;
}
pub mod command_log {
    pub use crate::persistence::command_log::*;
}

pub use config::Config;
pub use server::ArbitroServer;
pub use shard::router::ShardRouter;
/// Back-compat alias — old name for `ShardRouter`.
pub use shard::router::ShardRouter as Server;
pub use transport::ConnectionRegistry;
