//! common/ — shared primitives used across shard, transport and persistence.
//!
//! `gate` and `name_registry` live in the shared `arbitro-common` crate so
//! the upcoming kernel drainer (W4) can reach for them without depending on
//! `arbitro-server`. They are re-exported here to keep server-internal
//! call sites stable.
//!
//! `reply` and `session` remain server-local:
//! - `reply` depends on `crate::transport::ConnectionRegistry`.
//! - `session` depends on `tokio::sync::mpsc` (banned inside the engine).

pub mod reply;
pub mod session;

pub use arbitro_common::{Gate, NameRegistry};
pub use reply::{send_error, send_rep_ok, timestamp_now};
pub use session::{ConnIdGen, Session};
