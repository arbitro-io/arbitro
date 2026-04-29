//! common/ — shared primitives used across shard, transport and persistence.
//!
//! `gate` and `name_registry` live in the shared `arbitro-common` crate so
//! the kernel drainer can reach for them without depending on
//! `arbitro-server`. They are re-exported here to keep server-internal
//! call sites stable.
//!
//! `reply_v2` and `session` remain server-local:
//! - `reply_v2` depends on `crate::transport::ConnectionRegistry`.
//! - `session` depends on `tokio::sync::mpsc` (banned inside the engine).

pub mod reply_v2;
pub mod session;

pub use arbitro_common::{Gate, NameRegistry};
pub use reply_v2::{send_error_v2, send_rep_ok_v2};
pub use session::{ConnIdGen, Session};
