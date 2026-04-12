//! common/ — shared primitives used across shard, transport and persistence.
//!
//! * `gate` — drain-delivery doorbell (AtomicBool + spin + park).
//! * `reply` — shared `RepOk` / `RepError` frame builders.
//! * `session` — per-connection transport handle and `ConnIdGen`.

pub mod gate;
pub mod name_registry;
pub mod reply;
pub mod session;

pub use gate::Gate;
pub use name_registry::NameRegistry;
pub use reply::{send_error, send_rep_ok, timestamp_now};
pub use session::{ConnIdGen, Session};
