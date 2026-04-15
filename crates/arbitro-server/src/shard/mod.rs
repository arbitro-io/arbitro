//! shard/ — single-threaded per-shard worker and its supporting types.
//!
//! * `command` — owned command types crossing the mpsc boundary.
//! * `handle` — async `ShardHandle` (tx + unpark).
//! * `router` — `ShardRouter` spawns shard threads and routes by stream_id.
//! * `worker` — `ShardWorker` struct + run loop + dispatch.
//! * `roles/` — one file per role (publisher, accumulator, acker, drainer,
//!   seeder, admin). Each file adds `impl ShardWorker { ... }` with the
//!   handlers owned by that role (see `.agent/rules/roles.md`).

pub mod command;
pub mod drainer_v2;
pub mod handle;
pub mod roles;
pub mod router;
pub mod worker;

pub use handle::ShardHandle;
pub use router::ShardRouter;
pub use worker::ShardWorker;
