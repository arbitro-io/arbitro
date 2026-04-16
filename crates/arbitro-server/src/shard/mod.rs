//! shard/ — single-threaded per-shard worker and its supporting types.
//!
//! * `command` — owned command types crossing the mpsc boundary.
//! * `drain` — reactive linear-walk drain cycle (replaces legacy
//!   claim-based drainer).
//! * `handle` — async `ShardHandle` (tx + unpark).
//! * `handlers` — command handler implementations (publish, ack, admin).
//! * `router` — `ShardRouter` spawns shard threads and routes by stream_id.
//! * `worker` — `ShardWorker` struct + run loop + dispatch.

pub mod command;
pub mod drain;
pub mod handle;
pub mod handlers;
pub mod router;
pub mod worker;

pub use handle::ShardHandle;
pub use router::ShardRouter;
pub use worker::ShardWorker;
