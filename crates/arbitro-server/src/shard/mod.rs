//! shard/ — drain thread + command thread per shard, **zero Mutex**.
//!
//! * `command` — owned command types crossing the mpsc boundary.
//! * `drain` — reactive linear-walk drain cycle (atomics + snapshot).
//! * `handle` — async `ShardHandle` (tx + unpark).
//! * `handlers` — command handler implementations (ack, subscribe, admin).
//! * `router` — `ShardRouter` spawns shard threads and routes by stream_id.
//! * `shared` — lock-free shared state (SharedCounters, SnapshotSwap).
//! * `worker` — `DrainWorker` (pure drain) + `CommandWorker` (owns engine).

pub mod accumulator;
pub mod command;
pub mod consumer_subjects;
pub mod drain;
pub mod drain_events;
pub mod handle;
pub mod handlers;
pub mod idempotency;
pub mod router;
pub mod shared;
pub mod worker;

pub use handle::ShardHandle;
pub use router::ShardRouter;
pub use worker::{CommandWorker, DrainWorker};
