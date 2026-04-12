//! shard/roles/ — one file per shard role.
//!
//! Each file extends `ShardWorker` with the handlers owned by that role,
//! following the hot/cold path fences in `.agent/rules/roles.md`.
//!
//! | Role        | Path | Purpose                                              |
//! |-------------|------|------------------------------------------------------|
//! | publisher   | hot  | persist publish requests and ack directly           |
//! | accumulator | hot  | buffer small publishes into one batched append      |
//! | acker       | hot  | feed ack/nack back into the engine                  |
//! | drainer     | hot  | touch the journal, feed engine, ship `RepBatch`     |
//! | seeder      | cold | bulk-load store entries into engine ready state     |
//! | admin       | cold | stream/consumer/subscription lifecycle management   |

pub mod acker;
pub mod accumulator;
pub mod admin;
pub mod drainer;
pub mod publisher;
pub mod seeder;
