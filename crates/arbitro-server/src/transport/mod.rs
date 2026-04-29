//! transport/ — TCP-layer state and wire-frame dispatch.
//!
//! * `registry` — `ConnectionRegistry`: per-connection write channels.
//! * `dispatch_v2` — parse v2 wire frames and route them to the shard router.

pub mod dispatch_v2;
pub mod registry;

pub use registry::ConnectionRegistry;
