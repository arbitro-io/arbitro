//! transport/ — TCP-layer state and wire-frame dispatch.
//!
//! * `registry` — `ConnectionRegistry`: per-connection write channels.
//! * `dispatch` — parse wire frames and route them to the shard router.

pub mod dispatch;
pub mod registry;

pub use registry::ConnectionRegistry;
