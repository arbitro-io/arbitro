//! transport/ — TCP-layer state and wire-frame dispatch.
//!
//! * `registry` — `ConnectionRegistry`: per-connection write channels.
//! * `dispatch_v2` — parse v2 wire frames and route them to the shard router.
//! * `tls` — optional TLS support via `tokio-rustls` (feature `tls`).

pub mod dispatch_v2;
pub mod registry;
#[cfg(feature = "tls")]
pub mod tls;

pub use registry::ConnectionRegistry;
