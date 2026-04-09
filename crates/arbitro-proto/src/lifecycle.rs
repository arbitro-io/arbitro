//! Server lifecycle trait — shared contract for init/shutdown hooks.
//!
//! Any service that needs to act during server bootstrap (e.g. replay a
//! command log) or during graceful shutdown (e.g. flush buffers) implements
//! this trait and registers itself with the server.
//!
//! The server iterates registered services in order:
//! - `on_init()`:     called once during startup, before accepting connections.
//! - `on_shutdown()`: called once during graceful shutdown, after connections drain.

/// Lifecycle hooks for server-managed services.
///
/// Implementations must be `Send` so they can cross thread boundaries
/// during server setup.
pub trait LifeCycle: Send {
    /// Called once during server bootstrap, before the server accepts
    /// connections. Use this to replay logs, restore state, etc.
    fn on_init(&mut self);

    /// Called once during graceful shutdown, after connections have been
    /// drained. Use this to flush buffers, close files, etc.
    fn on_shutdown(&mut self);
}
