//! Client configuration — connection target, reconnect policy, heartbeat.
//!
//! Values mirror `arbitro-client::ClientConfig` defaults. Fields will be
//! consumed by `conn::session` (Step 4) and `conn::reconnect` (Step 4).

use std::time::Duration;

/// Top-level client configuration.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Broker address (`host:port`).
    pub addr: String,
    /// Reconnection backoff policy.
    pub reconnect: ReconnectPolicy,
    /// Heartbeat / dead-connection detection.
    pub keep_alive: KeepAlive,
    /// Bound for the writer mpsc (back-pressure threshold).
    pub write_queue_capacity: usize,
    /// TTL for hot-tier deferred acks before the sweep drops them unpersisted.
    pub ack_pending_ttl: Duration,
    /// Durable redelivery-dedup store (WAL), including **where it lives**.
    ///
    /// `None` (the default) means no dedup store: a redelivered message runs
    /// the handler again, which is the plain at-least-once contract.
    ///
    /// `Some(cfg)` makes [`Client::connect`](crate::Client::connect) open a
    /// [`Wal`](crate::ackstore::wal::Wal) so a restarted process recognizes
    /// messages it already processed. The storage path is
    /// [`WalConfig::dir`](crate::ackstore::WalConfig::dir):
    ///
    /// ```ignore
    /// // explicit path — what a packaged service should do
    /// ClientConfig { ack_store: Some(WalConfig::new("/var/lib/myapp/ackstore")), ..d }
    ///
    /// // platform default path (ARBITRO_ACKSTORE_DIR, else the OS state dir)
    /// ClientConfig { ack_store: Some(WalConfig::default()), ..d }
    /// ```
    ///
    /// See [`WalConfig`](crate::ackstore::WalConfig) for the default-path rules
    /// and the one-writer-per-directory requirement.
    ///
    /// [`Client::connect_with_ackstore`](crate::Client::connect_with_ackstore)
    /// still takes precedence when a caller wants a fully custom
    /// [`Store`](crate::ackstore::Store) implementation.
    pub ack_store: Option<crate::ackstore::WalConfig>,
    /// Bearer token sent once per connection, right after the Hello frame.
    ///
    /// `None` (the default, or `ARBITRO_TOKEN` unset) sends no `Auth` frame at
    /// all, which is what a broker with authentication disabled expects.
    ///
    /// The token is re-sent on every reconnect — it travels in the handshake,
    /// so a reconnect cannot silently drop it. Authentication happens once per
    /// connection and is never re-checked per message; rotating a credential
    /// means reconnecting, not sending a second `Auth`.
    ///
    /// A wrong token is terminal: the broker replies `AuthFailed` and closes,
    /// and the client stops reconnecting instead of hammering the broker with
    /// a credential that will never work.
    ///
    /// Capped at 4096 bytes — the broker rejects anything larger (it expects a
    /// token, never a payload).
    pub auth_token: Option<String>,
    /// TLS configuration. `None` → plain TCP. Requires the `tls` feature.
    #[cfg(feature = "tls")]
    pub tls: Option<TlsConfig>,
}

/// Maximum accepted `auth_token` length. Mirrors the broker's `Auth` frame cap
/// (`MAX_AUTH_FRAME_BODY` in `arbitro-server`): a longer token is rejected at
/// the socket, so catching it locally turns a confusing disconnect into a
/// clear configuration error.
pub const MAX_AUTH_TOKEN_LEN: usize = 4096;

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            // Server defaults to "0.0.0.0:9898" — see
            // `crates/arbitro-server/src/config.rs::ARBITRO_LISTEN`.
            addr: "127.0.0.1:9898".to_string(),
            reconnect: ReconnectPolicy::default(),
            keep_alive: KeepAlive::default(),
            write_queue_capacity: 4096,
            ack_pending_ttl: Duration::from_secs(24 * 3600),
            ack_store: None,
            // Env fallback so a deployment can turn auth on without a code
            // change. An explicit field set by the caller overrides this,
            // because `Default` runs first and the caller assigns after.
            auth_token: std::env::var("ARBITRO_TOKEN").ok().filter(|t| !t.is_empty()),
            #[cfg(feature = "tls")]
            tls: None,
        }
    }
}

/// TLS configuration for the client connection.
///
/// When provided, the client wraps the underlying TCP stream with a
/// TLS layer using `tokio-rustls`. The server name is used for SNI
/// and certificate verification.
#[cfg(feature = "tls")]
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Server name for SNI + cert verification (e.g. "broker.example.com").
    pub server_name: String,
    /// Accept invalid/self-signed certs. **Dangerous** — only for dev.
    pub danger_accept_invalid_certs: bool,
}

/// Decorrelated-jitter backoff policy (AWS algorithm):
/// `next = min(cap, rand(base, prev * 3))`.
#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    /// Initial / minimum delay.
    pub base: Duration,
    /// Maximum single-attempt delay.
    pub cap: Duration,
    /// Total attempts before giving up. `None` = retry forever.
    pub max_attempts: Option<u32>,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            base: Duration::from_millis(100),
            cap: Duration::from_secs(30),
            max_attempts: None,
        }
    }
}

/// Heartbeat watchdog — detects dead connections faster than TCP keepalive.
#[derive(Debug, Clone)]
pub struct KeepAlive {
    /// Send a Ping when the connection is idle for this long.
    pub interval: Duration,
    /// Declare the connection dead if no Pong arrives within this budget.
    pub timeout: Duration,
}

impl Default for KeepAlive {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(60),
        }
    }
}
