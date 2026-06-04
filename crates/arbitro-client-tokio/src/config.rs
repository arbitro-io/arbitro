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
    /// TLS configuration. `None` → plain TCP. Requires the `tls` feature.
    #[cfg(feature = "tls")]
    pub tls: Option<TlsConfig>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            // Server defaults to "0.0.0.0:9898" — see
            // `crates/arbitro-server/src/config.rs::ARBITRO_LISTEN`.
            addr: "127.0.0.1:9898".to_string(),
            reconnect: ReconnectPolicy::default(),
            keep_alive: KeepAlive::default(),
            write_queue_capacity: 4096,
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
