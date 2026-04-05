//! Server configuration — from environment variables.

use std::time::Duration;

/// Server configuration.
pub struct Config {
    /// TCP listen address (default: "0.0.0.0:4222").
    pub listen_addr: String,
    /// Maximum concurrent connections (default: 10_000).
    pub max_connections: u32,
    /// Write channel capacity per connection in frames (default: 8192).
    pub write_buffer_cap: usize,
    /// Idle timeout — close connections with no activity (default: 300s).
    pub idle_timeout: Duration,
    /// Keepalive interval — send Ping if no activity (default: 30s).
    pub keepalive_interval: Duration,
    /// Shutdown timeout — max wait for graceful drain (default: 10s).
    pub shutdown_timeout: Duration,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            listen_addr: env_or("ARBITRO_LISTEN", "0.0.0.0:4222"),
            max_connections: env_parse("ARBITRO_MAX_CONNECTIONS", 10_000),
            write_buffer_cap: env_parse("ARBITRO_WRITE_BUFFER_CAP", 8192),
            idle_timeout: Duration::from_secs(env_parse("ARBITRO_IDLE_TIMEOUT", 300)),
            keepalive_interval: Duration::from_secs(env_parse("ARBITRO_KEEPALIVE_INTERVAL", 30)),
            shutdown_timeout: Duration::from_secs(env_parse("ARBITRO_SHUTDOWN_TIMEOUT", 10)),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:4222".into(),
            max_connections: 10_000,
            write_buffer_cap: 8192,
            idle_timeout: Duration::from_secs(300),
            keepalive_interval: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
