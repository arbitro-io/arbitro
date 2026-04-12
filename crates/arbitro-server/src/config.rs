//! Server configuration — from environment variables.

use std::time::Duration;

/// Server configuration.
pub struct Config {
    /// TCP listen address (default: "0.0.0.0:9898").
    pub listen_addr: String,
    /// Number of engine shards (default: CPU count).
    pub shard_count: usize,
    /// mpsc channel capacity per shard (default: 4096).
    pub channel_capacity: usize,
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
    /// Max messages fed into the engine ready queue per drain cycle (default: 256).
    pub max_feed_per_cycle: usize,
    /// Data directory for persistence (None = in-memory only).
    pub data_dir: Option<String>,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            listen_addr: env_or("ARBITRO_LISTEN", "0.0.0.0:9898"),
            shard_count: env_parse(
                "ARBITRO_SHARDS",
                std::thread::available_parallelism().map(|p| p.get()).unwrap_or(4),
            ),
            channel_capacity: env_parse("ARBITRO_CHANNEL_CAPACITY", 4096),
            max_feed_per_cycle: env_parse("ARBITRO_MAX_FEED_PER_CYCLE", 256),
            max_connections: env_parse("ARBITRO_MAX_CONNECTIONS", 10_000),
            write_buffer_cap: env_parse("ARBITRO_WRITE_BUFFER_CAP", 8192),
            idle_timeout: Duration::from_secs(env_parse("ARBITRO_IDLE_TIMEOUT", 300)),
            keepalive_interval: Duration::from_secs(env_parse("ARBITRO_KEEPALIVE_INTERVAL", 30)),
            shutdown_timeout: Duration::from_secs(env_parse("ARBITRO_SHUTDOWN_TIMEOUT", 10)),
            data_dir: std::env::var("ARBITRO_DATA_DIR").ok(),
        }
    }

    pub fn shard_count(mut self, count: usize) -> Self {
        self.shard_count = count;
        self
    }

    pub fn channel_capacity(mut self, cap: usize) -> Self {
        self.channel_capacity = cap;
        self
    }

    pub fn listen_addr(mut self, addr: impl Into<String>) -> Self {
        self.listen_addr = addr.into();
        self
    }

    pub fn max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }

    pub fn max_feed_per_cycle(mut self, cap: usize) -> Self {
        self.max_feed_per_cycle = cap;
        self
    }

    pub fn write_buffer_cap(mut self, cap: usize) -> Self {
        self.write_buffer_cap = cap;
        self
    }

    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    pub fn keepalive_interval(mut self, interval: Duration) -> Self {
        self.keepalive_interval = interval;
        self
    }

    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    pub fn data_dir(mut self, dir: impl Into<String>) -> Self {
        self.data_dir = Some(dir.into());
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9898".into(),
            shard_count: std::thread::available_parallelism().map(|p| p.get()).unwrap_or(4),
            channel_capacity: 4096,
            max_feed_per_cycle: 256,
            max_connections: 10_000,
            write_buffer_cap: 8192,
            idle_timeout: Duration::from_secs(300),
            keepalive_interval: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(10),
            data_dir: None,
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
