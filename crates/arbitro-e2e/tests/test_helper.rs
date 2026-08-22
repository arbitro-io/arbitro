use arbitro_client_tokio::{Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;
use std::time::Duration;
use tokio::sync::watch;

/// Builder for configuring a TestServer instance.
#[allow(dead_code)]
pub struct TestServerBuilder {
    data_dir: Option<String>,
    shard_count: usize,
    shutdown_timeout: Duration,
    max_frame_size: Option<usize>,
    idle_timeout: Option<Duration>,
    keepalive_interval: Option<Duration>,
    write_buffer_cap: Option<usize>,
    drain_stall_evict_ms: Option<u64>,
    max_feed_per_cycle: Option<usize>,
    max_connections: Option<u32>,
    auth_token: Option<String>,
    shard_listeners: bool,
}

impl Default for TestServerBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl TestServerBuilder {
    pub fn new() -> Self {
        Self {
            data_dir: None,
            shard_count: 2,
            shutdown_timeout: Duration::from_millis(50),
            max_frame_size: None,
            idle_timeout: None,
            keepalive_interval: None,
            write_buffer_cap: None,
            drain_stall_evict_ms: None,
            max_feed_per_cycle: None,
            max_connections: None,
            auth_token: None,
            shard_listeners: false,
        }
    }

    /// Require this shared bearer token from every connection.
    pub fn auth_token(mut self, token: &str) -> Self {
        self.auth_token = Some(token.to_string());
        self
    }

    pub fn data_dir(mut self, dir: &str) -> Self {
        self.data_dir = Some(dir.to_string());
        self
    }

    pub fn shard_count(mut self, count: usize) -> Self {
        self.shard_count = count;
        self
    }

    pub fn shard_listeners(mut self, on: bool) -> Self {
        self.shard_listeners = on;
        self
    }

    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    pub fn max_frame_size(mut self, size: usize) -> Self {
        self.max_frame_size = Some(size);
        self
    }

    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }

    pub fn keepalive_interval(mut self, interval: Duration) -> Self {
        self.keepalive_interval = Some(interval);
        self
    }

    /// Per-connection write channel capacity in frames (ROB-23 tests use
    /// a tiny cap so a non-reading consumer backpressures the drain fast).
    pub fn write_buffer_cap(mut self, cap: usize) -> Self {
        self.write_buffer_cap = Some(cap);
        self
    }

    /// ROB-23: delivery-stall eviction window in milliseconds.
    pub fn drain_stall_evict_ms(mut self, ms: u64) -> Self {
        self.drain_stall_evict_ms = Some(ms);
        self
    }

    /// Max store entries fed into the drain per cycle (frame batching cap).
    pub fn max_feed_per_cycle(mut self, cap: usize) -> Self {
        self.max_feed_per_cycle = Some(cap);
        self
    }

    /// Accepted connection ceiling — part of matching the throughput bench's
    /// server configuration exactly.
    pub fn max_connections(mut self, max: u32) -> Self {
        self.max_connections = Some(max);
        self
    }

    pub async fn spawn(self) -> TestServer {
        // Bind `:0` and hand the LIVE listener to the server. Dropping the
        // listener and letting the server re-bind the port opened a TOCTOU
        // window where a parallel test could grab the port in the gap
        // (intermittent "Failed to connect" / cross-server flakes under
        // full-suite parallel load).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        self.spawn_inner(&addr, Some(listener)).await
    }

    /// Spawn a server on a specific address (for reconnect tests).
    pub async fn spawn_on(self, addr: &str) -> TestServer {
        self.spawn_inner(addr, None).await
    }

    async fn spawn_inner(
        self,
        addr: &str,
        listener: Option<tokio::net::TcpListener>,
    ) -> TestServer {
        let (tx, rx) = watch::channel(false);
        let mut config = Config::default()
            .listen_addr(addr)
            .shard_count(self.shard_count)
            .shard_listeners(self.shard_listeners)
            .shutdown_timeout(self.shutdown_timeout);

        if let Some(size) = self.max_frame_size {
            config = config.max_frame_size(size);
        }
        if let Some(timeout) = self.idle_timeout {
            config = config.idle_timeout(timeout);
        }
        if let Some(interval) = self.keepalive_interval {
            config = config.keepalive_interval(interval);
        }
        if let Some(cap) = self.write_buffer_cap {
            config = config.write_buffer_cap(cap);
        }
        if let Some(ms) = self.drain_stall_evict_ms {
            config = config.drain_stall_evict_ms(ms);
        }
        if let Some(cap) = self.max_feed_per_cycle {
            config = config.max_feed_per_cycle(cap);
        }
        if let Some(max) = self.max_connections {
            config = config.max_connections(max);
        }

        if let Some(ref dir) = self.data_dir {
            config = config.data_dir(dir);
        }
        if let Some(ref token) = self.auth_token {
            config.auth_token = Some(token.clone());
        }

        let mut server = ArbitroServer::new(config);

        // Hand over the pre-bound accept socket (no drop-and-rebind gap).
        if let Some(listener) = listener {
            server.set_listener(listener);
        }

        // Enable command-log persistence when data_dir is set.
        if let Some(ref data_dir) = self.data_dir {
            if !data_dir.is_empty() {
                let path = std::path::Path::new(data_dir).join("metadata.log");
                let log = arbitro_server::command_log::CommandLog::open(path).unwrap();
                server.set_command_log(arbitro_server::command_log::SharedCommandLog::new(log));
            }
        }

        // Grab the shared catalog before `server` moves into the task.
        let names = std::sync::Arc::clone(server.server().names());

        let handle = tokio::spawn(async move {
            let _ = server.run_with_shutdown(rx).await;
        });

        TestServer {
            addr: addr.to_string(),
            shutdown_tx: tx,
            handle: Some(handle),
            names,
        }
    }
}

/// Running server instance for tests.
#[allow(dead_code)]
pub struct TestServer {
    pub addr: String,
    shutdown_tx: watch::Sender<bool>,
    handle: Option<tokio::task::JoinHandle<()>>,
    /// Captured before the server moves into its task, so tests can inspect
    /// and steer catalog state (stream placement, filters) that has no wire
    /// representation.
    names: std::sync::Arc<arbitro_common::NameRegistry>,
}

#[allow(dead_code)]
impl TestServer {
    /// Connect with deterministic retries.
    pub async fn connect(&self) -> Client {
        Self::connect_to(&self.addr).await
    }

    pub async fn connect_to(addr: &str) -> Client {
        for _ in 0..100 {
            if let Ok(c) = Client::connect(ClientConfig {
                addr: addr.to_string(),
                ..ClientConfig::default()
            })
            .await
            {
                return c;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("Failed to connect to {}", addr);
    }

    /// Connect with a custom `ClientConfig` (e.g. to disable auto-reconnect
    /// so a disconnect is observable as an error rather than being masked
    /// by a background reconnect).
    pub async fn connect_with_config(&self, cfg: ClientConfig) -> Client {
        for _ in 0..100 {
            if let Ok(c) = Client::connect(ClientConfig {
                addr: self.addr.clone(),
                ..cfg.clone()
            })
            .await
            {
                return c;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("Failed to connect to {}", self.addr);
    }

    /// Deterministic shutdown.
    pub async fn shutdown(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = self.shutdown_tx.send(true);
            handle.await.expect("server task failed");
        }
    }

    /// The shared name registry — per-stream catalog state that never
    /// crosses the wire (placement, filters, quotas).
    pub fn names(&self) -> &arbitro_common::NameRegistry {
        &self.names
    }

    /// Quick helper to parse response IDs.
    pub fn parse_id(resp: &Bytes) -> u32 {
        u64::from_le_bytes(resp[..8].try_into().unwrap()) as u32
    }

    pub fn stream_count(resp: &Bytes) -> usize {
        u32::from_le_bytes(resp[..4].try_into().unwrap()) as usize
    }

    pub fn stream_names(resp: &Bytes) -> Vec<Vec<u8>> {
        let count = u32::from_le_bytes(resp[..4].try_into().unwrap()) as usize;
        let mut names = Vec::with_capacity(count);
        let mut pos = 4usize;
        for _ in 0..count {
            pos += 4; // wire_id
            let name_len = u16::from_le_bytes(resp[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            names.push(resp[pos..pos + name_len].to_vec());
            pos += name_len;
        }
        names
    }

    pub fn consumer_count(resp: &Bytes) -> usize {
        u32::from_le_bytes(resp[..4].try_into().unwrap()) as usize
    }

    pub fn find_stream_id(resp: &Bytes, name: &[u8]) -> Option<u32> {
        let count = u32::from_le_bytes(resp[..4].try_into().unwrap()) as usize;
        let mut pos = 4usize;
        for _ in 0..count {
            let wire_id = u32::from_le_bytes(resp[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let name_len = u16::from_le_bytes(resp[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let current_name = &resp[pos..pos + name_len];
            if current_name == name {
                return Some(wire_id);
            }
            pos += name_len;
        }
        None
    }
}
