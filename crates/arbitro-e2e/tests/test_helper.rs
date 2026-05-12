use arbitro_client_tokio::{Client, ClientConfig};
use arbitro_server::{ArbitroServer, Config};
use bytes::Bytes;
use std::time::Duration;
use tokio::sync::watch;

/// Builder para configurar una instancia de TestServer.
pub struct TestServerBuilder {
    data_dir: Option<String>,
    shard_count: usize,
    shutdown_timeout: Duration,
}

impl TestServerBuilder {
    pub fn new() -> Self {
        Self {
            data_dir: None,
            shard_count: 2,
            shutdown_timeout: Duration::from_millis(50),
        }
    }

    pub fn data_dir(mut self, dir: &str) -> Self {
        self.data_dir = Some(dir.to_string());
        self
    }

    pub fn shard_count(mut self, count: usize) -> Self {
        self.shard_count = count;
        self
    }

    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    pub async fn spawn(self) -> TestServer {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);

        let (tx, rx) = watch::channel(false);
        let mut config = Config::default()
            .listen_addr(&addr)
            .shard_count(self.shard_count)
            .shutdown_timeout(self.shutdown_timeout);
            
        if let Some(ref dir) = self.data_dir {
            config = config.data_dir(dir);
        }

        let mut server = ArbitroServer::new(config);

        // Si hay data_dir, activamos la persistencia del log de comandos.
        if let Some(ref data_dir) = self.data_dir {
            if !data_dir.is_empty() {
                let path = std::path::Path::new(data_dir).join("metadata.log");
                let log = arbitro_server::command_log::CommandLog::open(path).unwrap();
                server.set_command_log(arbitro_server::command_log::SharedCommandLog::new(log));
            }
        }

        let handle = tokio::spawn(async move {
            let _ = server.run_with_shutdown(rx).await;
        });

        TestServer {
            addr,
            shutdown_tx: tx,
            handle: Some(handle),
        }
    }
}

/// Instancia de servidor en ejecución para tests.
pub struct TestServer {
    pub addr: String,
    shutdown_tx: watch::Sender<bool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl TestServer {
    /// Conexión determinista con reintentos.
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

    /// Apagado determinista.
    pub async fn shutdown(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = self.shutdown_tx.send(true);
            let _ = handle.await.expect("server task failed");
        }
    }

    /// Helper rápido para parsear IDs de respuesta.
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
