//! arbitro-server — TCP message broker.

use std::sync::Arc;
use std::path::Path;
use arbitro_metadata::MetadataLog;
use arbitro_server::{ArbitroServer, Config, TokioTransport};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "arbitro_server=info".parse().unwrap()),
        )
        .init();

    let config = Config::from_env();
    let transport = Arc::new(TokioTransport::new(config.write_buffer_cap));
    
    // Optional persistence bootstrap
    let metadata_log = if let Some(data_dir) = &config.data_dir {
        let path = Path::new(data_dir);
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        let log_path = path.join("metadata.log");
        tracing::info!(path = %log_path.display(), "loading metadata log");
        Some(Arc::new(MetadataLog::open(log_path)?))
    } else {
        None
    };

    // Initialize server with NO metadata log initially to avoid recording replay
    let server = ArbitroServer::new(config, transport, None);

    // Replay log to restore state
    if let Some(log) = metadata_log {
        log.replay(server.engine())?;
        
        // Enable recording for future actions
        tracing::info!("metadata recovery complete, enabling persistence");
        *server.engine().ctx.metadata.write() = Some(log);
    }

    server.run().await
}
