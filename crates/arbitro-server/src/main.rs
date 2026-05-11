//! arbitro-server — TCP message broker.

use arbitro_server::{ArbitroServer, Config};
use arbitro_server::command_log::{CommandLog, SharedCommandLog};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "arbitro_server=info".parse().unwrap()),
        )
        .init();

    let config = Config::from_env();
    let mut server = ArbitroServer::new(config);

    // Wire command log if data_dir is configured
    if let Some(ref dir) = server.config().data_dir {
        let path = std::path::Path::new(dir).join("metadata.log");
        let log = CommandLog::open(path)?;
        server.set_command_log(SharedCommandLog::new(log));
    }

    server.run().await
}
