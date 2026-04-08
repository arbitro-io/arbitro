//! arbitro-server — TCP message broker.

use arbitro_server::{ArbitroServer, Config};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "arbitro_server=info".parse().unwrap()),
        )
        .init();

    let config = Config::from_env();
    let server = ArbitroServer::new(config);
    server.run().await
}
