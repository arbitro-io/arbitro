//! arbitro-server — TCP message broker.

use std::sync::Arc;

use arbitro_engine::EngineBuilder;
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

    // Build engine with the shared transport
    let engine = EngineBuilder::new()
        .transport(transport.clone())
        .build();

    let server = ArbitroServer::new(config, engine, transport);
    server.run().await
}
