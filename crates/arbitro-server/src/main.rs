//! arbitro-server — TCP message broker.

use arbitro_server::command_log::{CommandLog, SharedCommandLog};
use arbitro_server::{ArbitroServer, Config};

#[tokio::main]
async fn main() -> std::io::Result<()> {
    // ── CLI flags (no deps — just std::env) ───────────────────────────
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--version" || a == "-V") {
        println!("arbitro-server {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("arbitro-server {}", env!("CARGO_PKG_VERSION"));
        println!();
        println!("High-performance message broker.");
        println!();
        println!("USAGE: arbitro-server [OPTIONS]");
        println!();
        println!("OPTIONS:");
        println!("  -V, --version  Print version and exit");
        println!("  -h, --help     Print this help and exit");
        println!();
        println!("All configuration is via environment variables:");
        println!("  ARBITRO_LISTEN            Listen address (default: 0.0.0.0:9898)");
        println!("  ARBITRO_SHARDS            Number of engine shards (default: CPU count)");
        println!("  ARBITRO_CHANNEL_CAPACITY  mpsc channel capacity per shard (default: 4096)");
        println!("  ARBITRO_MAX_CONNECTIONS   Max concurrent connections (default: 10000)");
        println!("  ARBITRO_MAX_FRAME_SIZE    Max frame body bytes (default: 67108864 = 64 MiB)");
        println!("  ARBITRO_WRITE_BUFFER_CAP  Write channel capacity per conn (default: 8192)");
        println!("  ARBITRO_IDLE_TIMEOUT      Idle timeout seconds (default: 300)");
        println!("  ARBITRO_KEEPALIVE_INTERVAL Keepalive interval seconds (default: 30)");
        println!("  ARBITRO_SHUTDOWN_TIMEOUT  Graceful shutdown seconds (default: 10)");
        println!("  ARBITRO_METRICS_INTERVAL  Metrics log interval seconds (default: 5)");
        println!("  ARBITRO_MAX_FEED_PER_CYCLE  Messages per drain cycle (default: 256)");
        println!("  ARBITRO_DRAIN_BATCH_SIZE  Entries per RepBatch frame (default: 256)");
        println!("  ARBITRO_DATA_DIR          Persistence directory (unset = in-memory)");
        println!("  ARBITRO_TLS_CERT          TLS certificate PEM path");
        println!("  ARBITRO_TLS_KEY           TLS private key PEM path");
        println!("  ARBITRO_AUTH_TOKEN        Auth token for Hello frame");
        println!("  ARBITRO_MAX_OPS_PER_SEC  Max frames/sec per connection (0 = unlimited)");
        println!("  ARBITRO_FSYNC_POLICY     Metadata fsync: \"every\" (default) or \"none\"");
        println!("  ARBITRO_LOG               tracing filter directive (SIGHUP reloads)");
        return Ok(());
    }

    // L16: install a reloadable EnvFilter so SIGHUP can swap the filter
    // at runtime without a restart. The handle lives in a static so the
    // (unix-only) SIGHUP task can grab it on each signal.
    let initial = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "arbitro_server=info".parse().unwrap());
    #[cfg_attr(not(unix), allow(unused_variables))]
    let (filter, reload_handle) = tracing_subscriber::reload::Layer::new(initial);
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut sighup) = signal(SignalKind::hangup()) {
            tokio::spawn(async move {
                while sighup.recv().await.is_some() {
                    let new_directive = std::env::var("ARBITRO_LOG")
                        .unwrap_or_else(|_| "arbitro_server=info".to_string());
                    match new_directive.parse::<tracing_subscriber::EnvFilter>() {
                        Ok(f) => match reload_handle.reload(f) {
                            Ok(()) => tracing::info!(
                                directive = %new_directive,
                                "SIGHUP: log filter reloaded"
                            ),
                            Err(e) => tracing::warn!(error = ?e, "SIGHUP: reload failed"),
                        },
                        Err(e) => tracing::warn!(
                            directive = %new_directive,
                            error = ?e,
                            "SIGHUP: bad ARBITRO_LOG directive"
                        ),
                    }
                }
            });
        }
    }

    let config = Config::from_env();
    config.validate();
    let mut server = ArbitroServer::new(config);

    // Wire command log if data_dir is configured
    if let Some(ref dir) = server.config().data_dir {
        let path = std::path::Path::new(dir).join("metadata.log");
        let log = CommandLog::open_with_policy(path, server.config().fsync_policy)?;
        server.set_command_log(SharedCommandLog::new(log));
    }

    server.run().await
}
