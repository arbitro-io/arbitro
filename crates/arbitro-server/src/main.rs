//! arbitro-server — TCP message broker.

use arbitro_server::{ArbitroServer, Config};
use arbitro_server::command_log::{CommandLog, SharedCommandLog};

#[tokio::main]
async fn main() -> std::io::Result<()> {
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
    let mut server = ArbitroServer::new(config);

    // Wire command log if data_dir is configured
    if let Some(ref dir) = server.config().data_dir {
        let path = std::path::Path::new(dir).join("metadata.log");
        let log = CommandLog::open(path)?;
        server.set_command_log(SharedCommandLog::new(log));
    }

    server.run().await
}
