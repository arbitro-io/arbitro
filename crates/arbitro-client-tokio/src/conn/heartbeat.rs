//! Heartbeat watchdog placeholder.
//!
//! Wire-level Ping/Pong is not yet defined in `arbitro-proto::v2`; in
//! the meantime we rely on TCP keepalive plus `read_buf == 0` to detect
//! dead peers. The watchdog task here is a stub kept so `conn::mod`
//! compiles cleanly.

#![allow(dead_code)]

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::config::KeepAlive;

/// Idle ticker: cancels the session when no activity is observed.
///
/// Currently a no-op. Wire it up when the v2 ping/pong frames land.
pub(crate) async fn heartbeat_task(_cfg: KeepAlive, cancel: CancellationToken) {
    // Sleep on cancel forever — placeholder.
    tokio::select! {
        _ = cancel.cancelled() => {},
        _ = tokio::time::sleep(Duration::from_secs(u64::MAX / 2)) => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_heartbeat_task() {
        let cancel = CancellationToken::new();
        heartbeat_task(KeepAlive::default(), cancel).await;
    }
}
