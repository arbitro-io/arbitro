//! Where the ackstore WAL lives.
//!
//! The store directory is **configuration**, not something the library invents
//! per run. Resolution order, highest priority first:
//!
//! 1. An explicit directory on [`WalConfig::dir`](super::wal::WalConfig::dir)
//!    (set via [`WalConfig::new`](super::wal::WalConfig::new), which is what
//!    [`ClientConfig::ack_store`](crate::config::ClientConfig::ack_store)
//!    carries).
//! 2. The `ARBITRO_ACKSTORE_DIR` environment variable — an operator escape
//!    hatch that needs no code change and works for every client language.
//! 3. The platform state directory, `<state>/arbitro/ackstore`:
//!
//!    | platform | `<state>`                            |
//!    |----------|--------------------------------------|
//!    | Linux/BSD| `$XDG_STATE_HOME`, else `$HOME/.local/state` |
//!    | macOS    | `$HOME/Library/Application Support`   |
//!    | Windows  | `%LOCALAPPDATA%`                     |
//!
//! 4. Nothing — [`StoreError::NoDefaultDir`]. There is deliberately **no**
//!    fallback to the current working directory or to a temp dir:
//!
//!    * cwd-relative is hostile to a service: the store silently moves when
//!      systemd, a container entrypoint, or a cron job starts the process from
//!      a different directory, so a restart replays work that was already done
//!      — the exact failure the store exists to prevent.
//!    * a temp dir is wiped on reboot (and by `systemd-tmpfiles` well before
//!      that), which defeats "survives restart" *silently*: everything looks
//!      healthy and the dedup set is simply empty.
//!
//!    A loud error naming the two ways to fix it beats either silent failure.
//!
//! The layout is byte-identical across the Rust, Go and TS clients so a worker
//! rewritten in another language keeps reading the same log.

use std::path::{Path, PathBuf};

use super::store::StoreError;

/// Environment variable that overrides the default store directory.
pub const ENV_DIR: &str = "ARBITRO_ACKSTORE_DIR";

/// Directory the WAL uses when no explicit one is configured.
///
/// See the [module docs](self) for the resolution order. Returns
/// [`StoreError::NoDefaultDir`] when the platform exposes no home/state
/// directory (a container with no `HOME`, a bare systemd unit); the caller must
/// then configure a path explicitly.
pub fn default_dir() -> Result<PathBuf, StoreError> {
    if let Some(d) = env_override() {
        return Ok(d);
    }
    Ok(state_base_dir()?.join("arbitro").join("ackstore"))
}

fn env_override() -> Option<PathBuf> {
    // Only an all-whitespace value counts as "unset" — trailing spaces are
    // legal in a POSIX path, so the value itself is never trimmed.
    match std::env::var(ENV_DIR) {
        Ok(v) if !v.trim().is_empty() => Some(PathBuf::from(v)),
        _ => None,
    }
}

fn non_empty_env(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => None,
    }
}

#[cfg(windows)]
fn state_base_dir() -> Result<PathBuf, StoreError> {
    non_empty_env("LOCALAPPDATA")
        .map(PathBuf::from)
        .ok_or_else(|| StoreError::NoDefaultDir("%LOCALAPPDATA% is not set".into()))
}

#[cfg(target_os = "macos")]
fn state_base_dir() -> Result<PathBuf, StoreError> {
    Ok(home_dir()?.join("Library").join("Application Support"))
}

#[cfg(all(unix, not(target_os = "macos")))]
fn state_base_dir() -> Result<PathBuf, StoreError> {
    // XDG requires relative values to be ignored as if unset.
    if let Some(x) = non_empty_env("XDG_STATE_HOME") {
        let p = PathBuf::from(x);
        if p.is_absolute() {
            return Ok(p);
        }
    }
    Ok(home_dir()?.join(".local").join("state"))
}

#[cfg(not(any(unix, windows)))]
fn state_base_dir() -> Result<PathBuf, StoreError> {
    Err(StoreError::NoDefaultDir(
        "unsupported platform has no known state directory".into(),
    ))
}

#[cfg(unix)]
fn home_dir() -> Result<PathBuf, StoreError> {
    non_empty_env("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| StoreError::NoDefaultDir("$HOME is not set".into()))
}

/// Make `dir` usable as a store directory, or explain precisely why it is not.
///
/// Distinguishes the three failure modes that otherwise arrive as one opaque
/// `io::Error`: the path is a regular file, a parent component is a file, or
/// the process cannot create/traverse it.
pub(crate) fn prepare_dir(dir: &Path) -> Result<(), StoreError> {
    match std::fs::metadata(dir) {
        Ok(m) if m.is_dir() => Ok(()),
        Ok(_) => Err(StoreError::BadDir {
            path: dir.to_path_buf(),
            reason: "path exists but is not a directory".into(),
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(dir).map_err(|e| StoreError::BadDir {
                path: dir.to_path_buf(),
                reason: format!("cannot create directory: {e}"),
            })
        }
        Err(e) => Err(StoreError::BadDir {
            path: dir.to_path_buf(),
            reason: format!("cannot stat: {e}"),
        }),
    }
}
