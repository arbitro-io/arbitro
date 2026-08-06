//! Single-writer lock on a store directory.
//!
//! # Why this is detected rather than documented away
//!
//! `ackstore.log` is one append-only file whose *in-memory* symbol table is
//! authoritative. Two processes sharing it do not merely interleave bytes:
//! each allocates slot ids from its own `next_id` counter, so process B writes
//! `Register(slot 0 = orders/worker)` over a log where process A already means
//! something else by slot 0. After a restart, replay attributes A's `Record`
//! frames to B's slot — and a `seen()` hit is a message the handler is skipped
//! for. That is silent, unrecoverable work loss, not a corrupt-file error the
//! next open would notice.
//!
//! The cost of detecting it is one extra file handle and one syscall at open,
//! so it is detected. Failing loudly at startup ("already open by another
//! process") is strictly better than a dedup set that quietly lies, and it
//! matters more now that an unconfigured store resolves to a *shared* default
//! directory where two services on one host would otherwise collide by
//! accident.
//!
//! # Mechanism
//!
//! An OS advisory lock held on `ackstore.lock` for the lifetime of the [`Wal`]:
//! `flock(LOC_EX|LOCK_NB)` on unix, an exclusive (share-mode 0) open on
//! Windows. Both are released by the kernel when the handle closes — including
//! on `SIGKILL` or a panic — so a crashed process never leaves the store
//! permanently unopenable, which a plain `O_EXCL` pid-file would.
//!
//! The guarantee is per-machine: the lock does not survive a network
//! filesystem, so a WAL on NFS/SMB shared between hosts remains the caller's
//! responsibility (and is not a supported configuration).
//!
//! [`Wal`]: super::wal::Wal

use std::path::Path;

use super::store::StoreError;

/// Name of the lock file inside the store directory.
pub(crate) const LOCK_FILE: &str = "ackstore.lock";

/// Holds the directory lock. Dropping (or [`Wal::close`](super::store::Store::close))
/// releases it.
#[derive(Debug)]
pub(crate) struct DirLock {
    #[allow(dead_code)] // the open handle IS the lock on every platform
    file: std::fs::File,
}

fn open_opts() -> std::fs::OpenOptions {
    let mut o = std::fs::OpenOptions::new();
    o.read(true).write(true).create(true).truncate(false);
    o
}

fn io_err(dir: &Path, e: std::io::Error) -> StoreError {
    StoreError::BadDir {
        path: dir.to_path_buf(),
        reason: format!("cannot open {LOCK_FILE}: {e}"),
    }
}

/// Take the exclusive lock on `dir`, which must already exist.
#[cfg(unix)]
pub(crate) fn acquire(dir: &Path) -> Result<DirLock, StoreError> {
    use std::os::unix::io::AsRawFd;

    let path = dir.join(LOCK_FILE);
    let file = open_opts().open(&path).map_err(|e| io_err(dir, e))?;
    // SAFETY: `file` owns a valid, open fd for the duration of the call.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let e = std::io::Error::last_os_error();
        // EWOULDBLOCK == EAGAIN on Linux; both spellings are checked so the
        // "someone else has it" case is never reported as a generic io error.
        return Err(match e.raw_os_error() {
            Some(c) if c == libc::EWOULDBLOCK || c == libc::EAGAIN => {
                StoreError::Locked(dir.to_path_buf())
            }
            _ => io_err(dir, e),
        });
    }
    Ok(DirLock { file })
}

/// Take the exclusive lock on `dir`, which must already exist.
#[cfg(windows)]
pub(crate) fn acquire(dir: &Path) -> Result<DirLock, StoreError> {
    use std::os::windows::fs::OpenOptionsExt;

    /// `ERROR_SHARING_VIOLATION` — another handle already holds the file with
    /// an incompatible share mode. Inlined rather than pulled from
    /// `windows-sys` to keep the client dependency-free.
    const ERROR_SHARING_VIOLATION: i32 = 32;

    let path = dir.join(LOCK_FILE);
    // share_mode(0) => no other process may open this file at all.
    match open_opts().share_mode(0).open(&path) {
        Ok(file) => Ok(DirLock { file }),
        Err(e) if e.raw_os_error() == Some(ERROR_SHARING_VIOLATION) => {
            Err(StoreError::Locked(dir.to_path_buf()))
        }
        Err(e) => Err(io_err(dir, e)),
    }
}

/// Platforms with neither `flock` nor share modes: the caller owns
/// single-writer discipline. Documented in [`WalConfig`](super::wal::WalConfig).
#[cfg(not(any(unix, windows)))]
pub(crate) fn acquire(dir: &Path) -> Result<DirLock, StoreError> {
    let path = dir.join(LOCK_FILE);
    let file = open_opts().open(&path).map_err(|e| io_err(dir, e))?;
    Ok(DirLock { file })
}

/// The lock file for `dir`.
#[cfg(test)]
pub(crate) fn lock_path(dir: &Path) -> std::path::PathBuf {
    dir.join(LOCK_FILE)
}
