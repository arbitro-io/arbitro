# Changelog

All notable changes to `arbitro-server` (and the in-tree Rust reference client
`arbitro-client-tokio`) are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/); this project uses SemVer with
the pre-1.0 interpretation (breaking changes may land on a minor bump).

## [Unreleased]

### Added — `arbitro-client-tokio`
- **`ClientConfig::ack_store`** — the redelivery-dedup WAL, and the directory it
  lives in, are now declared on the normal client configuration struct, and
  plain `Client::connect` opens it. `WalConfig::new(dir)` pins an explicit path;
  `WalConfig::default()` selects the platform default: `$ARBITRO_ACKSTORE_DIR`,
  else `$XDG_STATE_HOME/arbitro/ackstore` (Linux/BSD),
  `~/Library/Application Support/arbitro/ackstore` (macOS), or
  `%LOCALAPPDATA%\arbitro\ackstore` (Windows). Never the cwd, never a temp dir
  — both silently defeat restart survival, so an unresolvable default is a hard
  error (`StoreError::NoDefaultDir`) instead.
- **`ackstore::default_dir()` / `WalConfig::resolve_dir()` / `Wal::dir()`** —
  report the resolved path (before or after opening) so it can be logged.
- **Single-writer directory lock** — `Wal::open` takes an OS advisory lock
  (`flock` on unix, exclusive share-mode open on Windows) on
  `<dir>/ackstore.lock`. A second client on the same directory now fails with
  `StoreError::Locked` instead of interleaving frames into one log, which after
  a restart misattributed records between slots and could skip real work. The
  kernel releases the lock on process exit, so a crash never wedges the store.
- **`StoreError::BadDir` / `NoDefaultDir` / `Locked`** — an unusable store
  directory now names the path and the specific problem instead of surfacing an
  opaque `io::Error`.

### Changed — `arbitro-client-tokio`
- `WalConfig::dir` is now `Option<PathBuf>` (`None` = platform default).
- A failed initial connect cancels the background tasks and closes the ack
  store — previously they leaked for the process lifetime, and with the new
  directory lock a retry loop would have hit `StoreError::Locked`.
- New unix-only dependency `libc` (for `flock`); it was already in this crate's
  graph via tokio, so no new crate is pulled in.

### Unchanged
- On-disk WAL format and store semantics. This is configuration only.
- `Client::connect` still opens no store by default; `connect_with_ackstore`
  still accepts a custom `Store` and takes precedence over `cfg.ack_store`.

## [0.6.2] - 2026-07-18

Cluster-hardening release. The Raft control plane gains transport-level mutual
TLS, authenticated peer identity, and a set of liveness/durability fixes that
close the gaps found while soak-testing multi-node restarts under chaos.

### Added
- **Cluster mTLS (D1).** Inter-node Raft traffic can run over mutual TLS behind
  the `cluster-tls` feature. Activated at runtime only when
  `ARBITRO_CLUSTER_TLS_CERT`, `ARBITRO_CLUSTER_TLS_KEY`, and
  `ARBITRO_CLUSTER_TLS_CA` are all set; otherwise the transport keeps the
  original plaintext path unchanged.
- **Peer identity binding (D2).** With `cluster-tls`, each connection's
  authenticated `PeerId` is derived from the peer certificate's SAN/CN, so a
  node can only speak for the identity its certificate authorizes.
- **Ack reliability layer.** Gated pending state, a cold tier for aged entries,
  and the `AckState` / `AckBatch` wire frames (`0x0A01`–`0x0A04`).
- **Rust client:** `pause_consumer` / `resume_consumer` on the reference client.

### Changed
- **Zero-copy proposal handoff.** `propose_command` now moves the payload into
  the Raft mailbox with `write_bytes(payload.into())`, avoiding a copy on the
  cluster metadata path.
- **Raft transport DoS hardening (D3).**
- **Client refactor** (Fable audit): buffer pool/lease, direct producers.

### Fixed
- **L13 — restarted node never rejoins.** The cluster transport now evicts dead
  peer connections on write failure, so a node that restarts is able to
  re-establish its Raft link instead of being pinned to a stale socket.
- **L10 — graceful Raft drain on shutdown.**
- **C8 — ENOSPC** on the Raft log now maps to `RaftError::Io` instead of a
  generic error.
- **SEC-8 — `fsync_policy` is now applied to the message journal** (previously
  configured but never enforced on that path).
- **Store recovery** scans the segment tail past the `.idx` sidecar so the tail
  batch is recovered after an unclean stop.
- **Delivery-loss fixes:** `WriterGone` tracking, rewind wiring, and a
  resubscribe race.
- **Client reconnect** fix.

### Dependencies
- **arbitro-kit** fan-in (`Mpsc`) wake path: fixed a store-buffering lost-wake in
  the caller-side notify gate and added a drop-guard to `Consumer::drain`. The
  production `NotifyRing` hand-off (OS-thread drain → tokio task) is also ~1.8×
  faster as a result of removing the gate's hot-line read.

[0.6.2]: https://github.com/arbitro-io/arbitro/compare/v0.6.1...v0.6.2
