# Changelog

All notable changes to `arbitro-server` (and the in-tree Rust reference client
`arbitro-client-tokio`) are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/); this project uses SemVer with
the pre-1.0 interpretation (breaking changes may land on a minor bump).

## [0.8.0] — 2026-08-17

### Added — `SubscribeBatch`: N subscriptions in one round-trip

An application that opens a hundred filtered subscriptions used to pay a
hundred round-trips. The broker's work per subscription is a filter check and
a binding, so the trip was nearly the whole cost.

- **`SubscribeBatch` (0x0303)** — body `{ entries: [Subscribe] }`. Entries are
  exactly the single-`Subscribe` body and run the identical admission rules;
  `consumer_id` is per entry, so one frame may span several consumers. Capped
  at 1024 entries.
- **`RepSubscribeBatch` (0x0304)** — body `{ ok, errors, fanout_consumers }`.
  Only **rejections** are listed: the client allocates every `subscription_id`
  before sending, so an id absent from `errors` was accepted. MQTT's SUBACK
  has to return a code per filter because there the server owns subscription
  identity; here it does not.
- A rejected entry does not abort its peers. When every entry failed for the
  same reason — an unknown `consumer_id` is the usual case — the reply
  collapses to a single `RepError` rather than repeating one code N times.
- `Subscribe` is unchanged on the wire and in behaviour.

### Breaking — a stream owns its slice, outright and alone

Catalog admission rules are now enforced, in one place (`transport::rules`),
and three of them reject configurations the broker used to accept:

- **A stream may not claim `>`.** It captures every subject, so it overlaps
  every peer by construction and no second stream could ever exist. A stream
  must declare a bounded slice.
- **Two streams may not claim overlapping slices.** `orders.premium.>` is
  refused next to `orders.>`, because `orders.premium.1` would belong to both
  and neither would be authoritative.
- **A consumer's filter must stay inside its stream's slice, and a
  subscription's inside its consumer's.** Reaching outside used to succeed and
  then quietly deliver nothing.

Existing deployments that create a stream with `subject_filter = ">"` stop
working on upgrade. Give each stream the slice it actually publishes under.

### Added

- **Every subscription has an id of its own.** The broker sends one wire copy
  per `(connection, consumer)`, stamped with the id of whichever subscription
  matched, and the client fans it out locally to the siblings whose filter
  accepts the subject. Acks travel under the subscription's id, not the
  consumer's — the broker opens one pending per subscription, so acking under
  the wrong id left the real one outstanding until `ack_wait`.
- **Four error codes** so a rejected creation names the rule that refused it:
  `InvalidStreamFilter` (0x0207), `StreamFilterConflict` (0x0208),
  `InvalidConsumerFilter` (0x0305), `InvalidSubscriptionFilter` (0x0306).
  Clients that predate them will surface the number.

### Fixed

- **`Client::delete_consumer` retires the routes it orphaned.** It sent the
  frame and left the local routing entries in place; they sat there until
  teardown and would catch deliveries if the consumer id were recycled. The
  entries are dropped only after the broker confirms.
- **Every catalog rejection reported the wrong code.** A stream refused for
  claiming `>` came back as `StreamAlreadyExists`, a subscription refused for
  reaching outside its consumer as `InvalidLength`. Each rule now answers for
  itself, and `catalog_rules` pins the codes rather than only the refusal.
- **`chaos` and `max_inflights` benches ran again.** Both created their stream
  with `>` and had died at the first call since the rules landed, so any number
  they last reported predates it.

### Known gaps

- `delete_stream` still strands the routes of the consumers it cascades. It
  takes a name and the client keeps no name → id map, so it cannot say which
  consumers to retire.

## [0.7.1] - 2026-08-08

### Added — connection authentication

A connection authenticates **once**, in the handshake, and never again. The TCP
stream is the security boundary: once the peer is established, every later
frame on that socket is from the same peer, so nothing on the delivery path
re-checks anything. The hot path is untouched.

- **`auth` module** — `Authenticator` is an enum (`Disabled` | `Token`) with a
  single call site. A future layer (mTLS common-name, JWT, an external auth
  service) is a new variant plus a new match arm; nothing else moves. New
  evidence is a field on `Credentials`, new identity a field on `Identity`.
- **`Identity` carries no secret.** The token that proved it stays in the
  authenticator instead of being cloned onto every connection, and a future
  mTLS/JWT identity has no static token to store anyway.
- **Authorization seam, no plumbing.** The resolved identity is stored in the
  connection registry, so a per-action check is `registry.identity(conn_id)`
  inside the existing `match action` — which already holds `conn_id` and the
  registry. No signature anywhere has to change.

### Fixed — connection authentication

- **An `Auth` frame killed a connection when the broker had auth disabled.**
  `Action::Auth` had no dispatch arm, so it fell through to `Unimplemented` +
  `Err`, which the read loop treats as a malformed frame and drops the
  connection. Every token-configured client died on frame one against a broker
  that simply doesn't want a token. Now a no-op: authentication is consumed by
  the handshake, and there is deliberately no re-auth.
- **The per-user token lookup was not constant-time.** The shared-token path
  compared without an early exit; the multi-user path used `==`, which stops at
  the first differing byte and turns response latency into an oracle. Both are
  constant-time now, and the user scan no longer breaks on match — returning
  early would leak how far down the list a token sits.
- **A malformed `ARBITRO_AUTH_USERS` silently disabled authentication.** An
  entry missing its colons was dropped, and a fully unparseable value left the
  broker booting with auth off and no warning. A set-but-unusable value now
  logs an error.
- **`ARBITRO_AUTH_TOKEN=` (set but empty) enabled auth with an empty token.**
  No client sends an `Auth` frame for an empty token, so this locked everyone
  out. Empty now means unset, matching every other credential read.
- **A tokenless client's oversized first frame got `InvalidLength`, not
  `AuthRequired`.** The size gate ran before the action check. Clients treat
  `AuthRequired` as terminal and `InvalidLength` as retryable, so the wrong code
  turned a clean stop into redial noise.

## [0.7.0] - 2026-08-08

### Fixed — `arbitro-server` (delay accuracy)

Three ways a deadline could mature EARLY, all live before this. Early is the
one direction a delay must never go: it hands back a message the consumer was
promised it still owned.

- **`ack_wait` truncated to whole seconds.** `(ack_wait_ms / 1000).max(1)` —
  integer division. An `ack_wait` of 1500ms became one tick, so the broker
  auto-nacked somewhere inside the first second and redelivered a message whose
  `ack_wait` had not run out. Deadlines are absolute now; nothing rounds.
- **`nack_delay` clamped at 119 seconds, silently.** The flat wheel's ring could
  not express anything further out, so a request for ten minutes came back after
  two and the caller was never told. No ceiling now.
- **Delayed publish stamped its deadline from the cached clock.** A task
  refreshes that clock on a timer, so it reads behind, and stamping from it
  anchors the deadline in the past. A 5-second delay matured at 3.967s against a
  container that had been up for hours — the cached clock and the maturity
  check had drifted apart. Stamping now reads the clock directly and rounds up,
  matching the source the maturity check uses.

Measured `nack_delay`, before → after: 300ms 1.001s → 400ms; 1000ms 1.001s →
1.098s; 1200ms 2.002s → 1.299s; 1800ms 2.002s → 1.898s.

### Added — `arbitro-server` (hierarchical timing wheel)

- **`HierarchicalTimingWheel`** in `arbitro-common` — Kafka-shaped, driven by
  absolute time rather than a tick count. Levels stack by a factor of 64, so
  100ms resolution reaches years in roughly 9 KB, where the flat wheel's memory
  was span ÷ resolution. Two deliberate divergences from Kafka: level 0
  releases a bucket at its END, so firing lands in
  `[deadline, deadline + tick_ms]` and is never early (Kafka fires up to a tick
  early, harmless at 1ms but not for an ack timeout); and a deadline past the
  top level parks and is re-placed rather than being clamped.
- **The shard timer sleeps to the wheel's next deadline instead of ticking.**
  A shard with nothing scheduled now arms no timer at all — the old 1-second
  tick woke every shard, every second, forever. Resolution went from 1s to
  100ms while idle cost went to zero.
- **Per-stream dedup windows take elapsed wall time**, not a count of worker
  wakeups. The two wheels no longer share a cadence; dividing the wakeups would
  desynchronise the first time the timer arm lost to command traffic, which is
  routine.

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

[0.7.1]: https://github.com/arbitro-io/arbitro/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/arbitro-io/arbitro/compare/v0.6.2...v0.7.0
[0.6.2]: https://github.com/arbitro-io/arbitro/compare/v0.6.1...v0.6.2
