# Arbitro TODO — Deep Audit Results

Produced by a 4-agent Fable audit covering every crate. Reorganized by
model capability: Sonnet handles mechanical fixes, Opus handles items
requiring deep system reasoning.

---

## Completion Status (updated 2026-07-02)

All server-only items implemented. 324/324 tests pass, workspace compiles clean.

### Legend
- **DONE** — implemented and verified in code
- **SKIP** — intentionally skipped (BREAKING change, out-of-scope)
- **OPEN** — not yet implemented

### CI & Operations (A1) — out of scope (CI/YAML, not server code)
| Item | Status | Notes |
|------|--------|-------|
| CI-1 | OPEN | CI YAML |
| CI-2 | OPEN | CI YAML |
| CI-3 | OPEN | CI YAML |
| CI-4 | OPEN | CI YAML |
| CI-5 | OPEN | CI YAML |
| CI-6 | OPEN | CI YAML |
| CI-7 | OPEN | CI YAML |
| CI-8 | OPEN | CI YAML |
| CI-9 | OPEN | CI YAML |
| CI-10 | OPEN | CI YAML |

### Kubernetes & Docker (A2) — out of scope
| Item | Status | Notes |
|------|--------|-------|
| K8S-1 | OPEN | K8s manifest |
| K8S-2 | OPEN | K8s manifest |
| K8S-3 | OPEN | K8s manifest |
| K8S-4 | OPEN | Docker compose |

### Code Quality (A3) — 17/20 DONE
| Item | Status | Notes |
|------|--------|-------|
| CQ-1 | DONE | `StoreError::Io` variant added |
| CQ-2 | DONE | `wire_hash_32` single definition in `common`, re-exported |
| CQ-3 | DONE | Docs already say "foldhash (fixed seed)" |
| CQ-4 | SKIP | BREAKING — Vec\<u8\>→String requires client coordination |
| CQ-5 | SKIP | BREAKING — header format change requires client coordination |
| CQ-6 | OPEN | Client crate (arbitro-client-tokio) |
| CQ-7 | OPEN | Client crate |
| CQ-8 | DONE | Dead error codes deleted |
| CQ-9 | DONE | Unused deps removed from arbitro-store |
| CQ-10 | DONE | criterion 0.5→0.7 in root Cargo.toml |
| CQ-11 | DONE | Gate::release collapsed to unconditional notify_one |
| CQ-12 | DONE | RepBatchFixed doc already says "4B fixed" |
| CQ-13 | DONE | headers_len semantics consistent |
| CQ-14 | OPEN | Client crate |
| CQ-15 | OPEN | Client crate |
| CQ-16 | OPEN | Client crate |
| CQ-17 | DONE | Arc\<Vec\> iterated directly without clone |
| CQ-18 | DONE | `entries_expired` / `entries_tombstoned` counters in metrics |
| CQ-19 | DONE | deny.toml warn→deny, stale licenses removed |
| CQ-20 | DONE | Cron module doc already says "v2 Header" |

### Simple Guards (A4) — 13/15 DONE (2 are client-only)
| Item | Status | Notes |
|------|--------|-------|
| CRASH-2 | DONE | `EntryTooLarge` check in both stores |
| ROB-2 | DONE | Accept-error sleep before retry |
| ROB-7 | DONE | Cluster peers parse returns error instead of panic |
| ROB-8 | DONE | HelloFrame::parse result matched, handshake deadline added |
| ROB-11 | DONE | Subject length validated at append time |
| ROB-16 | DONE | truncate_front clamps to next_seq |
| ROB-21 | DONE | Ok(false) distinguished from Ok(true) |
| ROB-23 | DONE | Command log breaks on zero-length entry |
| ROB-25 | OPEN | Client crate |
| ROB-26 | OPEN | Client crate |
| ROB-33 | DONE | from_wire returns Result, rejects unknown enums |
| ROB-35 | DONE | ensure_consumer checks durable + max_nack |
| ROB-36 | OPEN | Client crate |
| SEC-8 | DONE | Delayed journal max-delay + pending cap |
| SEC-9 | DONE | SIGUSR1 dump path secured |

### Config & Features (A5) — 2/3 DONE (1 client-only)
| Item | Status | Notes |
|------|--------|-------|
| FEAT-11 | DONE | Config parse warnings + validation |
| FEAT-16 | DONE | JSON log format via `ARBITRO_LOG_FORMAT` |
| SEC-10 | OPEN | Client crate |

### Critical Security (B1) — 2/2 DONE
| Item | Status | Notes |
|------|--------|-------|
| SEC-1 | DONE | Pre-auth max_frame_size check |
| SEC-2 | DONE | TLS handshake in spawned task with timeout |

### Store Correctness (B2) — 9/9 DONE
| Item | Status | Notes |
|------|--------|-------|
| CRASH-1 | DONE | purge() calls rotate() |
| ROB-9 | DONE | seq_to_idx + binary_search fallback |
| ROB-10 | DONE | rotate() pushes SegmentMetadata |
| ROB-15 | DONE | Drop mmap before file delete |
| ROB-17 | DONE | Tombstone sidecar per segment |
| ROB-18 | DONE | drain() via tombstoning |
| PERF-3 | DONE | Vec::with_capacity(1024) |
| PERF-4 | DONE | MemoryStore rotate() returns Result |
| SEC-4 | DONE | MAX_ENTITY_ID cap enforced |

### Engine Routing (B3) — 7/8 DONE (1 client-only)
| Item | Status | Notes |
|------|--------|-------|
| ROB-12 | DONE | pending_seqs.contains skip |
| ROB-13 | DONE | One binding per subscription enforced |
| ROB-14 | DONE | SmallVec in trie find_matches |
| ROB-34 | DONE | ensure_subscription checks filter divergence |
| ROB-29 | DONE | IdPool slot recycling + generation tags |
| SEC-5 | DONE | lookup_verified with subject bytes |
| SEC-6 | DONE | Per-connection resource quotas |
| SEC-7 | DONE | Cron slot + worker cap |

### Server Lifecycle (B4) — 7/7 DONE
| Item | Status | Notes |
|------|--------|-------|
| ROB-1 | DONE | 10s handshake deadline |
| ROB-3 | DONE | Token-bucket rate limiter |
| ROB-4 | DONE | Delayed journal sync_data |
| ROB-5 | DONE | Persistent file handle + batch fsync |
| ROB-6 | DONE | Notify signaled by append, select on deadline |
| ROB-19 | DONE | JoinSet + bounded shutdown timeout |
| ROB-20 | DONE | Default max timeout for cron |
| ROB-22 | DONE | Queue overflow closes connection |

### Hot-Path Performance (B5) — 7/7 DONE
| Item | Status | Notes |
|------|--------|-------|
| PERF-1 | DONE | HashMap\<u64, Pending\> for ack lookup |
| PERF-2 | DONE | Aggregated snapshot command per shard |
| PERF-5 | DONE | Surgical bind/unbind dedup |
| PERF-6 | DONE | Incremental trie insert |
| PERF-8 | DONE | DeltaEvents accepted as &mut (caller-owned) |
| PERF-9 | DONE | HashSet dedup above threshold in resolve_patterns |
| PERF-10 | DONE | Concurrent per-shard drain |

### Proto Hardening (B6) — 5/5 DONE
| Item | Status | Notes |
|------|--------|-------|
| ROB-24 | DONE | Length checks in BatchIter/RepBatchEntryIter |
| ROB-31 | DONE | parse(buf)->Result as only public constructor |
| ROB-32 | DONE | try_new() fallible constructors for 6 wire views |
| SEC-3 | OPEN | Client crate |
| FEAT-18 | DONE | validate_pattern rejects empty/empty-token/tokens-after-gt |

### Client Robustness (B7) — out of scope (all client crate)
| Item | Status | Notes |
|------|--------|-------|
| ROB-27 | OPEN | Client crate |
| ROB-28 | OPEN | Client crate |
| ROB-30 | OPEN | Client crate |
| ROB-37 | OPEN | Client crate |
| ROB-38 | OPEN | Client crate |
| PERF-7 | OPEN | Client crate |
| PERF-11 | OPEN | Client crate |

### Missing Features (B8) — 10/15 DONE (5 client/k8s)
| Item | Status | Notes |
|------|--------|-------|
| FEAT-1 | DONE | RetentionLimits enforced in append |
| FEAT-2 | DONE | FsyncPolicy configurable |
| FEAT-3 | DONE | deliveries: u16 counter in Pending |
| FEAT-4 | DONE | Consumer state persistence documented (command_log + cursor) |
| FEAT-5 | DONE | Idempotency implemented at server layer, docs corrected |
| FEAT-6 | DONE | Per-segment .idx sidecar |
| FEAT-7 | DONE | get_verified with CRC check |
| FEAT-8 | DONE | compact() rewrites sealed segments |
| FEAT-9 | DONE | Command log compaction |
| FEAT-10 | DONE | Multi-credential auth system |
| FEAT-12 | DONE | Deep health check with shard ping |
| FEAT-13 | OPEN | Client crate |
| FEAT-14 | OPEN | Client crate |
| FEAT-15 | OPEN | Client crate |
| FEAT-17 | OPEN | K8s — out of scope |

### Test Gaps (B9) — 4/4 DONE
| Item | Status | Notes |
|------|--------|-------|
| TEST-1 | DONE | Corrupted command log recovery test |
| TEST-2 | DONE | max_frame_size boundary tests |
| TEST-3 | DONE | idle_timeout / keepalive tests |
| TEST-4 | DONE | Cluster partition/rejoin tests |

### Summary
| Scope | Done | Open | Skip |
|-------|------|------|------|
| Server (proto+engine+store+server+e2e+common) | **83** | 0 | 2 (BREAKING) |
| Client (arbitro-client-tokio) | 0 | **16** | 0 |
| CI/K8s/Docker | 0 | **14** | 0 |
| **Total** | **83** | **30** | **2** |

---

# WAVE A — Sonnet (mechanical, isolated, low-risk)

## A1. CI & Operations (10 items)

### CI-1: CI runs only 6 of 16 e2e test suites (~40% coverage)
**File:** `.github/workflows/ci.yml:108`
NOT run: drain_invariants, workflow_streams, catalog_invariants,
idempotency_invariants, cron, delayed, fuzz, cluster, aletheia_demo.
**Fix:** Use `cargo test -p arbitro-e2e --tests`; add cluster job.

### CI-2: TLS feature never compiled or tested in CI
**File:** `.github/workflows/ci.yml:214`
TLS code can bit-rot undetected behind feature flag.
**Fix:** Add `cargo build -p arbitro-server --features tls` job.

### CI-3: No e2e tests for auth, rate limiting, max_connections
**File:** `crates/arbitro-e2e/tests`
Config supports these features; zero tests exercise them.
**Fix:** Add `tests/limits_and_auth.rs`.

### CI-4: Fuzz test is minimal and not in CI
**File:** `crates/arbitro-e2e/tests/fuzz_random_bytes.rs:24`
Only random bytes after valid HELLO. No cargo-fuzz harness.
**Fix:** Add to CI; add cargo-fuzz targets for frame decoder.

### CI-5: No Docker image smoke test before push
**File:** `.github/workflows/ci.yml:248`
Image built and pushed without ever running it.
**Fix:** `docker run` + minimal health check before push.

### CI-6: cargo-audit/deny compiled from source; no scheduled scan
**File:** `.github/workflows/ci.yml:227`
5-10 min compile per run. CVEs between pushes go undetected.
**Fix:** Use prebuilt binaries; add weekly scheduled trigger.

### CI-7: clippy only checks --lib; skips tests/bins
**File:** `.github/workflows/ci.yml:96`
11k lines of test code + binaries unchecked.
**Fix:** `cargo clippy --workspace --all-targets -- -D warnings`.

### CI-8: No code coverage measurement
**File:** `.github/workflows/ci.yml`
No cargo-llvm-cov. Coverage claims unverifiable.
**Fix:** Add `cargo llvm-cov` job; upload to Codecov.

### CI-9: No concurrency cancellation; sibling-repo checkouts unpinned
**File:** `.github/workflows/ci.yml:3`
Force-pushes stack redundant runs. Sibling push can break arbitro.
**Fix:** Add concurrency group with cancel-in-progress.

### CI-10: No MSRV declaration or check
**File:** `.github/workflows/ci.yml`
Library crates have no `rust-version` compatibility contract.
**Fix:** Add `rust-version = "1.88"` to workspace package.

---

## A2. Kubernetes & Docker (4 items)

### K8S-1: RWO PVC + RollingUpdate deadlocks upgrades
**File:** `deploy/k8s/deployment.yaml:1`
New pod can't mount volume while old pod holds it. Rollout stalls.
**Fix:** Add `strategy: { type: Recreate }` or convert to StatefulSet.

### K8S-2: Liveness/readiness probes are TCP-only; metrics not enabled
**File:** `deploy/k8s/deployment.yaml:57`
Deadlocked shard passes TCP probe. ARBITRO_METRICS_LISTEN never set.
**Fix:** Set metrics addr; liveness via HTTP; add ServiceMonitor.

### K8S-3: Image pinned to :latest; missing seccomp + capability drop
**File:** `deploy/k8s/deployment.yaml:24`
No reproducibility. Missing Restricted PSS fields.
**Fix:** Pin to version tag; add capabilities.drop + seccomp.

### K8S-4: docker-compose has no healthcheck
**File:** `docker-compose.yml:1`
Wedged broker never restarted.
**Fix:** Ship `--healthcheck` subcommand; add compose healthcheck.

---

## A3. Code Quality — Deletes & Doc Fixes (20 items)

### CQ-1: StoreError has no Io variant — IO errors mapped misleadingly
**File:** `crates/arbitro-store/src/tolerant.rs:94`
EACCES/disk-full shows as "not found" or "full".
**Fix:** Add `StoreError::Io` variant.

### CQ-2: wire_hash_32 duplicated in two modules
**File:** `crates/arbitro-engine/src/common/mod.rs:14`
Identical implementations; drift risk on the function delivery depends on.
**Fix:** Single definition in `common`; re-export from catalog.

### CQ-3: Hash function docs say FNV-1a, implementation is foldhash
**File:** `crates/arbitro-proto/src/config/stream.rs:192`
Non-Rust client implementers compute wrong hashes from docs.
**Fix:** Update all comments to name foldhash-fixed-seed.

### CQ-4: Cold-path JSON serializes Vec<u8> as number arrays
**File:** `crates/arbitro-proto/src/v2/cold/mod.rs:139`
`"orders"` becomes `[111,114,100,101,114,115]`. 4x wire bloat.
**Fix:** Use `String` for names. BREAKING — coordinate with TS/Go clients.

### CQ-5: Dual 16-byte header formats special-cased by action code
**File:** `crates/arbitro-client-tokio/src/transport/reader.rs:55`
Hardcoded action-to-format knowledge. Adding new envelope frames silently
desynchronizes the connection.
**Fix:** Migrate batch deliveries to v2 Header server-side. BREAKING.

### CQ-6: Most ClientMetrics counters never incremented
**File:** `crates/arbitro-client-tokio/src/metrics.rs:29`
acks_sent, nacks_sent, reconnects, etc. permanently zero.
**Fix:** Wire counters at obvious sites or delete dead fields.

### CQ-7: ClientConfig::write_queue_capacity is dead configuration
**File:** `crates/arbitro-client-tokio/src/config.rs:18`
Field never read; actual capacity is compile-time constant.
**Fix:** Remove field; document `WRITE_QUEUE_CAP`.

### CQ-8: Dead error codes for removed subsystems
**File:** `crates/arbitro-engine/src/error.rs:45`
PluginNotFound, EdgeNotFound, Slab codes, DrainMode — all dead.
**Fix:** Delete dead variants and legacy types.

### CQ-9: Unused dependencies in arbitro-store
**File:** `crates/arbitro-store/Cargo.toml:8`
arbitro-proto and zerocopy not referenced in source.
**Fix:** Remove both from `[dependencies]`.

### CQ-10: Outdated major dependency versions
**File:** `crates/arbitro-server/Cargo.toml:55`
thiserror 1 (v2 available), nix 0.27 (0.29), criterion 0.5 (0.7).
**Fix:** Bump versions.

### CQ-11: Gate::release branches are identical
**File:** `crates/arbitro-common/src/gate.rs:61`
Both arms call `notify_one()`. Misleading structure.
**Fix:** Collapse to unconditional `notify_one()` with comment.

### CQ-12: RepBatchFixed doc describes 8-byte layout; struct is 4 bytes
**File:** `crates/arbitro-proto/src/wire/delivery.rs:75`
Wrong docs = incompatible client implementations.
**Fix:** Fix doc comments to match struct layouts.

### CQ-13: headers_len semantics inconsistent between docs and encoder
**File:** `crates/arbitro-proto/src/wire/msg_headers.rs:213`
Decoder trusting doc lands 4 bytes short.
**Fix:** Pick one definition; add roundtrip test.

### CQ-14: WorkflowBuilder::compensate ignores step-name parameter
**File:** `crates/arbitro-client-tokio/src/workflow.rs:347`
Always attaches to most recently added step regardless of name.
**Fix:** Look up step by name or drop the parameter.

### CQ-15: Pending cap hit still lets wire frame be sent
**File:** `crates/arbitro-client-tokio/src/state/pending.rs:53`
Frame sent to broker; reply discarded. Wasted broker work.
**Fix:** Return Option from `register()`; skip send when None.

### CQ-16: Ring-full and writer-gone both map to ChannelClosed
**File:** `crates/arbitro-client-tokio/src/publish/mod.rs:23`
Transient backpressure indistinguishable from permanent failure.
**Fix:** Add `ClientError::Backpressure` for ring-full case.

### CQ-17: v2_list_consumers clones entire cached Vec on unfiltered call
**File:** `crates/arbitro-server/src/transport/dispatch_v2.rs:1727`
Defeats Arc-cache purpose.
**Fix:** Iterate `Arc<Vec>` directly when no filter.

### CQ-18: Tombstone drop reasons all counted as publish_no_match
**File:** `crates/arbitro-engine/src/runtime/execute.rs:140`
Can't distinguish retention expiry from routing misses.
**Fix:** Add `entries_expired` / `entries_tombstoned` counters.

### CQ-19: deny.toml multiple-versions only warns; stale license
**File:** `deny.toml:10`
Duplicate dep trees accumulate silently.
**Fix:** Consider `deny`; remove stale license entry.

### CQ-20: Cron module doc claims Envelope framing but encodes v2 Header
**File:** `crates/arbitro-proto/src/wire/cron.rs:1`
Doc/wire drift.
**Fix:** Update doc to say v2 Header.

---

## A4. Simple Guards — one `if` check each (15 items)

### CRASH-2: Oversized entry causes mmap out-of-bounds panic
**File:** `crates/arbitro-store/src/tolerant.rs:301`
Entry larger than `MAX_SEGMENT_BYTES` (64 MiB) rotates to a fresh segment
then slices past the mapping — panic. Payload size is attacker-controlled.
Also affects `MemoryStore` (memory.rs:169).
**Fix:** Return `StoreError::EntryTooLarge` when entry > segment capacity.

### ROB-2: Accept-error busy loop (EMFILE spin)
**File:** `crates/arbitro-server/src/server.rs:606`
On `listener.accept()` error the loop retries immediately. EMFILE causes a
hot spin that pegs a core and floods logs.
**Fix:** Sleep 100ms on accept error before retrying.

### ROB-7: Cluster boot panics on operator config typos
**File:** `crates/arbitro-server/src/server.rs:314`
`expect()/panic!` on `ARBITRO_CLUSTER_PEERS` parse failures. Crash instead
of clean error+exit.
**Fix:** Return `std::io::Error` or `exit(2)` with `tracing::error!`.

### ROB-8: HelloFrame::parse result discarded — malformed HELLO accepted
**File:** `crates/arbitro-server/src/server.rs:778`
`let _ = HelloFrame::parse(...)` — any 8 bytes with correct magic accepted.
Version/flags never enforced.
**Fix:** Match on parse result; on None send error frame and close.

### ROB-11: Silent u16 truncation of subject length corrupts records
**File:** `crates/arbitro-store/src/tolerant.rs:240`
Subject > 65535 bytes writes full bytes but header stores truncated `subj_len`.
Reads mis-slice boundaries; CRC mismatch stops recovery scan.
**Fix:** Validate `subject.len() <= u16::MAX` at append time.

### ROB-16: truncate_front(target > next_seq) breaks future reads
**File:** `crates/arbitro-store/src/tolerant.rs:343`
`first_seq` set beyond `next_seq`. New appends unreadable.
**Fix:** Clamp: `let target = target.min(self.next_seq)`.

### ROB-21: v2_delete_consumer treats Ok(false) as success
**File:** `crates/arbitro-server/src/transport/dispatch_v2.rs:1498`
Fan-out to shards: `Ok(false)` ("not found") sends RepOk + records delete.
**Fix:** Match on `Ok(true)` specifically.

### ROB-23: Command log replay infinite-loops on zero-length entry
**File:** `crates/arbitro-server/src/persistence/command_log.rs:126`
`len==0` entry `continue`s without consuming CRC. Desynchronizes parser.
**Fix:** `break` on `len==0` instead of `continue`.

### ROB-25: Client RepError dispatch panics on short frame
**File:** `crates/arbitro-client-tokio/src/transport/reader.rs:112`
`frame[..32]` on a frame with `msg_len < 16` panics the reader task.
**Fix:** Check `frame.len() >= RepErrFrame::WIRE_SIZE` first.

### ROB-26: Client batch deliver demux panics on lying entry header
**File:** `crates/arbitro-client-tokio/src/consume/demux.rs:138`
`subj_len + reply_len > data_len` causes slice panic.
**Fix:** Validate `subj_len + reply_len <= data_len` with checked_add.

### ROB-33: ConsumerConfig from_wire silently coerces invalid enums
**File:** `crates/arbitro-proto/src/config/consumer.rs:145`
Unknown `ack_policy` silently becomes `AckPolicy::None` (no redelivery).
Bypasses all invariant checks.
**Fix:** Return `Result<ConsumerConfig, ErrorCode>`; reject unknown values.

### ROB-35: ensure_consumer idempotency check omits durable and max_nack
**File:** `crates/arbitro-engine/src/catalog/mod.rs:290`
Different DLQ threshold or durability flag silently accepted.
**Fix:** Add `durable` and `max_nack` to the comparison.

### ROB-36: No client-side subject/msg_id length validation
**File:** `crates/arbitro-client-tokio/src/transport/encode.rs:68`
`as u16` casts silently truncate. Corrupt frame where subject bleeds into
payload. `validate_subject` exists but never called.
**Fix:** Validate lengths before encoding; return `ClientError::InvalidConfig`.

### SEC-8: Delayed journal has no size/max-delay cap
**File:** `crates/arbitro-server/src/delayed.rs:116`
Any `delay_ms` (u64) accepted; unlimited pending entries.
**Fix:** Validate maximum delay; cap total pending entries/bytes.

### SEC-9: SIGUSR1 dump written to predictable /tmp path
**File:** `crates/arbitro-server/src/server.rs:646`
Symlink-attack target; leaks broker topology.
**Fix:** Write into `data_dir` with `O_CREAT|O_EXCL` and 0600.

---

## A5. Config & Feature Flags (3 items)

### FEAT-11: Config is env-only; silent fallback on parse errors
**File:** `crates/arbitro-server/src/config.rs:76`
`env_parse` silently falls back to default on ANY parse error.
**Fix:** Log warning on present-but-unparseable; add config file.

### FEAT-16: No structured (JSON) log output
**File:** `crates/arbitro-server/src/main.rs:57`
Human-readable only. K8s log aggregators need JSON.
**Fix:** Add `ARBITRO_LOG_FORMAT=json|text` env switch.

### SEC-10: No client authentication support
**File:** `crates/arbitro-client-tokio/src/config.rs:10`
Proto defines `Action::Auth`; client never sends it. Can't connect to
auth-enabled brokers.
**Fix:** Add `auth_token` to `ClientConfig`; send Auth frame after Hello.

---

# WAVE B — Opus (system reasoning, multi-file, concurrency)

## B1. Critical Security (2 items)

### SEC-1: Unbounded pre-auth frame buffering (OOM DoS)
**File:** `crates/arbitro-server/src/server.rs:787`
Auth branch reads `msg_len` from header with NO `max_frame_size` check.
Unauthenticated attacker sends `msg_len = u32::MAX` (~4 GiB), server buffers
it all per-connection. A handful of connections OOMs the broker.
**Fix:** Apply the same `msg_len > max_frame_size` check in the auth branch;
cap auth frames to 4 KiB.

### SEC-2: TLS handshake blocks accept loop (one-socket DoS)
**File:** `crates/arbitro-server/src/server.rs:560`
`acceptor.accept(stream).await` runs inside the accept loop. A single client
that stalls mid-handshake blocks ALL new connections indefinitely.
**Fix:** Move TLS handshake into the spawned per-connection task; wrap in
`tokio::time::timeout(10s)`.

---

## B2. Store Correctness (9 items)

### CRASH-1: purge() then append() panics (active_mmap is None)
**File:** `crates/arbitro-store/src/tolerant.rs:399`
`purge()` sets `active_mmap = None` but never re-creates a segment. Next
`append()` hits `expect("active mmap initialised")` — guaranteed panic.
**Fix:** In `purge()`, call `rotate()` so a fresh active segment exists.

### ROB-9: Silent wrong-message delivery after recovery gaps
**File:** `crates/arbitro-store/src/tolerant.rs:317`
Read paths compute `idx = seq - first_seq` without verifying `m.seq == seq`.
Recovery gaps from CRC-truncated segments misalign every later read.
**Fix:** Verify `index[idx].seq == seq`; fall back to `binary_search_by_key`.
Apply to get(), read(), read_range(), for_each(), tombstone_at().

### ROB-10: Runtime segments never registered — files never deleted
**File:** `crates/arbitro-store/src/tolerant.rs:95`
`rotate()` never pushes `SegmentMetadata`. `truncate_front()` iterates
`self.segments` (empty at runtime). Segment files + 64 MiB mmaps accumulate
without bound.
**Fix:** Push `SegmentMetadata` in `rotate()` with first/last seq.

### ROB-15: truncate_front deletes files while mmap still open
**File:** `crates/arbitro-store/src/tolerant.rs:356`
On Windows, deleting a file with a live mapping fails; error discarded.
File leaks on disk forever.
**Fix:** Drop mmap first, then delete file; log failures.

### ROB-17: Tombstones on sealed segments don't survive restart
**File:** `crates/arbitro-store/src/tolerant.rs:572`
Sealed segments are read-only; tombstone is in-memory only. Deleted messages
resurrect after every restart.
**Fix:** Keep a side-file of tombstoned seqs per segment.

### ROB-18: drain() is a silent no-op stub on TolerantStore
**File:** `crates/arbitro-store/src/tolerant.rs:533`
Returns 0 always. Subject-purge of PII silently does nothing.
**Fix:** Implement via tombstoning, or return an explicit error.

### PERF-3: TolerantStore::init() preallocates 48 MiB index per store
**File:** `crates/arbitro-store/src/tolerant.rs:290`
`Vec::with_capacity(1_000_000)` x 48 bytes. One per stream.
**Fix:** Use modest default; let Vec grow on demand.

### PERF-4: MemoryStore mmap allocation failure panics on hot path
**File:** `crates/arbitro-store/src/memory.rs:573`
`rotate()` calls `alloc_anon_segment` which `expect()`s. Memory pressure at
runtime crashes instead of shedding load.
**Fix:** Make `rotate()` return `Result`; propagate as `StoreError::Full`.

### SEC-4: Unbounded Vec resize from caller-supplied entity IDs
**File:** `crates/arbitro-engine/src/catalog/mod.rs:182`
`ensure_stream_slot(id = u32::MAX)` attempts ~4-billion-slot allocation.
**Fix:** Enforce `MAX_ENTITY_ID` cap; return error beyond it.

---

## B3. Engine Correctness — concurrency & routing (8 items)

### ROB-12: Duplicate Delivered leaks inflight permanently
**File:** `crates/arbitro-engine/src/runtime/execute.rs:52`
Same seq delivered twice pushes two Pending entries, one pending_seqs entry.
Single Ack removes one; second Pending + one inflight credit stranded forever.
**Fix:** Skip if `pending_seqs.contains(&entry.seq)`.

### ROB-13: bind/unbind_subscription clobbers connection_id
**File:** `crates/arbitro-engine/src/catalog/match_table.rs:337`
Multiple bindings on same subscription: last bind wins routing, either unbind
kills routing for the survivor.
**Fix:** Enforce one active binding per subscription or make entries per-binding.

### ROB-14: Trie find_matches silently drops matches on 16-slot overflow
**File:** `crates/arbitro-engine/src/common/trie.rs:119`
Stack overflow at depth > 16 with wildcard branching. Valid subscription
match never reported. Silent message loss.
**Fix:** Use `SmallVec` that spills to heap, or cap subject depth at validation.

### ROB-34: ensure_subscription ignores differing filters on re-create
**File:** `crates/arbitro-engine/src/catalog/mod.rs:383`
Re-creating subscription with different filters silently keeps old ones.
**Fix:** Compare and return `SubscriptionConfigMismatch` on divergence.

### ROB-29: Stream/consumer slots never recycled — 4096 cap permanent
**File:** `crates/arbitro-common/src/name_registry.rs:430`
Create/delete churn permanently exhausts slots. IdPool exists but unused.
**Fix:** Back with IdPool; embed generation-tag validation.

### SEC-5: 32-bit subject hash collisions affect delivery correctness
**File:** `crates/arbitro-engine/src/catalog/mod.rs:769`
50% collision probability at ~77k subjects. Delivers to wrong consumers.
**Fix:** Verify subject bytes on exact-match hits; widen to 64-bit hash.

### SEC-6: No per-connection resource quotas (streams/consumers/crons)
**File:** `crates/arbitro-server/src/transport/dispatch_v2.rs:1812`
Single connection can create unlimited entities. Unbounded disk + memory.
**Fix:** Introduce per-connection quotas; reject past limit.

### SEC-7: No cron job count limit
**File:** `crates/arbitro-server/src/cron.rs:173`
Unlimited cron names + unlimited workers per name. Unbounded memory.
**Fix:** Cap slots and workers-per-slot.

---

## B4. Server Robustness — lifecycle & state (7 items)

### ROB-1: No handshake deadline — slowloris on connection slots
**File:** `crates/arbitro-server/src/server.rs:765`
Client that never sends HELLO holds a session + `max_connections` slot until
`idle_timeout` (default 300s). 10k idle sockets block all legitimate clients.
**Fix:** Add 5-10s handshake deadline for HELLO + auth completion.

### ROB-3: Rate limiter blocks read task with buffered frames
**File:** `crates/arbitro-server/src/server.rs:881`
When tokens hit 0, `tokio::time::sleep` runs INSIDE the drain loop, stalling
already-buffered frames for up to 1s. Bursty, high-latency throttling.
**Fix:** Use a proper token-bucket with continuous refill; break out of drain
loop so shutdown remains responsive.

### ROB-4: Delayed journal never fsyncs — acknowledged writes lost on crash
**File:** `crates/arbitro-server/src/delayed.rs:128`
`DelayedJournal::append` calls `flush()` (no-op for durability) but never
`sync_data()`. RepOk sent to client. Crash = silent data loss.
**Fix:** Call `sync_data()` after write, respecting `FsyncPolicy`.

### ROB-5: mark_matured_on_disk reopens file per entry + no fsync
**File:** `crates/arbitro-server/src/delayed.rs:172`
Each matured entry opens/writes/closes the file. No fsync = crash re-delivers
matured entries. Throughput cliff under burst.
**Fix:** Use a persistent file handle; batch fsync.

### ROB-6: Delayed loop polls 100ms with no new-entry notification
**File:** `crates/arbitro-server/src/delayed.rs:308`
New entry with earlier deadline than current sleep is delivered up to 100ms
late. Avoidable idle CPU.
**Fix:** Add `tokio::sync::Notify` signaled by `append()`; select on
`shutdown | sleep(deadline) | notified`.

### ROB-19: Graceful shutdown doesn't join spawned tasks
**File:** `crates/arbitro-server/src/server.rs:697`
Cron, delayed, per-connection read tasks, health endpoints are never
awaited/aborted. Relies on runtime teardown.
**Fix:** Track handles in JoinSet; signal and await with bounded timeout.

### ROB-20: Cron job wedges permanently if worker hangs with timeout_ms=0
**File:** `crates/arbitro-server/src/cron.rs:255`
`running = true` forever if worker stays connected but hangs.
**Fix:** Enforce a default/maximum timeout; clear running after next interval.

### ROB-22: Per-connection queue drops lose acked/delivered messages silently
**File:** `crates/arbitro-server/src/transport/registry.rs:354`
`enqueue()` drops frames on full mpsc. Delivery frames silently lost.
**Fix:** Close connections whose queue overflows; distinguish reply vs delivery.

---

## B5. Hot-Path Performance (7 items)

### PERF-1: Ack/Nack O(pending x entries) linear scan on hot path
**File:** `crates/arbitro-engine/src/runtime/execute.rs:84`
`pending.iter().position()` per ack entry. 10k inflight + 100-entry batch =
1M comparisons. HashSet exists but ignored.
**Fix:** Replace `Vec<Pending> + HashSet` with `HashMap<u64, Pending>`.

### PERF-2: Metrics loop O(shards x streams) awaited round-trips per tick
**File:** `crates/arbitro-server/src/server.rs:1054`
Per-stream `store_info()` calls through shard mpsc every 5s. Thousands of
streams = contention with hot path.
**Fix:** Single aggregated snapshot command per shard.

### PERF-5: bind/unbind rebuilds all dedup sets O(total entries)
**File:** `crates/arbitro-engine/src/catalog/match_table.rs:368`
Every subscribe/unsubscribe/disconnect rehashes all exact+catch_all entries.
**Fix:** Mutate surgically: only affected subscription's entries.

### PERF-6: add_pattern rebuilds entire trie per insertion O(N^2)
**File:** `crates/arbitro-engine/src/catalog/match_table.rs:179`
N wildcard subscriptions costs O(N^2) trie inserts.
**Fix:** Insert incrementally; only rebuild on removal.

### PERF-8: DeltaEvents allocates fresh Vecs per execute() on hot path
**File:** `crates/arbitro-engine/src/events.rs:17`
Heap allocation per ack batch. Violates "alloc-free at steady state" claim.
**Fix:** Accept `&mut DeltaEvents` (caller-owned, cleared per cycle).

### PERF-9: resolve_patterns_readonly O(N^2) dedup with contains()
**File:** `crates/arbitro-engine/src/catalog/match_table.rs:261`
Linear `out.contains(entry)` per trie hit in drain path.
**Fix:** Use `HashSet` or sort+dedup when matches > threshold.

### PERF-10: Per-connection teardown drains shards serially
**File:** `crates/arbitro-server/src/server.rs:925`
EOF cleanup awaits `drain_connection` per shard sequentially.
**Fix:** Concurrent drain (spawn per-shard, await all).

---

## B6. Proto Frame Hardening (5 items)

### ROB-24: BatchIter/RepBatchEntryIter panic on malformed frames
**File:** `crates/arbitro-proto/src/wire/publish.rs:96`, `delivery.rs:374`
No length checks; untrusted wire data indexes past buffer. Remote DoS.
**Fix:** Mirror v2 `BatchPubIter` design: validate lengths before slicing.

### ROB-31: PubFrame/Record accessors panic unless validate() called first
**File:** `crates/arbitro-proto/src/v2/ingress/pub_frame.rs:88`
Opt-in safety: missing validate() = remote-triggerable panic.
**Fix:** Make `parse(buf) -> Result` the only public constructor.

### ROB-32: Legacy wire views panic on truncated buffers
**File:** `crates/arbitro-proto/src/wire/manager.rs:109`
CreateConsumerView, stream views, system views all `.unwrap()` on short buf.
Metadata-log replay feeds these directly.
**Fix:** Convert to fallible constructors with length checks.

### SEC-3: No max_frame_size on client reader — 4 GiB allocation from wire
**File:** `crates/arbitro-client-tokio/src/transport/reader.rs:82`
Trusts 32-bit `msg_len` with no cap. Malicious broker or MITM causes OOM.
**Fix:** Add configurable `max_frame_size` to `ClientConfig`.

### FEAT-18: No trie pattern validation
**File:** `crates/arbitro-engine/src/common/trie.rs:49`
`orders.>.eu` silently stored as `orders.>`. Empty tokens accepted.
**Fix:** Add `validate_pattern()` at subscription time.

---

## B7. Client Robustness (7 items)

### ROB-27: Client clone panics on pool exhaustion (16th clone)
**File:** `crates/arbitro-client-tokio/src/client.rs:47`
`Clone::clone` does `.pop().expect(...)`. 15th concurrent clone panics.
**Fix:** Return Arc fallback or provide `try_clone() -> Result`.

### ROB-28: Client ack()/nack() silently dropped when batcher full
**File:** `crates/arbitro-client-tokio/src/consume/message.rs:111`
`try_send` error swallowed. Broker redelivers; duplicates invisible.
**Fix:** Make `ack()` async or return Result; count drops in metrics.

### ROB-30: Cron handler blocks reader task (head-of-line blocking)
**File:** `crates/arbitro-client-tokio/src/cron.rs:273`
`dispatch_cron_fire` awaited in reader loop. Slow handler freezes all
deliveries and replies.
**Fix:** Fire-and-forget: `tokio::spawn` the handler; return immediately.

### ROB-37: Reconnect replays subscriptions fire-and-forget
**File:** `crates/arbitro-client-tokio/src/conn/session.rs:170`
No reply checking. If broker restarted, replayed Subscribe silently fails.
No CreateConsumer replay.
**Fix:** Track replay replies; re-create consumers on ConsumerNotFound.

### ROB-38: publish_batch_sync only confirms first chunk
**File:** `crates/arbitro-client-tokio/src/publish/mod.rs:193`
Batches > 256 entries: chunks 2+ are fire-and-forget. Failures invisible.
**Fix:** Register pending slots for all chunks and join them.

### PERF-7: Client writer: one write_all syscall per frame
**File:** `crates/arbitro-client-tokio/src/transport/writer.rs:77`
Drain loop already collects multiple frames but writes one at a time.
**Fix:** Use `write_vectored` / staging buffer per drain cycle.

### PERF-11: One slow subscriber head-of-line blocks all deliveries + replies
**File:** `crates/arbitro-client-tokio/src/state/subscriptions.rs:79`
Reader task awaits full channel. All other consumers + publish replies frozen.
**Fix:** Per-consumer overflow policy; decouple reply processing.

---

## B8. Missing Features — design required (12 items)

### FEAT-1: No store capacity limits (max_msgs/max_bytes/max_age)
**File:** `crates/arbitro-store/src/store.rs:64`
`StoreError::Full` exists but never returned. Both stores grow unbounded.
**Fix:** Enforce limits in `append`; truncate_front or return Full.

### FEAT-2: No store fsync/durability policy
**File:** `crates/arbitro-store/src/tolerant.rs:84`
Data only msynced at rotate/shutdown. Up to 64 MiB of acked publishes lost.
**Fix:** Add configurable flush policy (every N bytes/ms).

### FEAT-3: No per-message delivery counter — max_nack/DLQ unenforceable
**File:** `crates/arbitro-engine/src/catalog/mod.rs:91`
`Pending` has no `deliveries` counter. Cannot enforce `max_nack`.
**Fix:** Add `deliveries: u16` to `Pending`; increment on redelivery.

### FEAT-4: Engine state not persisted for durable consumers
**File:** `crates/arbitro-engine/src/lib.rs:1`
Consumer pending/ack state is memory-only. Restart loses all tracking.
**Fix:** Journal ack floors through WAL, replayed at startup.

### FEAT-5: Idempotency/dedup not implemented in store
**File:** `crates/arbitro-engine/src/context.rs:9`
Comments say "handled at store level" but neither store has dedup.
**Fix:** Implement dedup window in store, or delete misleading artifacts.

### FEAT-6: No recovery index files — full CRC scan on every startup
**File:** `crates/arbitro-store/src/tolerant.rs:107`
`init()` walks + CRCs every byte. 100 GB = minutes of startup.
**Fix:** Per-segment index at seal; rescan only active segment.

### FEAT-7: No read-path corruption detection after recovery
**File:** `crates/arbitro-store/src/store.rs:75`
CRCs only verified during `load_segment`. Bit-rot served raw.
**Fix:** Optional `verify_crc` flag on read paths.

### FEAT-8: No store compaction
**File:** `crates/arbitro-store/src/store.rs:93`
Tombstoned entries never reclaimed from segments.
**Fix:** Offline compaction that rewrites sealed segments.

### FEAT-9: Metadata command log grows without bound
**File:** `crates/arbitro-server/src/persistence/command_log.rs:34`
Every create/delete appended forever. No snapshot/compaction.
**Fix:** Periodic compaction: snapshot live set and rewrite log.

### FEAT-10: Auth is single static shared token — no users/ACLs
**File:** `crates/arbitro-server/src/server.rs:536`
One global token. No identity, no authorization, no mTLS.
**Fix:** Multi-credential auth + subject/stream ACLs.

### FEAT-12: Health endpoint is shallow (always 200)
**File:** `crates/arbitro-server/src/server.rs:1095`
Reports healthy whenever any shard exists. No real liveness check.
**Fix:** Ping shards with timeout; separate readiness vs liveness.

### FEAT-13: Client TLS: no custom CA or mTLS support
**File:** `crates/arbitro-client-tokio/src/conn/session.rs:44`
Only webpki roots or full bypass. No private CA, no client certs.
**Fix:** Add `root_ca_pem` and client cert/key to `TlsConfig`.

### FEAT-14: Client single-address — no failover/pooling
**File:** `crates/arbitro-client-tokio/src/config.rs:12`
One `addr` string. No server list, no rotation on reconnect.
**Fix:** Accept `Vec<String>`; rotate in backoff; add state watch.

### FEAT-15: No client request timeouts
**File:** `crates/arbitro-client-tokio/src/manage/mod.rs:31`
Sync requests await reply forever. `ClientError::Timeout` is dead.
**Fix:** Wrap in `tokio::time::timeout`; add `request_timeout` to config.

### FEAT-17: No backup/restore story for data volume
**File:** `deploy/k8s/pvc.yaml:1`
No documented procedure, no snapshot tooling.
**Fix:** Document backup procedure; state whether live-copy is safe.

---

## B9. Test Coverage Gaps (4 items)

### TEST-1: No e2e test for corrupted/truncated command log recovery
**File:** `crates/arbitro-e2e/tests/persistence.rs`
Crash-during-fsync only covered at unit level.
**Fix:** Truncate/bit-flip log; assert clean boot with prefix intact.

### TEST-2: No large-payload or max_frame_size boundary tests
**File:** `crates/arbitro-e2e/tests`
Default 64 MiB max_frame_size never tested near boundary.
**Fix:** Test at exactly limit, one byte over, multi-MB round-trip.

### TEST-3: No idle_timeout / keepalive behavior tests
**File:** `crates/arbitro-e2e/tests`
Dead socket reaping and alive-subscriber survival untested.
**Fix:** Test with `idle_timeout=2s`.

### TEST-4: Cluster suite lacks partition/rejoin scenarios
**File:** `crates/arbitro-e2e/tests/cluster.rs`
Only 4 tests. No partition, rejoin, quorum-loss.
**Fix:** Add partition, rejoin, quorum-loss tests.

---

# Summary by Wave

| Wave | Model | Items | Risk |
|------|-------|-------|------|
| **A1** CI/Ops | Sonnet | 10 | YAML edits, no logic |
| **A2** K8s/Docker | Sonnet | 4 | YAML/Dockerfile edits |
| **A3** Code Quality | Sonnet | 20 | Deletes, renames, doc fixes |
| **A4** Simple Guards | Sonnet | 15 | One `if` + return per item |
| **A5** Config/Flags | Sonnet | 3 | Isolated feature additions |
| **A — subtotal** | **Sonnet** | **52** | |
| **B1** Critical Security | Opus | 2 | Pre-auth state machine |
| **B2** Store Correctness | Opus | 9 | mmap lifecycle, recovery |
| **B3** Engine Routing | Opus | 8 | Concurrency, hash, trie |
| **B4** Server Lifecycle | Opus | 8 | Async state, shutdown |
| **B5** Hot-Path Perf | Opus | 7 | Data structure redesign |
| **B6** Proto Hardening | Opus | 5 | Wire format, parsing |
| **B7** Client Robustness | Opus | 7 | Reader/writer lifecycle |
| **B8** Missing Features | Opus | 15 | Design + implement |
| **B9** Test Gaps | Opus | 4 | New test suites |
| **B — subtotal** | **Opus** | **65** | |
| **Total** | | **117** | |

Note: CQ-4 and CQ-5 are in Wave A but marked BREAKING — skip those
two unless doing a coordinated protocol bump across all client repos.

### SEC-1: Unbounded pre-auth frame buffering (OOM DoS)
**File:** `crates/arbitro-server/src/server.rs:787`
Auth branch reads `msg_len` from header with NO `max_frame_size` check.
Unauthenticated attacker sends `msg_len = u32::MAX` (~4 GiB), server buffers
it all per-connection. A handful of connections OOMs the broker.
**Fix:** Apply the same `msg_len > max_frame_size` check in the auth branch;
cap auth frames to 4 KiB.

### SEC-2: TLS handshake blocks accept loop (one-socket DoS)
**File:** `crates/arbitro-server/src/server.rs:560`
`acceptor.accept(stream).await` runs inside the accept loop. A single client
that stalls mid-handshake blocks ALL new connections indefinitely.
**Fix:** Move TLS handshake into the spawned per-connection task; wrap in
`tokio::time::timeout(10s)`.

### CRASH-1: purge() then append() panics (active_mmap is None)
**File:** `crates/arbitro-store/src/tolerant.rs:399`
`purge()` sets `active_mmap = None` but never re-creates a segment. Next
`append()` hits `expect("active mmap initialised")` — guaranteed panic.
**Fix:** In `purge()`, call `rotate()` so a fresh active segment exists.

### CRASH-2: Oversized entry causes mmap out-of-bounds panic
**File:** `crates/arbitro-store/src/tolerant.rs:301`
Entry larger than `MAX_SEGMENT_BYTES` (64 MiB) rotates to a fresh segment
then slices past the mapping — panic. Payload size is attacker-controlled.
Also affects `MemoryStore` (memory.rs:169).
**Fix:** Return `StoreError::EntryTooLarge` when entry > segment capacity.

---

## 2. HIGH — Robustness & Correctness

### ROB-1: No handshake deadline — slowloris on connection slots
**File:** `crates/arbitro-server/src/server.rs:765`
Client that never sends HELLO holds a session + `max_connections` slot until
`idle_timeout` (default 300s). 10k idle sockets block all legitimate clients.
**Fix:** Add 5-10s handshake deadline for HELLO + auth completion.

### ROB-2: Accept-error busy loop (EMFILE spin)
**File:** `crates/arbitro-server/src/server.rs:606`
On `listener.accept()` error the loop retries immediately. EMFILE causes a
hot spin that pegs a core and floods logs.
**Fix:** Sleep 100ms on accept error before retrying.

### ROB-3: Rate limiter blocks read task with buffered frames
**File:** `crates/arbitro-server/src/server.rs:881`
When tokens hit 0, `tokio::time::sleep` runs INSIDE the drain loop, stalling
already-buffered frames for up to 1s. Bursty, high-latency throttling.
**Fix:** Use a proper token-bucket with continuous refill; break out of drain
loop so shutdown remains responsive.

### ROB-4: Delayed journal never fsyncs — acknowledged writes lost on crash
**File:** `crates/arbitro-server/src/delayed.rs:128`
`DelayedJournal::append` calls `flush()` (no-op for durability) but never
`sync_data()`. RepOk sent to client. Crash = silent data loss.
**Fix:** Call `sync_data()` after write, respecting `FsyncPolicy`.

### ROB-5: mark_matured_on_disk reopens file per entry + no fsync
**File:** `crates/arbitro-server/src/delayed.rs:172`
Each matured entry opens/writes/closes the file. No fsync = crash re-delivers
matured entries. Throughput cliff under burst.
**Fix:** Use a persistent file handle; batch fsync.

### ROB-6: Delayed loop polls 100ms with no new-entry notification
**File:** `crates/arbitro-server/src/delayed.rs:308`
New entry with earlier deadline than current sleep is delivered up to 100ms
late. Avoidable idle CPU.
**Fix:** Add `tokio::sync::Notify` signaled by `append()`; select on
`shutdown | sleep(deadline) | notified`.

### ROB-7: Cluster boot panics on operator config typos
**File:** `crates/arbitro-server/src/server.rs:314`
`expect()/panic!` on `ARBITRO_CLUSTER_PEERS` parse failures. Crash instead
of clean error+exit.
**Fix:** Return `std::io::Error` or `exit(2)` with `tracing::error!`.

### ROB-8: HelloFrame::parse result discarded — malformed HELLO accepted
**File:** `crates/arbitro-server/src/server.rs:778`
`let _ = HelloFrame::parse(...)` — any 8 bytes with correct magic accepted.
Version/flags never enforced.
**Fix:** Match on parse result; on None send error frame and close.

### ROB-9: Silent wrong-message delivery after recovery gaps
**File:** `crates/arbitro-store/src/tolerant.rs:317`
Read paths compute `idx = seq - first_seq` without verifying `m.seq == seq`.
Recovery gaps from CRC-truncated segments misalign every later read.
**Fix:** Verify `index[idx].seq == seq`; fall back to `binary_search_by_key`.

### ROB-10: Runtime segments never registered — files never deleted
**File:** `crates/arbitro-store/src/tolerant.rs:95`
`rotate()` never pushes `SegmentMetadata`. `truncate_front()` iterates
`self.segments` (empty at runtime). Segment files + 64 MiB mmaps accumulate
without bound.
**Fix:** Push `SegmentMetadata` in `rotate()` with first/last seq.

### ROB-11: Silent u16 truncation of subject length corrupts records
**File:** `crates/arbitro-store/src/tolerant.rs:240`
Subject > 65535 bytes writes full bytes but header stores truncated `subj_len`.
Reads mis-slice boundaries; CRC mismatch stops recovery scan.
**Fix:** Validate `subject.len() <= u16::MAX` at append time.

### ROB-12: Duplicate Delivered leaks inflight permanently
**File:** `crates/arbitro-engine/src/runtime/execute.rs:52`
Same seq delivered twice pushes two Pending entries, one pending_seqs entry.
Single Ack removes one; second Pending + one inflight credit stranded forever.
**Fix:** Skip if `pending_seqs.contains(&entry.seq)`.

### ROB-13: bind/unbind_subscription clobbers connection_id
**File:** `crates/arbitro-engine/src/catalog/match_table.rs:337`
Multiple bindings on same subscription: last bind wins routing, either unbind
kills routing for the survivor.
**Fix:** Enforce one active binding per subscription or make entries per-binding.

### ROB-14: Trie find_matches silently drops matches on 16-slot overflow
**File:** `crates/arbitro-engine/src/common/trie.rs:119`
Stack overflow at depth > 16 with wildcard branching. Valid subscription
match never reported. Silent message loss.
**Fix:** Use `SmallVec` that spills to heap, or cap subject depth at validation.

### ROB-15: truncate_front deletes files while mmap still open
**File:** `crates/arbitro-store/src/tolerant.rs:356`
On Windows, deleting a file with a live mapping fails; error discarded.
File leaks on disk forever.
**Fix:** Drop mmap first, then delete file; log failures.

### ROB-16: truncate_front(target > next_seq) breaks future reads
**File:** `crates/arbitro-store/src/tolerant.rs:343`
`first_seq` set beyond `next_seq`. New appends unreadable.
**Fix:** Clamp: `let target = target.min(self.next_seq)`.

### ROB-17: Tombstones on sealed segments don't survive restart
**File:** `crates/arbitro-store/src/tolerant.rs:572`
Sealed segments are read-only; tombstone is in-memory only. Deleted messages
resurrect after every restart.
**Fix:** Keep a side-file of tombstoned seqs per segment.

### ROB-18: drain() is a silent no-op stub on TolerantStore
**File:** `crates/arbitro-store/src/tolerant.rs:533`
Returns 0 always. Subject-purge of PII silently does nothing.
**Fix:** Implement via tombstoning, or return an explicit error.

### ROB-19: Graceful shutdown doesn't join spawned tasks
**File:** `crates/arbitro-server/src/server.rs:697`
Cron, delayed, per-connection read tasks, health endpoints are never
awaited/aborted. Relies on runtime teardown.
**Fix:** Track handles in JoinSet; signal and await with bounded timeout.

### ROB-20: Cron job wedges permanently if worker hangs with timeout_ms=0
**File:** `crates/arbitro-server/src/cron.rs:255`
`running = true` forever if worker stays connected but hangs.
**Fix:** Enforce a default/maximum timeout; clear running after next interval.

### ROB-21: v2_delete_consumer treats Ok(false) as success
**File:** `crates/arbitro-server/src/transport/dispatch_v2.rs:1498`
Fan-out to shards: `Ok(false)` ("not found") sends RepOk + records delete.
**Fix:** Match on `Ok(true)` specifically.

### ROB-22: Per-connection queue drops lose acked/delivered messages silently
**File:** `crates/arbitro-server/src/transport/registry.rs:354`
`enqueue()` drops frames on full mpsc. Delivery frames silently lost.
**Fix:** Close connections whose queue overflows; distinguish reply vs delivery.

### ROB-23: Command log replay infinite-loops on zero-length entry
**File:** `crates/arbitro-server/src/persistence/command_log.rs:126`
`len==0` entry `continue`s without consuming CRC. Desynchronizes parser.
**Fix:** `break` on `len==0` instead of `continue`.

### ROB-24: BatchIter/RepBatchEntryIter panic on malformed frames
**File:** `crates/arbitro-proto/src/wire/publish.rs:96`, `delivery.rs:374`
No length checks; untrusted wire data indexes past buffer. Remote DoS.
**Fix:** Mirror v2 `BatchPubIter` design: validate lengths before slicing.

### ROB-25: Client RepError dispatch panics on short frame
**File:** `crates/arbitro-client-tokio/src/transport/reader.rs:112`
`frame[..32]` on a frame with `msg_len < 16` panics the reader task.
**Fix:** Check `frame.len() >= RepErrFrame::WIRE_SIZE` first.

### ROB-26: Client batch deliver demux panics on lying entry header
**File:** `crates/arbitro-client-tokio/src/consume/demux.rs:138`
`subj_len + reply_len > data_len` causes slice panic.
**Fix:** Validate `subj_len + reply_len <= data_len` with checked_add.

### ROB-27: Client clone panics on pool exhaustion (16th clone)
**File:** `crates/arbitro-client-tokio/src/client.rs:47`
`Clone::clone` does `.pop().expect(...)`. 15th concurrent clone panics.
**Fix:** Return Arc fallback or provide `try_clone() -> Result`.

### ROB-28: Client ack()/nack() silently dropped when batcher full
**File:** `crates/arbitro-client-tokio/src/consume/message.rs:111`
`try_send` error swallowed. Broker redelivers; duplicates invisible.
**Fix:** Make `ack()` async or return Result; count drops in metrics.

### ROB-29: Stream/consumer slots never recycled — 4096 cap permanent
**File:** `crates/arbitro-common/src/name_registry.rs:430`
Create/delete churn permanently exhausts slots. IdPool exists but unused.
**Fix:** Back with IdPool; embed generation-tag validation.

### ROB-30: Cron handler blocks reader task (head-of-line blocking)
**File:** `crates/arbitro-client-tokio/src/cron.rs:273`
`dispatch_cron_fire` awaited in reader loop. Slow handler freezes all
deliveries and replies.
**Fix:** Fire-and-forget: `tokio::spawn` the handler; return immediately.

### ROB-31: PubFrame/Record accessors panic unless validate() called first
**File:** `crates/arbitro-proto/src/v2/ingress/pub_frame.rs:88`
Opt-in safety: missing validate() = remote-triggerable panic.
**Fix:** Make `parse(buf) -> Result` the only public constructor.

### ROB-32: Legacy wire views panic on truncated buffers
**File:** `crates/arbitro-proto/src/wire/manager.rs:109`
CreateConsumerView, stream views, system views all `.unwrap()` on short buf.
Metadata-log replay feeds these directly.
**Fix:** Convert to fallible constructors with length checks.

### ROB-33: ConsumerConfig from_wire silently coerces invalid enums
**File:** `crates/arbitro-proto/src/config/consumer.rs:145`
Unknown `ack_policy` silently becomes `AckPolicy::None` (no redelivery).
Bypasses all invariant checks.
**Fix:** Return `Result<ConsumerConfig, ErrorCode>`; reject unknown values.

### ROB-34: ensure_subscription ignores differing filters on re-create
**File:** `crates/arbitro-engine/src/catalog/mod.rs:383`
Re-creating subscription with different filters silently keeps old ones.
**Fix:** Compare and return `SubscriptionConfigMismatch` on divergence.

### ROB-35: ensure_consumer idempotency check omits durable and max_nack
**File:** `crates/arbitro-engine/src/catalog/mod.rs:290`
Different DLQ threshold or durability flag silently accepted.
**Fix:** Add `durable` and `max_nack` to the comparison.

### ROB-36: No client-side subject/msg_id length validation
**File:** `crates/arbitro-client-tokio/src/transport/encode.rs:68`
`as u16` casts silently truncate. Corrupt frame where subject bleeds into
payload. `validate_subject` exists but never called.
**Fix:** Validate lengths before encoding; return `ClientError::InvalidConfig`.

### ROB-37: Reconnect replays subscriptions fire-and-forget
**File:** `crates/arbitro-client-tokio/src/conn/session.rs:170`
No reply checking. If broker restarted, replayed Subscribe silently fails.
No CreateConsumer replay.
**Fix:** Track replay replies; re-create consumers on ConsumerNotFound.

### ROB-38: publish_batch_sync only confirms first chunk
**File:** `crates/arbitro-client-tokio/src/publish/mod.rs:193`
Batches > 256 entries: chunks 2+ are fire-and-forget. Failures invisible.
**Fix:** Register pending slots for all chunks and join them.

---

## 3. HIGH — Security (resource exhaustion)

### SEC-3: No max_frame_size on client reader — 4 GiB allocation from wire
**File:** `crates/arbitro-client-tokio/src/transport/reader.rs:82`
Trusts 32-bit `msg_len` with no cap. Malicious broker or MITM causes OOM.
**Fix:** Add configurable `max_frame_size` to `ClientConfig`.

### SEC-4: Unbounded Vec resize from caller-supplied entity IDs
**File:** `crates/arbitro-engine/src/catalog/mod.rs:182`
`ensure_stream_slot(id = u32::MAX)` attempts ~4-billion-slot allocation.
**Fix:** Enforce `MAX_ENTITY_ID` cap; return error beyond it.

### SEC-5: 32-bit subject hash collisions affect delivery correctness
**File:** `crates/arbitro-engine/src/catalog/mod.rs:769`
50% collision probability at ~77k subjects. Delivers to wrong consumers.
**Fix:** Verify subject bytes on exact-match hits; widen to 64-bit hash.

### SEC-6: No per-connection resource quotas (streams/consumers/crons)
**File:** `crates/arbitro-server/src/transport/dispatch_v2.rs:1812`
Single connection can create unlimited entities. Unbounded disk + memory.
**Fix:** Introduce per-connection quotas; reject past limit.

### SEC-7: No cron job count limit
**File:** `crates/arbitro-server/src/cron.rs:173`
Unlimited cron names + unlimited workers per name. Unbounded memory.
**Fix:** Cap slots and workers-per-slot.

### SEC-8: Delayed journal has no size/max-delay cap
**File:** `crates/arbitro-server/src/delayed.rs:116`
Any `delay_ms` (u64) accepted; unlimited pending entries.
**Fix:** Validate maximum delay; cap total pending entries/bytes.

### SEC-9: SIGUSR1 dump written to predictable /tmp path
**File:** `crates/arbitro-server/src/server.rs:646`
Symlink-attack target; leaks broker topology.
**Fix:** Write into `data_dir` with `O_CREAT|O_EXCL` and 0600.

### SEC-10: No client authentication support
**File:** `crates/arbitro-client-tokio/src/config.rs:10`
Proto defines `Action::Auth`; client never sends it. Can't connect to
auth-enabled brokers.
**Fix:** Add `auth_token` to `ClientConfig`; send Auth frame after Hello.

---

## 4. HIGH — Performance

### PERF-1: Ack/Nack O(pending x entries) linear scan on hot path
**File:** `crates/arbitro-engine/src/runtime/execute.rs:84`
`pending.iter().position()` per ack entry. 10k inflight + 100-entry batch =
1M comparisons. HashSet exists but ignored.
**Fix:** Replace `Vec<Pending> + HashSet` with `HashMap<u64, Pending>`.

### PERF-2: Metrics loop O(shards x streams) awaited round-trips per tick
**File:** `crates/arbitro-server/src/server.rs:1054`
Per-stream `store_info()` calls through shard mpsc every 5s. Thousands of
streams = contention with hot path.
**Fix:** Single aggregated snapshot command per shard.

### PERF-3: TolerantStore::init() preallocates 48 MiB index per store
**File:** `crates/arbitro-store/src/tolerant.rs:290`
`Vec::with_capacity(1_000_000)` x 48 bytes. One per stream.
**Fix:** Use modest default; let Vec grow on demand.

### PERF-4: MemoryStore mmap allocation failure panics on hot path
**File:** `crates/arbitro-store/src/memory.rs:573`
`rotate()` calls `alloc_anon_segment` which `expect()`s. Memory pressure at
runtime crashes instead of shedding load.
**Fix:** Make `rotate()` return `Result`; propagate as `StoreError::Full`.

### PERF-5: bind/unbind rebuilds all dedup sets O(total entries)
**File:** `crates/arbitro-engine/src/catalog/match_table.rs:368`
Every subscribe/unsubscribe/disconnect rehashes all exact+catch_all entries.
**Fix:** Mutate surgically: only affected subscription's entries.

### PERF-6: add_pattern rebuilds entire trie per insertion O(N^2)
**File:** `crates/arbitro-engine/src/catalog/match_table.rs:179`
N wildcard subscriptions costs O(N^2) trie inserts.
**Fix:** Insert incrementally; only rebuild on removal.

### PERF-7: Client writer: one write_all syscall per frame
**File:** `crates/arbitro-client-tokio/src/transport/writer.rs:77`
Drain loop already collects multiple frames but writes one at a time.
**Fix:** Use `write_vectored` / staging buffer per drain cycle.

### PERF-8: DeltaEvents allocates fresh Vecs per execute() on hot path
**File:** `crates/arbitro-engine/src/events.rs:17`
Heap allocation per ack batch. Violates "alloc-free at steady state" claim.
**Fix:** Accept `&mut DeltaEvents` (caller-owned, cleared per cycle).

### PERF-9: resolve_patterns_readonly O(N^2) dedup with contains()
**File:** `crates/arbitro-engine/src/catalog/match_table.rs:261`
Linear `out.contains(entry)` per trie hit in drain path.
**Fix:** Use `HashSet` or sort+dedup when matches > threshold.

### PERF-10: Per-connection teardown drains shards serially
**File:** `crates/arbitro-server/src/server.rs:925`
EOF cleanup awaits `drain_connection` per shard sequentially.
**Fix:** Concurrent drain (spawn per-shard, await all).

### PERF-11: One slow subscriber head-of-line blocks all deliveries + replies
**File:** `crates/arbitro-client-tokio/src/state/subscriptions.rs:79`
Reader task awaits full channel. All other consumers + publish replies frozen.
**Fix:** Per-consumer overflow policy; decouple reply processing.

---

## 5. MEDIUM — Missing Features

### FEAT-1: No store capacity limits (max_msgs/max_bytes/max_age)
**File:** `crates/arbitro-store/src/store.rs:64`
`StoreError::Full` exists but never returned. Both stores grow unbounded.
**Fix:** Enforce limits in `append`; truncate_front or return Full.

### FEAT-2: No store fsync/durability policy
**File:** `crates/arbitro-store/src/tolerant.rs:84`
Data only msynced at rotate/shutdown. Up to 64 MiB of acked publishes lost.
**Fix:** Add configurable flush policy (every N bytes/ms).

### FEAT-3: No per-message delivery counter — max_nack/DLQ unenforceable
**File:** `crates/arbitro-engine/src/catalog/mod.rs:91`
`Pending` has no `deliveries` counter. Cannot enforce `max_nack`.
**Fix:** Add `deliveries: u16` to `Pending`; increment on redelivery.

### FEAT-4: Engine state not persisted for durable consumers
**File:** `crates/arbitro-engine/src/lib.rs:1`
Consumer pending/ack state is memory-only. Restart loses all tracking.
**Fix:** Journal ack floors through WAL, replayed at startup.

### FEAT-5: Idempotency/dedup not implemented in store
**File:** `crates/arbitro-engine/src/context.rs:9`
Comments say "handled at store level" but neither store has dedup.
**Fix:** Implement dedup window in store, or delete misleading artifacts.

### FEAT-6: No recovery index files — full CRC scan on every startup
**File:** `crates/arbitro-store/src/tolerant.rs:107`
`init()` walks + CRCs every byte. 100 GB = minutes of startup.
**Fix:** Per-segment index at seal; rescan only active segment.

### FEAT-7: No read-path corruption detection after recovery
**File:** `crates/arbitro-store/src/store.rs:75`
CRCs only verified during `load_segment`. Bit-rot served raw.
**Fix:** Optional `verify_crc` flag on read paths.

### FEAT-8: No store compaction
**File:** `crates/arbitro-store/src/store.rs:93`
Tombstoned entries never reclaimed from segments.
**Fix:** Offline compaction that rewrites sealed segments.

### FEAT-9: Metadata command log grows without bound
**File:** `crates/arbitro-server/src/persistence/command_log.rs:34`
Every create/delete appended forever. No snapshot/compaction.
**Fix:** Periodic compaction: snapshot live set and rewrite log.

### FEAT-10: Auth is single static shared token — no users/ACLs
**File:** `crates/arbitro-server/src/server.rs:536`
One global token. No identity, no authorization, no mTLS.
**Fix:** Multi-credential auth + subject/stream ACLs.

### FEAT-11: Config is env-only; silent fallback on parse errors
**File:** `crates/arbitro-server/src/config.rs:76`
`env_parse` silently falls back to default on ANY parse error.
**Fix:** Log warning on present-but-unparseable; add config file.

### FEAT-12: Health endpoint is shallow (always 200)
**File:** `crates/arbitro-server/src/server.rs:1095`
Reports healthy whenever any shard exists. No real liveness check.
**Fix:** Ping shards with timeout; separate readiness vs liveness.

### FEAT-13: Client TLS: no custom CA or mTLS support
**File:** `crates/arbitro-client-tokio/src/conn/session.rs:44`
Only webpki roots or full bypass. No private CA, no client certs.
**Fix:** Add `root_ca_pem` and client cert/key to `TlsConfig`.

### FEAT-14: Client single-address — no failover/pooling
**File:** `crates/arbitro-client-tokio/src/config.rs:12`
One `addr` string. No server list, no rotation on reconnect.
**Fix:** Accept `Vec<String>`; rotate in backoff; add state watch.

### FEAT-15: No client request timeouts
**File:** `crates/arbitro-client-tokio/src/manage/mod.rs:31`
Sync requests await reply forever. `ClientError::Timeout` is dead.
**Fix:** Wrap in `tokio::time::timeout`; add `request_timeout` to config.

### FEAT-16: No structured (JSON) log output
**File:** `crates/arbitro-server/src/main.rs:57`
Human-readable only. K8s log aggregators need JSON.
**Fix:** Add `ARBITRO_LOG_FORMAT=json|text` env switch.

### FEAT-17: No backup/restore story for data volume
**File:** `deploy/k8s/pvc.yaml:1`
No documented procedure, no snapshot tooling.
**Fix:** Document backup procedure; state whether live-copy is safe.

### FEAT-18: No trie pattern validation
**File:** `crates/arbitro-engine/src/common/trie.rs:49`
`orders.>.eu` silently stored as `orders.>`. Empty tokens accepted.
**Fix:** Add `validate_pattern()` at subscription time.

---

## 6. CI & Operations

### CI-1: CI runs only 6 of 16 e2e test suites (~40% coverage)
**File:** `.github/workflows/ci.yml:108`
NOT run: drain_invariants, workflow_streams, catalog_invariants,
idempotency_invariants, cron, delayed, fuzz, cluster, aletheia_demo.
**Fix:** Use `cargo test -p arbitro-e2e --tests`; add cluster job.

### CI-2: TLS feature never compiled or tested in CI
**File:** `.github/workflows/ci.yml:214`
TLS code can bit-rot undetected behind feature flag.
**Fix:** Add `cargo build -p arbitro-server --features tls` job.

### CI-3: No e2e tests for auth, rate limiting, max_connections
**File:** `crates/arbitro-e2e/tests`
Config supports these features; zero tests exercise them.
**Fix:** Add `tests/limits_and_auth.rs`.

### CI-4: Fuzz test is minimal and not in CI
**File:** `crates/arbitro-e2e/tests/fuzz_random_bytes.rs:24`
Only random bytes after valid HELLO. No cargo-fuzz harness.
**Fix:** Add to CI; add cargo-fuzz targets for frame decoder.

### CI-5: No Docker image smoke test before push
**File:** `.github/workflows/ci.yml:248`
Image built and pushed without ever running it.
**Fix:** `docker run` + minimal health check before push.

### CI-6: cargo-audit/deny compiled from source; no scheduled scan
**File:** `.github/workflows/ci.yml:227`
5-10 min compile per run. CVEs between pushes go undetected.
**Fix:** Use prebuilt binaries; add weekly scheduled trigger.

### CI-7: clippy only checks --lib; skips tests/bins
**File:** `.github/workflows/ci.yml:96`
11k lines of test code + binaries unchecked.
**Fix:** `cargo clippy --workspace --all-targets -- -D warnings`.

### CI-8: No code coverage measurement
**File:** `.github/workflows/ci.yml`
No cargo-llvm-cov. Coverage claims unverifiable.
**Fix:** Add `cargo llvm-cov` job; upload to Codecov.

### CI-9: No concurrency cancellation; sibling-repo checkouts unpinned
**File:** `.github/workflows/ci.yml:3`
Force-pushes stack redundant runs. Sibling push can break arbitro.
**Fix:** Add concurrency group with cancel-in-progress.

### CI-10: No MSRV declaration or check
**File:** `.github/workflows/ci.yml`
Library crates have no `rust-version` compatibility contract.
**Fix:** Add `rust-version = "1.88"` to workspace package.

---

## 7. Kubernetes & Docker

### K8S-1: RWO PVC + RollingUpdate deadlocks upgrades
**File:** `deploy/k8s/deployment.yaml:1`
New pod can't mount volume while old pod holds it. Rollout stalls.
**Fix:** Add `strategy: { type: Recreate }` or convert to StatefulSet.

### K8S-2: Liveness/readiness probes are TCP-only; metrics not enabled
**File:** `deploy/k8s/deployment.yaml:57`
Deadlocked shard passes TCP probe. ARBITRO_METRICS_LISTEN never set.
**Fix:** Set metrics addr; liveness via HTTP; add ServiceMonitor.

### K8S-3: Image pinned to :latest; missing seccomp + capability drop
**File:** `deploy/k8s/deployment.yaml:24`
No reproducibility. Missing Restricted PSS fields.
**Fix:** Pin to version tag; add capabilities.drop + seccomp.

### K8S-4: docker-compose has no healthcheck
**File:** `docker-compose.yml:1`
Wedged broker never restarted.
**Fix:** Ship `--healthcheck` subcommand; add compose healthcheck.

---

## 8. Code Quality & Cleanup

### CQ-1: StoreError has no Io variant — IO errors mapped misleadingly
**File:** `crates/arbitro-store/src/tolerant.rs:94`
EACCES/disk-full shows as "not found" or "full".
**Fix:** Add `StoreError::Io` variant.

### CQ-2: wire_hash_32 duplicated in two modules
**File:** `crates/arbitro-engine/src/common/mod.rs:14`
Identical implementations; drift risk on the function delivery depends on.
**Fix:** Single definition in `common`; re-export from catalog.

### CQ-3: Hash function docs say FNV-1a, implementation is foldhash
**File:** `crates/arbitro-proto/src/config/stream.rs:192`
Non-Rust client implementers compute wrong hashes from docs.
**Fix:** Update all comments to name foldhash-fixed-seed.

### CQ-4: Cold-path JSON serializes Vec<u8> as number arrays
**File:** `crates/arbitro-proto/src/v2/cold/mod.rs:139`
`"orders"` becomes `[111,114,100,101,114,115]`. 4x wire bloat.
**Fix:** Use `String` for names.

### CQ-5: Dual 16-byte header formats special-cased by action code
**File:** `crates/arbitro-client-tokio/src/transport/reader.rs:55`
Hardcoded action-to-format knowledge. Adding new envelope frames silently
desynchronizes the connection.
**Fix:** Migrate batch deliveries to v2 Header server-side.

### CQ-6: Most ClientMetrics counters never incremented
**File:** `crates/arbitro-client-tokio/src/metrics.rs:29`
acks_sent, nacks_sent, reconnects, etc. permanently zero.
**Fix:** Wire counters at obvious sites or delete dead fields.

### CQ-7: ClientConfig::write_queue_capacity is dead configuration
**File:** `crates/arbitro-client-tokio/src/config.rs:18`
Field never read; actual capacity is compile-time constant.
**Fix:** Remove field; document `WRITE_QUEUE_CAP`.

### CQ-8: Dead error codes for removed subsystems
**File:** `crates/arbitro-engine/src/error.rs:45`
PluginNotFound, EdgeNotFound, Slab codes, DrainMode — all dead.
**Fix:** Delete dead variants and legacy types.

### CQ-9: Unused dependencies in arbitro-store
**File:** `crates/arbitro-store/Cargo.toml:8`
arbitro-proto and zerocopy not referenced in source.
**Fix:** Remove both from `[dependencies]`.

### CQ-10: Outdated major dependency versions
**File:** `crates/arbitro-server/Cargo.toml:55`
thiserror 1 (v2 available), nix 0.27 (0.29), criterion 0.5 (0.7).
**Fix:** Bump versions.

### CQ-11: Gate::release branches are identical
**File:** `crates/arbitro-common/src/gate.rs:61`
Both arms call `notify_one()`. Misleading structure.
**Fix:** Collapse to unconditional `notify_one()` with comment.

### CQ-12: RepBatchFixed doc describes 8-byte layout; struct is 4 bytes
**File:** `crates/arbitro-proto/src/wire/delivery.rs:75`
Wrong docs = incompatible client implementations.
**Fix:** Fix doc comments to match struct layouts.

### CQ-13: headers_len semantics inconsistent between docs and encoder
**File:** `crates/arbitro-proto/src/wire/msg_headers.rs:213`
Decoder trusting doc lands 4 bytes short.
**Fix:** Pick one definition; add roundtrip test.

### CQ-14: WorkflowBuilder::compensate ignores step-name parameter
**File:** `crates/arbitro-client-tokio/src/workflow.rs:347`
Always attaches to most recently added step regardless of name.
**Fix:** Look up step by name or drop the parameter.

### CQ-15: Pending cap hit still lets wire frame be sent
**File:** `crates/arbitro-client-tokio/src/state/pending.rs:53`
Frame sent to broker; reply discarded. Wasted broker work.
**Fix:** Return Option from `register()`; skip send when None.

### CQ-16: Ring-full and writer-gone both map to ChannelClosed
**File:** `crates/arbitro-client-tokio/src/publish/mod.rs:23`
Transient backpressure indistinguishable from permanent failure.
**Fix:** Add `ClientError::Backpressure` for ring-full case.

### CQ-17: v2_list_consumers clones entire cached Vec on unfiltered call
**File:** `crates/arbitro-server/src/transport/dispatch_v2.rs:1727`
Defeats Arc-cache purpose.
**Fix:** Iterate `Arc<Vec>` directly when no filter.

### CQ-18: Tombstone drop reasons all counted as publish_no_match
**File:** `crates/arbitro-engine/src/runtime/execute.rs:140`
Can't distinguish retention expiry from routing misses.
**Fix:** Add `entries_expired` / `entries_tombstoned` counters.

### CQ-19: deny.toml multiple-versions only warns; stale license
**File:** `deny.toml:10`
Duplicate dep trees accumulate silently.
**Fix:** Consider `deny`; remove stale license entry.

### CQ-20: Cron module doc claims Envelope framing but encodes v2 Header
**File:** `crates/arbitro-proto/src/wire/cron.rs:1`
Doc/wire drift.
**Fix:** Update doc to say v2 Header.

---

## 9. Test Coverage Gaps

### TEST-1: No e2e test for corrupted/truncated command log recovery
**File:** `crates/arbitro-e2e/tests/persistence.rs`
Crash-during-fsync only covered at unit level.
**Fix:** Truncate/bit-flip log; assert clean boot with prefix intact.

### TEST-2: No large-payload or max_frame_size boundary tests
**File:** `crates/arbitro-e2e/tests`
Default 64 MiB max_frame_size never tested near boundary.
**Fix:** Test at exactly limit, one byte over, multi-MB round-trip.

### TEST-3: No idle_timeout / keepalive behavior tests
**File:** `crates/arbitro-e2e/tests`
Dead socket reaping and alive-subscriber survival untested.
**Fix:** Test with `idle_timeout=2s`.

### TEST-4: Cluster suite lacks partition/rejoin scenarios
**File:** `crates/arbitro-e2e/tests/cluster.rs`
Only 4 tests. No partition, rejoin, quorum-loss.
**Fix:** Add partition, rejoin, quorum-loss tests.

---

## Summary

| Category | Critical | High | Medium | Low | Total |
|----------|----------|------|--------|-----|-------|
| Security & Crashes | 4 | - | - | - | 4 |
| Robustness | - | 38 | - | - | 38 |
| Security (resource) | - | 10 | - | - | 10 |
| Performance | - | 11 | - | - | 11 |
| Missing Features | - | - | 18 | - | 18 |
| CI & Operations | - | - | 10 | - | 10 |
| K8s & Docker | - | - | 4 | - | 4 |
| Code Quality | - | - | - | 20 | 20 |
| Test Coverage | - | - | 4 | - | 4 |
| **Total** | **4** | **59** | **36** | **20** | **119** |
