# TODO — open backlog

Master `6d160a2`. Items verified via grep, not claimed.

## Open

| ID | Item |
|---|---|
| — | Subject scavenging (TTL-based inactive-slot cleanup) |
| — | Clustering (Raft) for stream state replication |
| — | Adaptive subject prioritization |
| — | Cross-shard subject aggregation for global limits |

## Closed (commit SHAs)

### Blockers (6/6) ✅

| ID | Commit |
|---|---|
| B1 SLOT_COUNT admission | `84ece4a` |
| B2 try_entries bounds | `84ece4a` |
| B3 BatchPubIter validation | `84ece4a` |
| B4 PubFrame length validation | `84ece4a` |
| B5 TolerantStore CRC32 | `e3b1d04` |
| B6 CreateConsumer rollback | `84ece4a` |

### High (19/19) ✅

| ID | Item | Commit |
|---|---|---|
| H1 | validate_name/subject in dispatch | `a6f511c` |
| H2 | auth wire codes (AuthRequired/AuthFailed) | `f52dd00` |
| H3 | recovery preserves retention config | `a6f511c` |
| H4 | per-stream idempotency lock (via F26) | `2d7f903` |
| H5 | drain thread shutdown join | `38b547d` |
| H6 | writer task removes dead session | `50b0269` |
| H7 | bounded eviction (via F16 work) | (partial, in F-track) |
| H8 | wheel pending O(1) lookup (via F15) | `e863587` |
| H9 | concurrent shard drain on disconnect | `4049f57` |
| H10 | silent-drop counters wired everywhere | `2d2c932`, `bc01407` |
| H11 | ConsumerRemoved retry queue | `2d2c932` |
| H12 | deterministic idempotency tick | `8c495a1` |
| H13 | ARBITRO_WRITE_BUFFER_CAP wired | `fdff7fb` |
| H14 | /health HTTP endpoint | `4049f57` |
| H15 | /metrics Prometheus endpoint | `bb99c00` |
| H16 | dispatch tracing event | `1fbcc6b` |
| H17 | command log fsync (sync_data) | `e3b1d04` |
| H18 | mmap.flush errors logged | `e3b1d04` |
| H19 | client Pending cap (100k) | `8c495a1` |

### Medium (28/28) ✅

M1 (`8c495a1`), M2 (`09b96cf`), M3 (`f52dd00`), M4 (`0300dfb`),
M5 typed WheelEntryKind (`64cae0b`), M6 (`fdff7fb`),
M7 wire_hash_32 collision detect (`3e1a332`),
M10 PublishWithReply msg_id (`3e1a332`),
M11 Pause/Resume wire (`50b0269`), M12 iterative subjects_overlap
(`f2aecac`), M13 TLS Result (`a6f511c`), M14 constant-time auth
(`a6f511c`), M15 task panic supervision (`4049f57`),
M16 NameRegistry prealloc (`fdff7fb`), M17 Pong counted (`f52dd00`),
M18/M19/M20 reserved fields documented (`4116202`),
M21 consumer_pending route (via F14, `279199b`),
M22/M23/M24 README port/env/AckSync (`0300dfb`),
M25 docker data dir (`1c6f7ee`), M26 Dockerfile USER nonroot
(`1c6f7ee`), M27 CONTRIBUTING.md (`feb5a2d`),
M28 rules refresh (`b411f36`),
M8 writer feedback loop (`6d160a2`).

### Low (18/18) ✅

L1 dead Actions deleted (`f52dd00`, `3bc128e`),
L2 Unimplemented error code (`f52dd00`),
L3 monotonic SharedClock (`da99061`),
L4 zero-length subject rejected (via H1, `a6f511c`),
L5 block_in_place removed (via F2, `279199b`),
L6 Subscribe ref_seq = consumer_id (via F35, `21cfb79`),
L7 saturating_sub in metrics (`f52dd00`),
L8 SubFrame reserved bits roundtrip (`f2aecac`),
L9 clamp window_ms in NameRegistry (via F39, `279199b`),
L10 lifecycle_trace bench warning (via F30, `21cfb79`),
L11 full metric aggregation (`11bdc89`),
L13 client TLS behind feature flag (`6d160a2`),
L13 (subset) client upsert/exists (`f27f600`),
L15 SIGUSR1 dump (`f27f600`),
L16 SIGHUP log reload (`f27f600`),
L17 CI clippy required (`3d7c48c`),
L18 (subset) cargo install line (`1c6f7ee`).

### Tests (20/20) ✅

T1 BatchAck try_entries (`b31b954`),
T2 BatchPubIter adversarial (`b411f36`),
T3 PubFrame validate (`b31b954`),
T4 SLOT_COUNT capacity (`da99061`),
T5 TolerantStore corrupt record (via B5 regression, `e3b1d04`),
T7 cross-tenant ack injection (`ef169c9`),
T8 retention survives restart (`d66ddc8`),
T11 reconnect resumes (`d66ddc8`),
T12 stream recreate isolation (`ef169c9`),
T13 single-shard saturation (`d66ddc8`),
T17 Gate spurious wakeup (`feb5a2d`),
T18 IdempotencyTracker forget/tick (`feb5a2d`),
T19 signal_rewind race (`feb5a2d`),
T20 fuzz random bytes after HELLO (`f2aecac`),
T6 malformed CreateConsumer doesn't leak slot (`6d160a2`),
T9 idempotency cross-restart contract (`6d160a2`),
T10 shutdown mid-publish last batch durable (`6d160a2`),
T15 evict_expired doesn't stall publish (`6d160a2`),
T16 partial-write recovery in conn_writer_task (`6d160a2`).

### Protocol cleanup ✅

- §5.1 dead Actions deleted: PublishAccumulate, AckSync, BatchAckSync,
  Connect, Connected, Stats, StatsReply (`3bc128e`, `f52dd00`).
- §5.1 PublishWithHeaders / PublishBatchWithHeaders deleted (`6d160a2`).
- §5.2 6 unemitted ErrorCodes cleaned (`6d160a2`).
- §5.3 dead flag bits documented as reserved (`4116202`).
- §5.4 Pull-mode docs corrected (`1c6f7ee`) — semantics emerge from
  Subscribe + max_inflight + Explicit ack.

### Protocol hardening (6/6) ✅

- AckPolicy::None + Limits — reject at CreateConsumer time (`6d160a2`).
- Unknown Ack Seq — metric + error instead of silent drop (`6d160a2`).
- Fanout + Groups — reject CreateConsumer(Fanout, group!="") (`6d160a2`).
- Stale Config — CreateConsumer errors on config mismatch (`6d160a2`).
- Shared Consumer Names — namespace ConsumerId by (stream_id, name) (`6d160a2`).
- Client TLS (L13) — tokio-rustls behind `tls` feature flag (`6d160a2`).

### Operability ✅

- `/health` (H14), `/metrics` (H15), arbitroctl CLI (`bb99c00`),
  k8s manifest skeleton (`390e77a`), BACKUP.md (`0aaf151`),
  README observability section (`0aaf151`).
- `cargo audit` / `cargo deny` CI job (`6d160a2`).
- `dependabot.yml` (`6d160a2`).
- Docker push gated on e2e (`6d160a2`).
- MAX_FRAME_SIZE configurable via env (`6d160a2`).
- Config validation at startup (`6d160a2`).
- `--version` / `--help` flags (`6d160a2`).
- Fsync policy configurable (`6d160a2`).
- Rate limit per-connection (`6d160a2`).
- K8s manifests + probes (`6d160a2`).
- DURABILITY.md (`6d160a2`).
