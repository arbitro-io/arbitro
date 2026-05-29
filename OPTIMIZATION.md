# OPTIMIZATION — performance backlog

> Status as of master `21cfb79`. Original audit at commit `3d2c719`
> (tag `v0.1.0`) on 2026-05-12. Performance-focused companion of
> [`TODO.md`](./TODO.md). Correctness items live there.
>
> Project performance rules (`.agents/rules/performance.md`):
> 1. **Zero allocations on the steady-state publish / ack / drain path.**
> 2. **No mutex held across I/O or async await.**
> 3. **Dense IDs → `Vec<T>` direct indexing, sparse → `HashMap` with foldhash.**
> 4. **One timestamp per batch — never `Instant::now()` per message.**

## Status summary

- **Completed**: 31 of 39 F items + S1 (NameRegistry hot-path read split).
  See `git log --grep='F[0-9]'` for the closing commits.
- **Pruned as unnecessary** (4 items): F23, F31, F33, F37. Reasons
  below — each was verified to either be disproven by a benchmark,
  target a cold path, or give zero measurable gain.
- **Reinstated** (1 item): F34 — see Remaining work.
- **Genuinely pending** (4 items + 5 structural): F9, F16, F34, F36 +
  S2–S5, S7.

## Pruned with reason

These items are removed from the backlog. They were on the original
audit list but verification shows the change either does not help or
helps something that doesn't need helping.

### F23 — pool `Vec<DeliveredEntry>` between notifications
**Removed.** The notification crosses a SPSC ring from drain to
command thread; ownership transfers out of any pool the drain holds.
Pooling on the drain side gains nothing because the receiver consumes
and drops. A real fix would need a return path (drain → ring → command
ack → drain reclaims). Cost vs benefit not justified.

### F31 — hybrid `local_inflight` Vec/HashMap above N≥8
**Removed.** Vec linear scan wins at N < 16 thanks to cache locality
(0.7–3 ns per op vs 1.4 ns for HashMap+foldhash). Typical N is 1–4
consumers per drain cycle (`drain.rs:90`). The threshold proposed by
the audit was below the crossover. *(Note: the `local_delta.rs` bench
referenced by the original audit is not in the tree — rationale is
from first-principles analysis and code comments.)*

### F33 — Header re-parsed on every dispatch frame
**Removed.** `Header::ref_from_bytes` is a `zerocopy` cast — zero
runtime work, the compiler inlines the field accesses. Threading the
parsed header from `read_loop` into `dispatch_frame_v2` would add an
argument without saving any cycles.

### ~~F34~~ — moved back to Remaining work
~~Removed (wrong target).~~ **Reinstated.** Audit found `send_parts`
IS called on the publish-reply hot path: `send_rep_ok_v2` →
`registry.send_parts()` runs after every successful `v2_publish`,
`v2_publish_batch`, and `v2_publish_with_reply` (`dispatch_v2.rs:225`,
`:316`). The deliver hot path bypasses the registry (drain writes
directly via `write_tx`), but every publish acknowledgment allocates a
`BytesMut` through `send_parts`. See F34 entry under Remaining work.

### F37 — `ArcSwap<Vec<StreamInfo>>` snapshot for list_streams
**Removed (wrong target).** `list_streams` is admin / management, not
hot. At ≤ 1 query/s the 16-shard mpsc round-trip is sub-microsecond
amortised. Investing in an eager-rebuild snapshot saves cycles
nobody is paying.

## Remaining work (in priority order)

### F9 — `MetricsSnapshot` aggregation does N × M shard round-trips per tick
**Status: NOT started.** Both the Prometheus `/metrics` endpoint
(`server.rs:1047–1056`) and the periodic log aggregator use the same
shard-command fan-out: 3 commands (`shard.metrics()`,
`shard.list_streams()`, `shard.consumer_states()`) × N shards per
tick, each as a oneshot request-reply through the shard mpsc channel.
`EngineMetrics` is owned inline on `EngineContext` (not
`Arc`-shared), so no direct atomic reads from outside the worker are
possible today.
**To close**: hoist counters into `Arc<EngineMetrics>` shared between
worker and the metrics/prometheus tasks, read with `Relaxed` loads,
drop the `Metrics` shard command entirely. ~1–2 days.

### F16 — `evict_expired` walks the whole store every 5 s under the publish lock
**Status: partially addressed.** A bounded walk cap
(`EVICT_WALK_CAP = 10_000` at `handlers.rs:772`) and an incremental
resume cursor (`evict_resume_seq` at `worker.rs:386`) are already
implemented. On each tick the scan processes at most 10 K entries,
saves the resume position (`handlers.rs:877`), and picks up where it
left off on the next 5 s tick (`handlers.rs:820`).
**Remaining**: the store mutex is still held during the capped walk.
Per-stream `oldest_ts` cache would let eviction skip streams entirely
when no entries are expired, reducing lock hold time from O(10 K) to
O(streams). ~1 day.

### F34 — pool `BytesMut` for publish-reply `send_parts`
**Status: not started.** Every successful publish replies via
`send_rep_ok_v2` → `registry.send_parts()` (`reply_v2.rs:21`), which
allocates a fresh `BytesMut`, copies the 24 B `RepOkFrame`, and
freezes. At 100 k+ publishes/s this is 100 k+ small allocations.
**Approach**: thread-local or per-connection `BytesMut` pool for the
fixed-size `RepOkFrame`. The frame is always 24 B, so a single
pre-allocated buffer per connection suffices. ~0.5 day.

### F36 — `BoxedWriter = Box<dyn AsyncWrite>` adds vtable per write
**Status: not started.** At 100k+ writes/s the indirect call costs a
measurable few percent. A full fix turns the accept loop's writer
into `enum Writer { Plain(OwnedWriteHalf), Tls(TlsStream) }` —
monomorphised, no vtable. Multi-day refactor of `transport::registry`
and the connection task. Owner-led decision.

### S2 — Lift `block_in_place` + merge publish-side store mutex with drain-side lock
Already partly done — F2 dropped `block_in_place`. The remaining
structural win is collapsing the publish/drain mutex contention via
SeqLock or per-shard SPSC publish. ~1 week + benchmarks.

### S3 — Replace dense-keyed `HashMap`s in catalog with `Vec<Option<…>>`
Engine catalog (`catalog/mod.rs:134–155`) still has
`streams: HashMap<StreamId, …>`, `consumers: HashMap<ConsumerId, …>`,
`subscriptions: HashMap<SubscriptionId, …>`,
`bindings: HashMap<BindingId, …>`, `demand: HashMap<StreamId, …>` —
all keyed by dense-monotonic u32 IDs and candidates for
`Vec<Option<…>>` direct indexing.
**Already Vec-indexed**: `match_tables` (by `StreamId.raw()`),
`SharedCounters` (fixed-size `Box<[AtomicU32]>` by raw ID).
**Must stay HashMap**: `by_connection` and `connections` — keyed by
unbounded-monotonic `ConnectionId` (u64 atomic counter,
`shared.rs:262`).
Engine-side refactor, ~3 days.

### S4 — Per-shard concurrent connection routing
`ConnectionRegistry` is a single `parking_lot::Mutex<HashMap<u64,
Session>>`. F8 made `touch()` lock-free via per-session `AtomicU64`
for `last_activity`, but the registry itself is not sharded. Every
publish reply still calls `enqueue()` which takes the global mutex.
The drain hot path already bypasses the registry (writes directly via
`write_tx`), so the main contention is on publish-reply dispatch. A
per-shard local registry slice would remove that. ~1 week.

### S5 — Drain Phase 1/2/3 reuse store guard on backpressure retry
Today on backpressure the drain releases the store lock, the next
cycle re-takes it. Caching the read result across cycles eliminates
the redundant lock acquisitions when downstream is full. ~2 days.

### S6 — Publish flow off dispatch tokio task into shard-local SPSC
**Removed (incorrect framing).** The client already serializes its
outbound frames through `arbitro_kit::Mpsc` before the socket, and the
server's `read_loop` is one task per TCP connection — there is no
intra-connection race to eliminate. Adding a server-side ring on top
puts TWO hops in the publish path (client ring + server ring) where
there was one (the store mutex). At ~80 ns per ring hop vs ~30 ns
uncontested `parking_lot::Mutex` acquire, this is a 50-60% latency
regression in the common case for zero gain. The only contention
mode left is "N connections publish to the SAME shard simultaneously",
which would need MPSC (not SPSC) and even then the real bottleneck at
saturation is fan-out delivery, not store ingress.

### S7 — Flatten match table `exact` lookup
**Partially done.** The `binding_idx` stamping (Fase C.2,
`match_table.rs:49`) is already implemented — drain uses
`bindings[match_entry.binding_idx]` for direct Vec access
(`drain.rs:649–655`) instead of a HashMap lookup on
`(consumer_id, connection_id)`.
**Remaining**: `mt.lookup(subject_hash)` still probes
`exact: HashMap<u32, Vec<MatchEntry>>` (`match_table.rs:226`).
Since subject hashes are sparse 32-bit values, HashMap is the correct
structure here. A perfect-hash or flat array only helps if hashes were
dense. The drain already caches resolved results per cycle via
`resolve_cache` in `DrainScratch` (`drain.rs:73`). Further gains
are marginal. Consider closing unless profiling shows lookup as a
bottleneck. ~0.5 day to evaluate, ~1 week if a custom hash table
is warranted.

## Priority order — pick wins by impact-per-effort

Given the closed items have already taken publish RTT down by the
measured ~40% projected in the original audit, the remaining list is
diminishing returns territory. If a 2-week sprint goes here, pick:

1. **F9** — hoist `EngineMetrics` into `Arc`, read atomics directly
   from metrics/prometheus tasks (~1–2 days). Eliminates per-tick
   shard fan-out jitter for both `/metrics` and the periodic log.
2. **F34** — pool `BytesMut` for publish-reply `send_parts` (~0.5
   day). Eliminates 100 k+ small allocs/s at high publish rates.
3. **F16 remainder** — per-stream `oldest_ts` cache (~1 day). The
   bounded walk cap is already in place; this further reduces lock
   hold time by skipping streams with no expired entries.
4. **F36 enum monomorph** (~2–3 days). 1–3 % on writes; useful only
   if writes are the measured bottleneck.

Everything else (S2–S5, S7) is a quarter-scale project and should be
gated on a real workload that's hitting the existing ceiling.
