# arbitro-server — Robustness & Invariant-Coverage Audit

Auditor: Fable (convergent audit, static-first). Date: 2026-07-27.
Scope: `crates/arbitro-server/src/**` at the current working tree, with supporting reads of
`arbitro-engine` (execute path, match table), `arbitro-common` (Gate), `arbitro-store` usage sites,
`arbitro-e2e/tests/**`, and `arbitro-client-tokio/tests/deliver_demux.rs`.
Every claim below cites a file:line actually read. Items that could not be confirmed by reading
are labeled **UNVERIFIED — needs test/repro**.

---

## 1. Executive summary

The server's frame-parsing boundary and single-shard happy path are in good shape: wire input is
bounds-checked (B2/B3/B4), the Gate is lost-wake-safe, the historical drain shutdown deadlock is
fixed and guarded, and the e2e suite genuinely covers fanout/queue-group/idempotency/persistence.
The core structural weakness is the **split-brain between the drain's atomic inflight accounting
and the engine's pending truth**: the drain increments per delivery with no redelivery dedup while
the ack path decrements blindly by request size — the two books drift in both directions, up to and
including u32 underflow, permanently wedging a consumer or queue (**P0, 2 findings**). Second-tier:
the per-shard drain cursor is shared by all consumers, so any rewind (nack, late subscriber,
ack-timeout) re-delivers already-acked messages to *other* consumers — legal under at-least-once but
unbounded, untested, and the amplifier for the P0 drift. Recovery silently loses three configs
(consumer ack cursors are never persisted; quota/discard and deliver_policy are not replayed).
Cluster data-plane replication has no working catch-up path, so a single dropped batch desyncs a
follower forever. **P0 count: 2. Biggest invariant gaps:** cross-consumer temporal isolation on
cursor rewind, unknown/double-ack handling, cursor persistence across restart, fsync-policy
durability proof.

---

## 2. Part A — Findings by dimension

Severity: **P0** = correctness/liveness blocker reachable in normal operation; **P1** = serious,
reachable with realistic traffic or failure; **P2** = minor / latent / bounded.

### 2.1 Soundness / memory safety

| Sev | Location | Finding / failure scenario |
|-----|----------|----------------------------|
| P1 | `src/cluster/transport.rs:820-822` | `transmute::<&[&[u8]], &'static [&'static [u8]]>` erases the borrow of caller-owned buffers into a `Send` future. Soundness rests on an unchecked cross-crate comment ("RaftNode guarantees the slices live until this future completes"). Any future caller that spawns or outlives-polls this future after freeing the buffers reads freed memory and writes it to the network. Not locally enforceable; already in the raft-audit P0 family — this is the server-side site. |
| ~~P2~~ **FIXED (ROB-24)** | `src/cluster/storage.rs` (`read_entries` error path) + `src/cluster/apply_loop.rs:96-101` | ~~`read_entries` pushes `LogEntry<'a>` views into `out` *before* returning `Err("payload_buf too small")` … invalidated-reference UB one refactor away from a real use-after-free.~~ Fixed: `out.clear()` on the error path — an `Err` never leaks partially-filled transmuted views. Unit: `storage.rs read_entries_error_path_clears_partial_fill` (asserts `out` empty on error + clean retry after resize). |
| ~~P2~~ **FIXED (ROB-24)** | `src/persistence/command_log.rs` (`MAX_REPLAY_ENTRY_LEN`) | ~~Replay allocates `vec![0u8; len]` from an untrusted length prefix (up to 4 GiB) before reading. A corrupted/hostile command-log file can OOM the broker at boot.~~ Fixed: `len` capped at `MAX_REPLAY_ENTRY_LEN` (1 MiB — metadata commands are tiny); an over-cap length means the framing itself is corrupt (no resync possible), so replay stops there like the truncated-tail path. `record()` symmetrically refuses over-cap commands so a legit write can never poison the tail. Units: `over_cap_entry_length_stops_replay_without_alloc`, `record_rejects_over_cap_command`. |
| ~~P2~~ **FIXED (ROB-24)** | `src/transport/dispatch_v2.rs` (`v2_publish_batch` entry build) | ~~Batch publish: msg-ids are correctly extracted from HAS_HEADERS `ExtendedPayload` for dedup, but the stored entries are written with `flags: 0`. Consumers then receive the raw ExtendedPayload TLV bytes as payload and restart dedup rebuild misses these ids.~~ Fixed: batch entries now mirror the single-publish flag resolution — HAS_HEADERS batches store `arbitro_store::flags::HAS_HEADERS` (drain strips the TLV, recovery finds the ids), and dedicated-field msg_id entries are server-wrapped into ExtendedPayload (single-publish case 2) so batch dedup also survives restart. E2e (fail→pass): `idempotency_invariants.rs batch_publish_with_headers_delivers_clean_payload_and_dedups_after_restart` (raw-frame foreign-client batch; asserts clean delivered payloads + post-restart duplicate rejection). |
| OK | `src/transport/dispatch_v2.rs:250-261, 513-532, 809, 887, 1004` | Wire boundary is properly hardened: `PubFrame::validate()` (B4), batch entry-count cross-check (B3), `try_entries`/`try_seqs` bounds-checked views (B2). Payload sub-slicing in the drain is length-guarded (`src/shard/drain.rs:787-817`). Fuzz test exists: `arbitro-e2e/tests/fuzz_random_bytes.rs:24`. |

No `unsafe` outside the two cluster sites above (`grep unsafe src/` → storage.rs:706,833, transport.rs:822).

### 2.2 Concurrency correctness

| Sev | Location | Finding / failure scenario |
|-----|----------|----------------------------|
| **P0** | `src/shard/drain.rs:413-419` vs `arbitro-engine/src/runtime/execute.rs:49-67` | **Inflight double-count on redelivery.** The engine dedups `Delivered` for already-pending seqs (ROB-12, execute.rs:54), but the drain increments `SharedCounters` per delivery unconditionally (drain.rs:415). Any cursor rewind that re-walks a window containing a consumer's still-pending (delivered, unacked) seqs — a partial nack (`handlers.rs:449-456`), a late subscriber (`handlers.rs:557-559`), a wheel timeout — re-delivers those seqs and bumps the atomic again. Acks later decrement once per seq. Net: phantom inflight accumulates until `consumer_has_capacity` (`src/shard/shared.rs:124-126`) is permanently false → consumer starves, cursor pins at its skipped seq, drain busy-polls. Concrete repro: deliver 1..10, ack 1..4, nack 5 → rewind to 4 → seqs 6..10 redelivered (capacity permits) → atomic +5 forever. |
| **P0** | `src/shard/handlers.rs:264-275` (also `:390-398`, `:435-443`) vs `arbitro-engine/src/runtime/execute.rs:84-107` | **Blind decrement on ack/nack → u32 underflow.** The server sets `accepted = cmd.entries.len()` and calls `dec_inflight_bulk` by that amount regardless of how many entries the engine actually matched (`matched` at execute.rs:98; unmatched only bumps a metric). A double-ack, an ack for a never-delivered seq, or an ack raced with a wheel auto-nack decrements an inflight of 0 → `fetch_sub` wraps (`src/shard/shared.rs:104-114`, no clamp) → `consumer_inflight ≈ u32::MAX` → `consumer_has_capacity` false forever → the consumer AND its whole queue (`queue` counter, shared.rs:106) are wedged until restart. Reachable by an ordinary client retry; ownership check (handlers.rs:229-234) does not protect against a consumer wedging itself/its group. Only `v2_ack_batch` pre-filters `seq <= cursor` (`dispatch_v2.rs:931-944`); `v2_ack` and `v2_batch_ack` do not. |
| P1 | `src/shard/handlers.rs:449-456` and `src/shard/worker.rs:808-816` | **Rewind protocol violated by nack and wheel_tick.** Both do `set_cursor(min-1)` + unconditional `clear_rewind()` and never `signal_rewind`. A drain cycle already past its `take_rewind` (worker.rs:237-242) writes `new_cursor` forward at cycle end (drain.rs:450), clobbering the rewind; the `clear_rewind()` additionally wipes any *pending* rewind signalled by BUG2 retirement (`worker.rs:972-977`) or a resubscribe (`handlers.rs:557-559`). Result: a nacked or timed-out message is silently never redelivered until an unrelated rewind occurs. This is exactly the race the code itself documents as BUG3/M3 (`handlers.rs:549-556`, `shared.rs:216-227`) and fixes in `handle_subscribe`/`handle_bind` — these two sites were missed. Same class, lower risk: `DeliverPolicy::ByStartSeq` clears pending signals (`handlers.rs:582-585`). |
| P1 | `src/shard/drain.rs:394-450` + `src/shard/handlers.rs:557-559, 817` | **Shared per-shard cursor → cross-consumer redelivery storms.** There is no per-consumer delivery floor anywhere in the dispatch path (`dispatch_recipients`, drain.rs:631-852, checks only conn/queue-dedup/dead/unbound/write_failed/paused/capacity/subject-limit). Every rewind re-delivers the window to *all* matching consumers with capacity, including messages they already acked (plain ack does not tombstone). `handle_bind` even rewinds to 0 (handlers.rs:817), replaying the whole store to every existing consumer. Allowed by at-least-once, but unbounded duplication, and it is the trigger for the P0 drift above. |
| P2 | `src/shard/worker.rs:277-284` | **Busy-poll under sustained backpressure.** When a consumer is capacity-blocked or its writer channel full, `more_pending` keeps the gate open and the stalled drain loops with only a 50 µs sleep → ~20k cycles/s/shard of CPU (known open follow-up). Since ROB-23, the writer-full case is bounded to `drain_stall_evict_ms` per stalled conn (then the conn is evicted and the loop goes idle); the capacity-blocked case still polls until the ack arrives. Gate-wait conversion deferred — see action #7 for the missing writer→gate wake path. |
| P2 | `src/shard/drain.rs:905-922, 985-999` | Notify-ring overflow drops `Delivered` notifications (counted in `silent_drops` only). The engine never learns those seqs are pending → their later ack matches nothing (feeding P0-2's underflow) and no ack-timeout wheel entry is armed. |
| P2 | `src/shard/worker.rs:947-959` + `src/shard/drain_events.rs:31-36` | Drain-event ring overflow drops `Ack` decrements → permanent per-(consumer, subject) inflight leak in `ConsumerSubjects` → a subject-limited consumer eventually starves on that subject. (ConsumerRemoved is retried via `pending_consumer_remove`, worker.rs:521-537 — Ack is not.) |
| OK (verified) | `src/shard/worker.rs:177-189` | The 0.6.2 drain shutdown-liveness fix is present: `running` is checked at the top of the *inner* loop every cycle; `ShardRouter::shutdown` flips all flags and releases gates before joining (`src/shard/router.rs:504-532`). No sibling found: the command worker exits via `handle_or_shutdown` (worker.rs:852-881) and both select arms handle channel close. |
| OK (verified) | `arbitro-common/src/gate.rs:56-95` | Gate is lost-wake-safe (notified() built before flag check, release always notifies). Drain clears the gate before reading the store (worker.rs:193-215) and `drain_deliver` only re-opens (drain.rs:468-475) — the tail-message lost-wake is closed. |
| OK (verified) | `src/shard/drain.rs:397-410` + test at `drain.rs:1023-1086` | BUG1 (WriterGone must not advance cursor) fixed and unit-tested. |

### 2.3 Resource management

| Sev | Location | Finding / failure scenario |
|-----|----------|----------------------------|
| ~~P1~~ **FIXED (ROB-23)** | `src/shard/drain.rs` (`stall_evict_ms` in `drain_deliver` Phase 3) vs `src/transport/registry.rs:454-467` | **Asymmetric slow-consumer policy: delivery path never ejects.** ~~The reply path closes a connection whose outbound queue is full (ROB-22, registry.rs:460-466), but the drain treats a full writer channel as transient forever (`Backpressured` → retry next cycle). A live-but-not-reading consumer (keeps pinging, never drains) pins the shard cursor at its first undeliverable seq indefinitely → busy-poll (2.2) + rewalk-duplicates to healthy consumers each cycle. No backpressure-duration bound exists.~~ Fixed: the drain now keeps a per-connection stall clock (`DrainScratch.stalled_conns`); once every flush to a conn has come back `Backpressured` with zero `Ok` progress for `ARBITRO_DRAIN_STALL_EVICT_MS` (default 5000 ms, 0 = disabled), the conn is queued as `ConnectionDead` — same retirement path as `WriterGone` — and the cursor advances past it. Any `Ok` flush resets the clock, so a slow-but-alive consumer is never evicted. No message loss: retirement never touches the store; an evicted explicit-ack consumer resumes from its per-consumer ack floor on resubscribe, and its released pending redelivers via the BUG2 retirement rewind. Unit: `drain.rs backpressured_conn_evicted_after_stall_window`, `flush_progress_resets_stall_clock`. E2e (fail→pass captured): `drain_invariants.rs dead_reading_consumer_does_not_starve_healthy_sibling` (pre-fix: healthy sibling starved at 68/400). |
| ~~P2~~ **FIXED (ROB-24)** | `src/shard/handlers.rs` (`handle_delete_consumer`) + `src/shard/worker.rs` (`clear_consumer_nack_counts`) | ~~`dlq_nack_counts` entries are removed on ack and on threshold, but never on consumer delete → unbounded growth under nack-heavy churn when `max_nack > 0`.~~ Fixed: `handle_delete_consumer` now drops every `(consumer, seq)` counter of the deleted consumer (`clear_consumer_nack_counts`, cold path). Disconnect intentionally does NOT clear — a durable consumer survives its connection and its nack history must survive with it. Unit: `worker.rs delete_consumer_clears_its_dlq_nack_counts`. |
| P2 — **DEFERRED → cluster/raft workstream** | `src/cluster/state_machine.rs:72-74, 441` | `applied: Vec<ClusterCommand>` retains every committed metadata command forever; `snapshot()` (445-448) serializes the full history. Unbounded RAM + ever-growing snapshots on long-lived clusters. **Not fixed here on purpose:** a correct bound requires real snapshot/compaction (replace the retained prefix with a state snapshot and truncate the raft log accordingly) — a naive cap/ring would silently corrupt `restore()` on followers that install the snapshot. `TODO(cluster)` marker added at the field; belongs to the same feature project as action #8's catch-up/ISR work (see the arbitro-raft audit). |
| OK | `src/transport/registry.rs:210-232, 316-321` | SEC-6 per-connection create quotas enforced and cleaned up on remove. Writer-task error path removes the session (H6, registry.rs:487-501). Consumer slot exhaustion is guarded (B1, `dispatch_v2.rs:1637-1641`; e2e `catalog_invariants.rs:374`). Idempotency window clamped to 5 min (`src/shard/idempotency.rs:111`). Delayed journal bounded (`src/delayed.rs:31-36, 194-199`). |
| P3 / UNVERIFIED | `src/delayed.rs:97-121` | `delayed.log` is append-only with in-place `matured` marks; no compaction was found in the portion read — the file may grow without bound across long uptimes. Needs confirmation of `recover()`'s truncation behavior. |

### 2.4 Error handling & recovery

| Sev | Location | Finding / failure scenario |
|-----|----------|----------------------------|
| P1 | `arbitro-proto/src/metadata.rs:122` (zero call sites) vs `src/persistence/recovery.rs:346-359` | **Consumer ack cursors are never persisted.** `build_cursor_update` is dead code — nothing records `CMD_CURSOR_UPDATE`, so the replay arm for it can never run. `names.set_consumer_cursor` happens only in RAM (`handlers.rs:257-261`). After restart every cursor is 0 → a DeliverPolicy::All resubscribe replays the entire retained stream to a fully-acked consumer. In-process reconnect works (`t11_reconnect_resumes_unacked_tail`); across restart the feature silently does not exist. |
| P1 | `src/persistence/recovery.rs:201-233` (missing `set_stream_quota`) vs `src/transport/dispatch_v2.rs:1245-1249` | **DiscardPolicy::New quota lost on restart.** Replay restores idempotency window and replicas (recovery.rs:214-219) but never calls `set_stream_quota` → after restart the publish pre-check (`dispatch_v2.rs:330-344`) sees no quota and the stream silently flips from reject-new to truncate-oldest (data loss of head entries). |
| P1 | `src/persistence/recovery.rs:259-319` (missing `set_consumer_deliver_policy`) vs `src/transport/dispatch_v2.rs:1651-1653` | **deliver_policy/start_seq lost on restart.** `v2_subscribe` falls back to `(0, 0)` = All (`dispatch_v2.rs:1063-1066`) → a DeliverPolicy::New consumer becomes full-replay after restart (compounds the P1 above). |
| P2 (known) | `src/persistence/recovery.rs:316`, `src/cluster/state_machine.rs:346` | `max_nack: 0` hardcoded on both replay paths. Harmless *today* because the DLQ hotfix redelivers instead of dropping (`handlers.rs:337-356`), but it is config loss and must be fixed together with the real DLQ. |
| ~~P2~~ **FIXED (ROB-24)** | `src/transport/dispatch_v2.rs` (`v2_publish_delayed`) | ~~Delayed publish (delay > 0) bypasses both the idempotency check and the quota pre-check that the immediate path enforces.~~ Fixed: both checks now run before any journal/store mutation, on BOTH the delay>0 and the delay=0 fast path, with the exact semantics of the immediate path; a frame msg_id is additionally server-wrapped into ExtendedPayload + HAS_HEADERS (journal `flags` propagate to the store on maturation) so delivery is clean and post-maturation restart rebuild finds the id. E2e (fail→pass): `delayed.rs delayed_publish_duplicate_msg_id_is_deduped` (raw frame ×2 → one delivery + immediate-path duplicate rejected), `delayed_publish_respects_discard_new_quota` (both delay paths rejected with StreamFull). **Two documented semantic edges (same-as-immediate by design):** (1) the DiscardPolicy::New quota is evaluated against store occupancy at PUBLISH time — maturation does not re-check, so a delayed message admitted under quota can still mature into a store that filled up meanwhile; (2) the msg-id is recorded in the in-RAM tracker at publish time but is NOT rebuilt from the delayed journal — a restart BEFORE maturation forgets ids of still-pending delayed messages (after maturation they are rebuilt from the store like any publish). Closing either edge needs maturation-time policy, deliberately out of scope here. |
| OK (verified) | `src/shard/router.rs:191-213` | SEC-8 fix present: operator fsync policy is propagated into the message journal (`TolerantStore::set_fsync_policy`). |
| OK (verified) | `src/persistence/command_log.rs:104-174` | CRC-framed replay tolerates truncated tails and skips corrupt entries; e2e-covered (`persistence.rs:974, 1058`). Shard-count marker guard (M1, `src/server.rs:1351-1404`) prevents silent re-sharding. Idempotency rebuild windows correctly (`recovery.rs:379-475`). |

### 2.5 Cluster / replication (server-side integration)

| Sev | Location | Finding / failure scenario |
|-----|----------|----------------------------|
| P1 | `src/cluster/replication.rs:512-532` + `:659-746` (TODO, no caller sends catch-up) | **No working catch-up → one dropped batch desyncs a follower forever.** The leader drops batches on channel-full without retry (`handlers.rs:160-164` `try_send` comment) and on TCP error (replication.rs:397-406). The follower's BUG-4 guard then rejects every subsequent batch ("catch-up required"), but `KIND_REPLICATE_CATCH_UP_REQ` is never sent by anyone and `handle_catch_up_request`'s frames are never transmitted (replication.rs:733-736 TODO). ~~Worse: an **empty** follower accepts *any* `first_seq` (replication.rs:521 `expected = leader_first_seq` when `messages == 0`) and stores it at local seq 1 → silent seq divergence.~~ **Empty-follower half FIXED** (see action #8): the guard now requires `first_seq == last_seq + 1` unconditionally (`follower_expected_first_seq`), so an empty follower rejects gapped batches loudly instead of silently renumbering. The no-catch-up half remains (deferred to the cluster workstream). |
| P1 | `src/server.rs:669-687` + `src/cluster/replication.rs:582-602` | **ISR / high-watermark not enforced.** The ISR tracker is spawned with a fresh, never-fed instance (`record_ack` has no caller on the ack-receive path — the handler at replication.rs:594-601 is explicitly "informational"); `HighWatermarks` (replication.rs:169-193) is wired to nothing. Publishers get RepOk before any replication (`handlers.rs:122-131` reply precedes the fire-and-forget replication at 133-167), and consumers on the leader see unreplicated messages → leader kill loses acknowledged-to-publisher data even with replicas > 1. Cluster e2e (`arbitro-e2e/tests/cluster.rs:564`) exists; whether it truly pins this down is **UNVERIFIED — needs run**. **Update (action #8): now HONEST — best-effort status is warned at cluster startup and on `replicas > 1` stream creation, and documented in README; ISR/HW enforcement itself remains deferred to the cluster workstream.** |
| P2 | `src/server.rs:556-560` + `src/cluster/apply_loop.rs:12-15` | Leader double-apply by design (dispatch executes locally after propose AND the apply loop applies the committed entry). Idempotent creates make it mostly benign, but the two paths build *different* configs (see next row), so the winner depends on timing. |
| P2 | `src/cluster/state_machine.rs:110-128` (destructures `..`), `:348` | Replicated CreateStream drops `replicas`, `discard`/quota, and retention on followers (only idempotency window is set, state_machine.rs:213-216); replicated CreateConsumer drops subject limits (`Vec::new()`, :348). Follower state silently diverges from leader config. |
| P2 | `src/transport/dispatch_v2.rs:2297` vs `src/cluster/state_machine.rs:272-283` | CreateConsumer replicates the stream as `format!("{}", body.stream_id)` (a numeric wire-hash string) that the follower re-parses; DeleteConsumer replicates real names (BUG-7 fix). Two conventions for the same concept — fragile, and it breaks if the wire-id ever stops being the name hash. |
| P2 | `src/cluster/replication.rs:373-407` | Leader replication loop writes to peers sequentially with no write timeout — one hung follower TCP stalls replication to all peers (head-of-line blocking). |

### 2.6 Backpressure / DoS

| Sev | Location | Finding / failure scenario |
|-----|----------|----------------------------|
| ~~P1~~ **FIXED (ROB-23)** — eviction; busy-poll now BOUNDED | (same as 2.3-P1) `src/shard/drain.rs` + `src/shard/worker.rs:277-284` | A single slow/dead-reading consumer stalls its shard's cursor and burns CPU; ~~no eviction, no bound~~. Eviction landed (see 2.3); the cursor-pin DoS is closed. The 50 µs stall-sleep busy-poll (2.2 P2) remains, but is now bounded to at most `drain_stall_evict_ms` per stalled connection instead of forever. |
| P2 | `src/config.rs:112-113` + `src/server.rs:1032-1033, 1188-1208` | Defaults: `max_frame_size` = 64 MiB, `max_ops_per_sec` = 0 (unlimited), auth optional. The read loop buffers a full frame per connection before dispatch → worst case 10k conns × 64 MiB of attacker-controlled buffering. Frame-count rate limiting (ROB-3) does not bound bytes. |
| OK | `src/server.rs:1053, 1098-1115, 767-771` | Handshake deadline (ROB-1), auth-frame 4 KiB cap (SEC-1), max_connections gate, TLS handshake off the accept loop with timeout (SEC-2), accept-error backoff (ROB-2), constant-time token compare (M14). Cluster transport has its own inbound limits (D3, `src/server.rs:526-535`). |

---

## 3. Part B — Invariant coverage map

Status: **TESTED** (e2e/integration proves it), **PARTIAL** (some cases proven, load-bearing case missing), **GAP** (untested — proposed test given).
Test paths are relative to `crates/arbitro-e2e/tests/` unless noted.

| # | Invariant | Status | Evidence / proposed test |
|---|-----------|--------|--------------------------|
| D1 | At-least-once: published message reaches a subscribed consumer | TESTED | `invariants.rs:125 publish_single_delivers_correctly`, `integration.rs:62 test_publish_ack_cycle` |
| D2 | No loss under backpressure (slow consumer, small max_inflight) | TESTED | `drain_invariants.rs:863 slow_consumer_fast_publisher_is_lossless`, `drain_invariants.rs:1235 t13_single_shard_saturation_no_silent_drops` |
| D3 | No loss across drain-cycle boundaries / concurrent publishers | TESTED | `drain_invariants.rs:1062 concurrent_publishers_one_consumer_exactly_n` (asserts exactly-N, no dups for the single-consumer case) |
| D4 | No loss across restart (disk store) | TESTED | `persistence.rs:214 messages_survive_restart_with_disk_store`, `persistence.rs:607`, `shutdown.rs:106 acked_messages_survive_shutdown`, `shutdown.rs:368` |
| D5 | **No duplicate delivery to consumer A when consumer B rewinds the shard cursor** (B nacks, B subscribes late with DeliverPolicy::All, or B binds) | **GAP — and statically the invariant is VIOLATED** | No test subscribes/nacks a second consumer *after* the first has consumed+acked. Code: rewind at `handlers.rs:449-456 / 557-559 / 817` + no per-consumer floor in `drain.rs:631-852` ⇒ A gets re-delivered acked messages. Proposed: `late_subscriber_does_not_redeliver_to_acked_peer` — A consumes+acks 10 msgs; B creates+subscribes (policy All); assert A's handle stays silent while B receives 10. Rank: **highest risk**. |
| A1 | Ack prevents redelivery (same consumer) | TESTED | `invariants.rs:302 ack_prevents_redelivery` |
| A2 | Nack causes redelivery | TESTED | `invariants.rs:340 nack_causes_redelivery`, `workflow_streams.rs:77 workflow_step_retry_on_nack` |
| A3 | Nack of one message does not disturb other pending messages of the same consumer (no dup, no counter drift) | **GAP** | Statically violated (P0-1: `drain.rs:413-419`). Proposed: `partial_nack_does_not_duplicate_or_leak_inflight` — deliver 10, ack 4, nack seq 5, assert exactly seq 5 redelivered and that after acking everything the consumer can still receive `max_inflight` new msgs. Rank: **2**. |
| A4 | Unknown/double ack is safe (rejected or no-op; consumer keeps working) | **GAP** | Statically violated (P0-2: `handlers.rs:264-275`, underflow at `shared.rs:104-114`). Proposed: `double_ack_and_bogus_ack_do_not_wedge_consumer` — ack the same seq twice + ack seq 999999, then publish N more and assert all N delivered. Rank: **3**. |
| A5 | Ack-timeout (ack_wait_ms) redelivers unacked messages | TESTED | `drain_invariants.rs:120 ack_wait_timeout_redelivers`, `workflow_streams.rs:391 workflow_step_timeout_redelivers` |
| A6 | Cursor resume on reconnect (same process) | TESTED | `client_ack_invariants.rs:421 t11_reconnect_resumes_unacked_tail`, `drain_invariants.rs:1161 resubscribe_continues_from_cursor` |
| A7 | Cursor resume across broker **restart** | **GAP** | Impossible today — cursors never persisted (`build_cursor_update` dead, `arbitro-proto/src/metadata.rs:122`). Proposed: `acked_cursor_survives_restart` — consume+ack all, restart broker (disk), resubscribe policy-All, assert zero redeliveries. Rank: **4**. |
| I1 | Msg-id dedup within window (frame field) | TESTED | `idempotency_invariants.rs:54, 107, 135` |
| I2 | Dedup across restart | TESTED | `idempotency_invariants.rs:331 cross_restart_dedup_survives`, `:531` |
| I3 | Dedup when msg-id is carried in headers (single publish) | TESTED | `idempotency_invariants.rs:743 publish_with_headers_dedup`, delivery strips metadata `:472` |
| I4 | Dedup + clean payload for **batch** publish with headers | ~~GAP~~ **TESTED (ROB-24)** | Fixed (storage flags now mirror single publish) and pinned by `idempotency_invariants.rs batch_publish_with_headers_delivers_clean_payload_and_dedups_after_restart` — raw BatchPubFrame with `entry_flags = HAS_HEADERS` (the reference client cannot send one), asserts TLV-free delivered payloads, same-session dedup, and post-restart duplicate rejection. |
| I5 | Batch dedup is all-or-nothing (rollback on internal duplicate) | TESTED | `idempotency_invariants.rs:296 batch_with_internal_duplicate_is_rejected`, `:249` |
| O1 | Per-stream delivery order (single consumer, acking) | TESTED | `invariants.rs:389 delivery_preserves_order` (monotonic seq), `invariants.rs:206 publish_sequences_monotonic` |
| O2 | Order preserved across redelivery/rewind | PARTIAL — **deliberately left untested (ROB-24)** | Only the monotonic-happy-path is asserted; redelivered messages re-enter in store order by construction (`drain_read` walk, `drain.rs:233-255`) but no test pins ordering after a mid-stream nack. Low risk. A meaningful pin (max_inflight=1, nack-then-continue sequence assertions) exceeded the agreed complexity budget for this pass; add alongside the A3/D5 temporal-isolation tests when the per-consumer floor work (action #4) gets its suite. |
| U1 | Fsync policy actually applied to the message journal; kill -9 durability | **GAP / UNVERIFIED — deferred, harness missing (ROB-24)** | Fix verified in code (`router.rs:191-213`, SEC-8) but no e2e kills the process non-gracefully under `ARBITRO_FSYNC_POLICY=every` and asserts the tail batch survives. All restart tests use graceful shutdown. **Why deferred:** the entire e2e harness runs the broker in-process (`test_helper.rs` `tokio::spawn(server.run_with_shutdown(..))`), so there is no child PID to SIGKILL — killing the process kills the test. A real U1 needs a child-process harness: a small broker binary (or `cargo run -p arbitro-server` wrapper) spawned via `std::process::Command`, readiness-probed over TCP, SIGKILLed (the `nix` dev-dependency is already present), then restarted in-process for the assertion. That is new infrastructure, not a test — build it as its own task. Proposed test name unchanged: `fsync_every_survives_process_kill`. Rank: **5**. |
| U2 | Last batch durable on graceful shutdown | TESTED | `shutdown.rs:106 acked_messages_survive_shutdown`, `shutdown.rs:319 shutdown_mid_publish_metadata_survives` |
| **C1** | Distinct durable names ⇒ distinct consumer IDs (namespacing) | TESTED | `catalog_invariants.rs:334 distinct_names_have_distinct_ids`; stream-scoped namespacing `catalog_invariants.rs:158` |
| **C2** | **Different durable names, fanout ⇒ each consumer independently receives its own full copy (NOT shared/stolen)** | **TESTED (end-to-end, delivery-level — not just ID-namespacing)** | `invariants.rs:440 fanout_two_consumers_each_receive_all` (same conn, both get all 5, acked), `invariants.rs:1113 fanout_multi_client` (separate conns), `drain_invariants.rs:1442 triple_fanout_two_explicit_one_none_all_receive`, filters variant `invariants.rs:579`. Client demux side: `arbitro-client-tokio/tests/deliver_demux.rs:146` (independent streams). **Caveat:** all of these subscribe *before* publishing — the temporal-isolation case is D5 (GAP). So: copies-not-shared is PROVEN; independent-*cursor* semantics is NOT (there is no per-consumer drain cursor at all — isolation degrades to "at-least-once with cross-consumer duplicates" the moment any rewind happens). |
| **C3** | Same queue group (shared) ⇒ each message delivered to exactly one member, no duplication | TESTED | `invariants.rs:501 queue_group_distributes_messages` (total == N), `invariants.rs:1004 queue_group_multi_client` (asserts all seqs UNIQUE — no dups), `invariants.rs:1186 queue_group_three_clients_100_msgs`; no false dedup across filters `invariants.rs:717, 816`; in-drain rotation is seq-based round-robin (`drain.rs:643-667`). Load-*balance* (each member gets > 0) is not asserted — only totals/uniqueness. |
| C4 | Same durable NAME subscribed from two connections — defined semantics (shared consumer identity) | **GAP — OPEN PRODUCT DECISION (ROB-24), do not test-pin until decided** | `get_or_create_consumer` dedups by name (`dispatch_v2.rs:1636`) so both connections bind the same ConsumerId; the drain delivers per match-entry while both bindings share one inflight budget (`drain.rs:665-721`), so today's emergent behavior is "double-deliver, shared budget" — an accident of implementation, not a chosen contract. **The two coherent options:** (a) **queue-share** (NATS-durable-style): the two connections are implicit members of one work queue — each message goes to exactly one binding, shared ack floor, natural client-side failover; (b) **explicit rejection**: second `subscribe` of an already-bound durable name returns an error (e.g. `ConsumerAlreadyExists`), forcing distinct names or an explicit queue group — simplest contract, no hidden sharing. Double-delivery-as-contract is NOT a candidate (it duplicates work while corrupting per-consumer accounting). Writing the e2e now would freeze the accidental behavior; decide (a) vs (b) first, then pin with `same_durable_name_two_connections_semantics`. Rank: 8. |
| C5 | Fanout vs queue-group vs durable-name are distinct and enforced | TESTED (via coercion) | Fanout forces QueueId(0) (GAP-5, `dispatch_v2.rs:1616-1648`; recovery mirrors it `recovery.rs:277-282`); queue mode via group (`invariants.rs:501`); AckPolicy::None coercions below. |
| F1 | Stale-config rejection (CreateConsumer with different config ⇒ error, not silent merge) | ~~GAP~~ **TESTED (ROB-24)** | `catalog_invariants.rs create_consumer_config_mismatch_rejected` — create max_inflight 10, re-create same name with 20 → `InvalidConsumerConfig`; idempotent re-create with 10 returns the same id; behavioral proof that the ORIGINAL cap is in force (15 published, exactly 10 unacked deliveries arrive). Caveat noted below: dispatch mutates NameRegistry deliver-policy/queue metadata BEFORE the engine's config check, so a mismatched re-create with a different deliver_policy silently overwrites registry state even though rejected — see the new note under action #10. |
| F2 | AckPolicy::None ⇒ max_inflight ignored (coerced unlimited) | TESTED | `drain_invariants.rs:610 ack_policy_none_ignores_max_inflight`, enforcement of Explicit `:667` |
| F3 | AckPolicy::None ⇒ subject limits dropped | TESTED | `drain_invariants.rs:723 ack_policy_none_ignores_max_subject_inflight` (code: B6, `dispatch_v2.rs:1627-1634`) |
| F4 | Recycled stream_id does not leak old entries (created_at_seq) | TESTED | `persistence.rs:1117 created_at_seq_filters_old_entries_after_recycle`, `lifecycle_flow.rs:82 t12` |
| F5 | Hash-collision stream names rejected, slot exhaustion safe | TESTED | code M7 (`dispatch_v2.rs:1195-1212`); slots `catalog_invariants.rs:374 create_4097_consumers_does_not_panic` |

**Consumer-isolation verdict (USER-PRIORITY):** the *"different durable name = independent full
delivery, NOT shared"* invariant **is proven end-to-end** at the delivery level (C2 row — real
messages, real acks, same and separate connections), not merely ID-namespacing. The precise GAP is
**temporal**: nothing proves (and the code contradicts) that one consumer's cursor-rewinding action
(late subscribe with DeliverPolicy::All, bind, nack, timeout) leaves an already-acked sibling
undisturbed — because there is one shared drain cursor per shard and no per-consumer delivered/acked
floor in the dispatch path. D5/A3 are the tests that close it; both are expected to FAIL today,
which is the point.

---

## 4. Top-10 prioritized actions

1. **Reconcile inflight accounting with engine truth (P0).** Either dedup drain-side increments
   (skip `counters.inc_inflight` for seqs already pending — requires pending visibility, e.g. a
   `Delivered` echo of engine `matched`), or drive both counters from one owner. Sites:
   `src/shard/drain.rs:413-419`, `arbitro-engine/src/runtime/execute.rs:49-67`.
2. **Clamp and truth-source the decrements (P0).** Use the engine's `matched` count (plumb it out of
   `Command::Ack/Nack`) instead of `entries.len()` at `src/shard/handlers.rs:264-275, 390-398,
   435-443`, and make `dec_inflight_bulk` saturating (`src/shard/shared.rs:104-114`). Add e2e
   `double_ack_and_bogus_ack_do_not_wedge_consumer`.
3. **Fix the rewind protocol in `handle_nack` and `wheel_tick` (P1).** Replace
   `set_cursor + clear_rewind` with the `set_cursor + signal_rewind` protocol already used by
   `handle_subscribe`/`handle_bind`. Sites: `src/shard/handlers.rs:449-456`,
   `src/shard/worker.rs:808-816`.
4. **Introduce a per-consumer delivery floor (P1)** so rewinds only redeliver to the consumer(s)
   that need them (`consumer_cursor` already exists in NameRegistry — stamp it into the snapshot
   and filter in `dispatch_recipients`, `src/shard/drain.rs:631-852`). Then add e2e
   `late_subscriber_does_not_redeliver_to_acked_peer` (closes D5/A3).
5. **Persist consumer cursors (P1).** Record `build_cursor_update` (batched/periodic, cold path) on
   ack; the replay arm already exists (`src/persistence/recovery.rs:346-359`). Add e2e
   `acked_cursor_survives_restart`.
6. **Complete recovery config restore (P1).** Add `set_stream_quota` and
   `set_consumer_deliver_policy` to replay (`src/persistence/recovery.rs:201-233, 259-319`),
   mirroring `dispatch_v2.rs:1245-1249, 1651-1653`; un-hardcode `max_nack` when DLQ lands.
7. **Bound slow-consumer backpressure on the delivery path (P1).** **DONE (ROB-23, part 1):**
   after `drain_stall_evict_ms` (default 5000 ms, `ARBITRO_DRAIN_STALL_EVICT_MS`, 0 = off) of
   continuous zero-progress `Backpressured` flushes for the same conn, the drain marks it dead
   like WriterGone (`src/shard/drain.rs`, Phase 3 of `drain_deliver`). E2e fail→pass:
   `drain_invariants.rs dead_reading_consumer_does_not_starve_healthy_sibling`.
   **DEFERRED (part 2 — 50 µs stall-sleep → Gate wait / backoff, `src/shard/worker.rs:277-284`):**
   converting the stall to a `gate.acquire()` wait requires EVERY unblock condition to signal the
   Gate. Three of four are covered today (ack frees capacity → `apply_delta_and_sync` releases;
   new publish → dispatch releases; shutdown → `handle_or_shutdown` releases). The fourth is NOT:
   **a writer channel draining (the per-connection `conn_writer_task` in
   `src/transport/registry.rs` popping frames / unblocking from `write_all`) has no signal path
   back to any shard Gate** — the transport layer holds no shard gate reference, and a conn's
   consumers may span shards. Parking the drain on the Gate while writer-full would therefore be
   a lost-wake hang until an unrelated publish/ack. Given this shard's history of a
   shutdown-liveness deadlock (0.6.2), the busy-poll is left untouched; ROB-23 bounds it to at
   most `drain_stall_evict_ms` per stalled conn. Proper fix needs writer→gate wake plumbing
   (e.g. a per-shard waker registered in `WriterIndexEntry`) designed and proven separately.
8. **Cluster: make replication self-healing or honest (P1).**
   **PARTIALLY DONE (honest half): empty-follower silent-divergence FIXED + best-effort now
   logged/documented.** The follower guard no longer special-cases `messages == 0`
   (`replication.rs`, `follower_expected_first_seq`): an empty follower accepts only
   `first_seq == local last_seq + 1` (seq 1 for a fresh store, the true continuation for a
   store emptied by retention) and rejects any gapped batch with the same "catch-up required"
   warn as the non-empty BUG-4 case — divergence is now loud, never silent renumbering.
   Unit-pinned by `empty_follower_rejects_gap_first_seq` (a real 2-node divergence e2e is
   impractical: no batch-drop injection hook, no late-join membership). Best-effort status is
   now stated at cluster startup (`server.rs` warn after "data-plane replication tasks
   started"), at stream creation when `replicas > 1` (`dispatch_v2.rs` create-stream path),
   at the RepOk/fire-and-forget site (`shard/handlers.rs` comment), and in `README.md`
   ("Message Replication" limitation note).
   **DEFERRED (feature half → arbitro-raft / cluster workstream):** catch-up send is still
   unwired — `KIND_REPLICATE_CATCH_UP_REQ` is never sent by anyone and
   `handle_catch_up_request`'s frames are never transmitted (`TODO(cluster)` markers in
   `replication.rs`), so a dropped batch still leaves a follower behind until rebuilt; ISR
   `record_ack` has no caller and `HighWatermarks` gates nothing, so publisher acks precede
   replication and consumer visibility ignores the HW. Wiring catch-up + ISR/HW is a feature
   project overlapping the arbitro-raft audit (see `AUDIT_REPORT.md` in the `arbitro-raft`
   crate/workstream) and is intentionally NOT attempted in this audit-fix pass.
9. **Fix batch-publish HAS_HEADERS storage flags (P2). DONE (ROB-24).**
   Batch entries now carry `arbitro_store::flags::HAS_HEADERS` when the frame does, and
   dedicated-field batch msg_ids are server-wrapped into ExtendedPayload exactly like single
   publish (`src/transport/dispatch_v2.rs`, `v2_publish_batch`) — clean delivery + restart dedup
   for both id conventions. Delayed publish (`v2_publish_delayed`) now enforces the idempotency
   window AND the DiscardPolicy::New quota pre-check on both its delay>0 and delay=0 paths, and
   wraps frame msg_ids the same way (journal `flags` propagate to the store at maturation).
   E2e (fail→pass): `batch_publish_with_headers_delivers_clean_payload_and_dedups_after_restart`,
   `delayed_publish_duplicate_msg_id_is_deduped`, `delayed_publish_respects_discard_new_quota`.
   Two same-as-immediate semantic edges documented in 2.4 (quota checked at publish time, not
   re-checked at maturation; pending-delayed msg-ids not rebuilt across a pre-maturation restart).
10. **Small hardening batch (P2). PARTIALLY DONE (ROB-24) — item-by-item:**
    - **DONE — command-log replay cap:** `MAX_REPLAY_ENTRY_LEN` (1 MiB) in
      `src/persistence/command_log.rs`; over-cap length = corrupt tail → replay stops (like the
      truncated-tail path); `record()` symmetrically refuses over-cap commands. Units:
      `over_cap_entry_length_stops_replay_without_alloc`, `record_rejects_over_cap_command`.
    - **DONE — `dlq_nack_counts` cleanup:** `handle_delete_consumer` calls
      `clear_consumer_nack_counts` (`src/shard/worker.rs`). Unit:
      `delete_consumer_clears_its_dlq_nack_counts`.
    - **DONE — `out.clear()` on `read_entries` error:** `src/cluster/storage.rs`; unit
      `read_entries_error_path_clears_partial_fill` (built and run with `--features cluster`).
    - **DONE — config-mismatch e2e (F1):** `catalog_invariants.rs
      create_consumer_config_mismatch_rejected`.
    - **DEFERRED — bound `ArbitroStateMachine::applied`:** needs real snapshot/compaction; a naive
      cap corrupts `restore()`. `TODO(cluster)` at the field; goes with action #8's cluster
      workstream (see 2.3).
    - **DEFERRED — fsync-every kill -9 e2e (U1):** the e2e harness runs the broker in-process, so
      there is no child PID to SIGKILL; needs a child-process harness first (see Part B row U1 for
      the concrete design).
    - **DEFERRED — `src/cluster/transport.rs:820-822` transmute (2.1 P1):** not locally
      enforceable from arbitro-server; the lifetime contract must be fixed at the RaftNode API
      boundary. Tracked in the arbitro-raft audit (`AUDIT_REPORT.md`, P0 soundness family) — the
      server-side site should be re-audited once that lands.
    - **NEW (noted during F1 work, unfixed):** `v2_create_consumer` writes
      `set_consumer_queue` / `set_consumer_stream` / `set_consumer_deliver_policy` into the
      NameRegistry BEFORE the engine's config-mismatch check (`src/transport/dispatch_v2.rs`,
      between `get_or_create_consumer` and `shard.create_consumer`). A rejected re-create with a
      different deliver_policy/start_seq therefore silently overwrites the registry's copy while
      the engine keeps the original — after a restart, replay/subscribe reads the registry value.
      Move the registry writes after the `Ok(1)`/`Ok(0)` arms (or restore them on `Ok(2)`).

---

*Method note: this audit is static-first (all findings are code-cited); no test run was performed
in this pass. Rows marked UNVERIFIED are exactly the ones where a run would change confidence, and
the proposed tests in Part B are ordered so the two P0s are falsifiable by the first three tests.*
