# The shard owns its domains

Reference for the migration on `refactor/shard-owns-its-domains`. Everything
here was either measured, found by audit, or learned by getting it wrong
first. Numbers are from this repo, not from the literature.

---

## 1. The one rule

**When something has an owner, nobody asks permission.**

Every mechanism this document removes exists because a piece of state had
no clear owner, so the code either asked whether it was allowed to touch it,
or kept a second copy so it would not have to ask.

### There are no thread constraints here

A shard is one thread. The drain, the command worker and every connection on
that shard are **tasks on one `current_thread` runtime**. They cannot run at
the same time. There is nothing to synchronise, no race to reason about, no
`Send` to satisfy between them.

This is settled and must not be re-derived. `router.rs` already says it:

> Same runtime as this shard's drain — that pairing IS the change. Two tasks
> on one thread cannot contend for the store.

Any comment in this tree that justifies a mechanism with threads is **stale
documentation describing a thread that no longer exists**, and should be
treated as a defect. `drain_events.rs` still says "The drain OS thread
consumes them". There is no such thread.

**So when X cannot reach Y, the cause is ALWAYS ownership or borrowing.**
The drain could not write `binding.pending` because `engine` is a field of
`CommandWorker` and that struct belongs to another task — not because of
threads. The fix is a borrow route, never a channel, a lock or a copy.

---

## 2. The structure

```rust
pub struct Shard {
    id: u16,
    listener: TcpListener,      // its port — what arrives here belongs to it

    engine: Engine,             // pending, bindings, consumers, inflight, cursor
    store: Box<dyn Store>,      // the journal
    dedup: Dedup,               // msg_id -> ticket
    scheduler: Scheduler,       // ack_wait, nack_delay, dedup expiry, eviction
    egress: Egress,             // its connections' sockets
}
```

Fields. No `Arc`, no `Mutex`, no `RefCell`, no thread-local, and **no
`shard_id` in any signature** — there is nobody to prove ownership to.

### Domains

A domain owns a job, declares its scheduler namespace, and is handed exactly
the state it needs at the moment it acts:

```rust
struct Redelivery {
    ns: Ns<AckTimeout>,
    due: Vec<AckTimeout>,          // its own buffer, reused
}

impl Redelivery {
    fn new(sched: &mut Scheduler) -> Self {
        Self { ns: sched.namespace::<AckTimeout>("redelivery"), due: Vec::new() }
    }

    fn armed(&self, sched: &mut Scheduler, b: BindingId, seq: u64, at_ms: u64) -> Ticket {
        sched.queue(self.ns, AckTimeout { binding: b, seq }, at_ms).ticket().unwrap()
    }

    fn acked(&self, sched: &mut Scheduler, t: Ticket) {
        sched.cancel(t);
    }

    fn run(&mut self, sched: &mut Scheduler, engine: &mut Engine) {
        sched.take(self.ns, &mut self.due);
        for e in &self.due {
            engine.redeliver(e.binding, e.seq);
        }
    }
}
```

And the shard only orchestrates:

```rust
self.scheduler.tick(now);
self.redelivery.run(&mut self.scheduler, &mut self.engine);
self.dedup.run(&mut self.scheduler);
self.eviction.run(&mut self.scheduler, &mut self.engine);
```

Three properties matter here:

- **The namespace belongs to the domain.** `Redelivery` is the only code
  that knows what an `AckTimeout` is; the scheduler never sees the type.
- **No stored closure.** `run` receives `&mut Engine` at the moment it acts,
  so it can touch what it needs. A registered callback could not, and would
  cost a vtable call per entry (measured: 6.58 ns vs 1.29 ns direct).
- **The scheduler belongs to nobody in particular.** The shard owns it and
  lends it. That is why `Ns<T>` is `Copy` and borrows nothing.

### One fact, one owner

`SharedCounters` mirrors what the engine already has: inflight, cursor,
demand. Every bug in that file is a mirror bug. Audit #A4 in the tree
documents one: decrementing by `entries.len()` instead of by what actually
matched drove a `u32` below zero and wedged the consumer permanently.

**The engine is the owner.** It puts entries into `pending` and takes them
out, so it is the only thing that can know the inflight. The mirror exists
so the drain can read without asking the engine — and the drain does not
need to ask, because it is a field of the same struct.

---

## 3. Publish, end to end

### What it is today

```
conn task  →  v2_publish
              names().snapshot()            arc_swap guard, 10.22 ns
              stream_idempotency_window_ms
              dedup check (local door only)
              stream_quota → store_stats().await   ← an await PER MESSAGE
              header/ExtendedPayload resolution
              server.append(...)
                 owns(idx)?  ── no ──→  owned_entries()  Vec<PublishEntryOwned>
                                        mpsc → shard worker
                                        handle_publish
                                          dedup_routed()   ← the check, again
                                          with_store(shard_id)  ← ask permission
                                          append_batch
                                          reply from HERE
```

### What it should be

```rust
// ingress.rs, on the frame that just arrived
let seq = shard.publish(stream_id, entries, now)?;
reply_ok(conn, req_seq, seq);
```

```rust
impl Shard {
    fn publish(&mut self, stream: StreamId, entries: &[EntryRef], now: u64)
        -> Result<u64, Refused>
    {
        self.dedup.admit(stream, entries)?;
        let seq = self.store.append_batch(entries, now)?;
        self.drain.wake();
        Ok(seq)
    }
}
```

What disappears, and why each was only ever the price of crossing:

| Gone | Why it existed |
|---|---|
| `PublishCmd` | there is no command; it is a call |
| `PublishReply` (3 variants) | publisher and replier were different owners |
| `PublishEntryOwned` | entries had to be owned to cross |
| `dedup_routed` | a second implementation of one rule |
| `LocalSink::new(shard_id, ..)` | building a handle and proving ownership |
| the double 3-arm `match` | a `Result` and `?` |

**Zero copies survive, and that is already true today.** `v2_publish`
contains no `copy_from_slice`, no `to_vec`, no `clone`. `share()` uses
`Bytes::slice_ref` — a refcount over the same buffer. The only copying
branch is the msg-id injection, which rebuilds a payload. The frame arrives
zero-copy and reaches the store without anyone duplicating a byte. What
remains is **allocation of structure**, not of data, and it exists only to
cross.

### The idempotency window, and why it lives in three places

One tracker per stream, in a per-shard map, in a thread-local. Expiry is a
timing wheel ticked by the command worker. That part is right.

The check is written **six times for one rule**:

| Site | What |
|---|---|
| `dispatch_v2.rs:341` | `v2_publish` — local |
| `dispatch_v2.rs:516` | `v2_publish_with_reply` — local |
| `dispatch_v2.rs:712` | `v2_publish_batch` — local |
| `handlers.rs:165` | `dedup_routed` — routed |
| `handlers.rs:194` | `handle_record_dedup` — delayed |
| `router.rs:828` | `record_dedup`, local branch |

Plus `handlers.rs:257`, `rebuild_idempotency`, which is a different job
(rehydrating from the journal at startup) and stays.

Two causes, and only one is real:

- **The ingress thread may not own the stream.** Not a cause — the defect.
  It is the case to delete, not to serve.
- **A delayed publish is admitted without being appended.** Real: it goes to
  the delayed journal, so there is no append to carry the check.

`local::idempotency(shard_id)` is an ownership question — "is this thread's
map the one for shard N?" — that only a non-owner needs to ask. The command
worker pays it per publish anyway: thread-local access, `RefCell`, an id
compare, an `Rc` bump, then another `RefCell` + hash + `Rc` bump to reach
the tracker. **Six steps and two refcounts to reach a map the caller owns.**

`with_store(shard_id, ..)` is the same question about the journal.

> Blocker found while trying this: the worker cannot hold the map as a
> field, because `rt.spawn(cmd_worker.run())` requires `Send` and the map is
> an `Rc`. Removing the per-publish lookup needs `spawn_local` on a
> `LocalSet`, which changes how the shard runtime starts. Nothing in the
> server uses `LocalSet` today.

---

## 4. Ack, in one pass

`handle_ack` walked the entries **seven times**. The engine's walk is the
only one that does work; everything else re-derived what it already knew.

Now: the engine walks, and the floor, the DLQ counters and the persisted
cursor derive from what it MATCHED, inside the walk `apply_delta_and_sync`
was already making.

**Deriving beats re-deriving on correctness, not only cost.**
`ack_floor.rs` states its rule as "record ONLY seqs that matched a pending
delivery". The delta IS that set, so the rule holds by construction instead
of by a second walk reproducing the engine's criterion and staying in step
with it. Same for the persisted cursor: from the matched high-water mark,
not the highest seq the client submitted — which used to let a bogus ack
push the resume point past messages the consumer still owed.

The ideal remains simpler than what is there now:

```rust
impl Shard {
    fn ack(&mut self, conn: ConnectionId, consumer: ConsumerId, entries: &[AckEntry])
        -> Released
    {
        self.engine.ack(conn, consumer, entries)
    }
}
```

The engine has `pending`, the inflight, the cursor and the bindings. Given
an ack it knows what to do; nobody has to tell it anything. It returns the
result so the caller can answer the client, and emits the events it caused.
Whoever needs to know, listens.

**And removal IS release.** If capacity is `pending.len()`, taking the entry
out frees the slot, stops the redelivery and updates the inflight in one
operation. There is nothing to announce. The only work that genuinely
remains is durability.

### Tenant isolation belongs in the walk

`connection_owns` walked the entries doing one hash each — 256 per
`BatchAck` — repeating the lookup the engine performs while acking. That is
the "multiple validations of the same frame" ban in the engine's own
`.agent/rules`.

It now happens inside that walk: the named arm is keyed by (connection,
subscription) and also checks the binding's consumer; the unnamed arm
compares the binding's `connection_id`. Finer (per binding, not per
consumer) and an integer compare on data already loaded.

The named arm's consumer check is load-bearing: a frame may name a
subscription the connection really owns while declaring a DIFFERENT
consumer in its header, and crediting that consumer corrupts a third
party's inflight.

The unnamed arm is NOT dead code. The broker generates unnamed entries for
itself: an `ack_wait` expiry has no client frame to take a `sub_id` from.
That path used to pass `ConnectionId(0)` — a sentinel `command.rs`
criticises elsewhere — and now passes the owning binding's connection,
recovered from the same scan that was already running.

---

## 5. Delivery, and the window that used to exist

Six mechanisms existed because the drain could not write `pending`:

| Existed | Only because |
|---|---|
| the notification ring | the drain could not write `pending` |
| `drain_notifications` | somebody had to collect from it |
| `notifications_settled` | somebody had to know if anything was owed |
| the `dup_hashes` pre-scan | the drain incremented BLINDLY, then undid it |
| deferred `wheel_insert_delivered` | the timer was armed late |
| the ack paying an unbounded bill | it could not read stale state |

**The window.** The drain wrote the frame to the socket and announced the
delivery afterwards. A client can ack over loopback faster than the local
task hop that records it. An ack arriving first matched nothing, was a
silent no-op, and the message came back on `ack_wait` — a duplicate the
client had already processed.

`drain_notifications` at the top of `handle_ack` was not a feature. It was a
defence: "catch up before you look, in case you look at state that does not
exist yet."

**The fix removes the window instead of defending it.** The drain registers
the pending in the same cycle that puts the frame on the wire, so the record
exists before the frame does. And `register_delivered` filters the entries
in place to what it actually registered, so the drain counts those and only
those — no blind increment, nothing to reverse.

### The loss path this closed

On a full ring the drain **dropped** the notification
(`silent_drops.inc_notify_ring`) after the frame had flushed and the
counters had already moved. The engine never registered the pending: the ack
matched nothing, the inflight credit leaked, and the seq sat in the
suppression set with no release path. That is the "permanent starvation"
shape this codebase's own comments call strictly worse than any transient
stall, and a plausible fingerprint for the open `missing=N` tail bug.

### The wake that had to be added

Arming a deadline while the command worker is parked updates
`next_timer_ms` but not the `sleep` it is already awaiting. `rearm_timer`
now notifies, **and only when the deadline moved earlier**. A wake, not a
queue: no data, no ownership, no order. `ack_wait_timeout_redelivers` is the
test that catches its absence — twice, in this session.

---

## 6. The scheduler

One wheel per shard, many domains. Shipped in `arbitro-kit` as
`kit::scheduler` (commit `d81fc2f`).

```rust
sched.namespace::<T>(name) -> Ns<T>     // declare once, keep the token
sched.queue(ns, job, at_ms) -> Queued<T>
sched.unqueue(ns, ticket)   -> Option<T>   // cancel AND get the job back
sched.cancel(ticket)        -> bool
sched.take(ns, &mut buf)                   // your namespace, your type

sched.next_wake_ms() -> Option<u64>        // the wheel decides
sched.tick(now_ms)                         // and sorts what is due
```

`next_wake_ms` and `tick` are the whole driver contract. **Neither blocks** —
one is a read, the other is computation — which is what makes it safe beside
a `current_thread` runtime carrying the rest of a shard.

### Why the payload belongs to the domain

A shared entry struct is the union of what every user needs, and then
nobody's identity is right. `WheelEntry` stored `consumer_id` because it
shared a struct with nack-delay, so an ack-timeout expiry could not name the
binding that owed the message and had to walk every binding of the consumer
probing `is_pending`. `Ns<T>` gives each domain its own `T` and that search
stops existing.

*(Fixed in the current tree by adding `binding_id` to `WheelEntry` — it fits
in padding the tag was already wasting, so still 24 bytes.)*

### Measured

Precision is `[deadline, deadline + tick_ms]` on both sides, at every
horizon from 150 ms to a day, cascading included. Real clock, real sleeping:

| asked | delivered (tick=10ms) | delivered (tick=100ms) |
|---|---|---|
| 37 ms | 40.10 ms | 100.22 ms |
| 137 ms | 140.32 ms | 200.29 ms |
| 1000 ms | 1010.59 ms | 1100.61 ms |

The OS contributes under a millisecond; the tick is the whole error. Worst
case is always exactly `tick_ms`, average always half — uniform, no tail.

Memory: 1 536 B idle (one level; the rest are lazy), **0 marginal bytes per
job** once the arena is warm, and a million jobs release to 48 bytes on drop.

| tick_ms | 1M jobs queued | after firing | on drop |
|---|---|---|---|
| 1 | 44.1 MB | 48.1 MB | 48 B |
| 10 | 42.5 MB | 46.5 MB | 48 B |
| 1000 | 34.0 MB | 38.0 MB | 48 B |

### Choosing `tick_ms` — and a correction

**Do not pick it for memory.** The per-job minimum (ticket + slot +
generation) does not depend on the tick. What changes is how the load lands
across levels: a span that falls just above a level boundary piles into one
fat bucket whose `Vec` doubles and keeps its capacity.

That is why `tick=10` measured WORST for a 60 s span (239 KB arena) while
both 1 and 1000 measured ~30-80 KB. The rule is not "finer is better" or
"coarser is cheaper" — it is **avoid a span that lands just past a level
boundary**.

Pick it from the SHORTEST delay a client may ask for. At 100 ms a
`nack_delay(50)` comes back in up to 150 ms — three times what was asked.

### Audit findings, fixed

An audit of the scheduler found seven; two were job loss.

1. **`queue` destroyed the payload** on the already-elapsed path. `Elapsed(T)`
   hands it back — which meant `Queued` could not stay `Copy`.
2. **A panicking listener stranded the rest of its batch.** `advance_to` had
   already emptied them out of the wheel, so they were live forever and
   reachable by no future wake. The batch and its cursor now live on the
   struct. *Fixing this exposed a bug in the first attempt: `advance_to`
   CLEARS the vec it is handed, so parking leftovers in that same vec
   deleted them. The test caught it.*
3. **`u16` generation ABA** — the free list is LIFO, so reuse concentrates
   and 16 bits came back around in hours on a hot slot. Widened to `u32`.
4. **Silent slot-index truncation** — `as u32` now `try_from().expect()`.
5. **Cross-scheduler tokens** — `Ns`/`Ticket` carry a scheduler id. The bad
   case was not the panic; it was the silence, where `cancel` destroyed a
   stranger's job and returned `true`.
6. The wheel re-export leaked the abstraction. Removed.
7. **No way to recover a payload** — `unqueue` returns it, without which
   rescheduling forces every caller to keep a second copy of the state this
   module exists to remove.

The ticket grew 8 → 12 bytes for that soundness: 1M jobs at tick=10 went
42.5 MB → 56.7 MB. Named so the trade is visible, not buried.

Still true and untested: the `u32` ABA is safe **by construction**, not by
test — 4 billion reuses cannot be exercised.

---

## 7. Measurements

### The drain stall, and its cause

`replay` timed out at 120 s with `missing=144672`, in bursts of 2.5M msg/s
separated by dead windows of exactly 30 s and 60 s. Same binary with pinning
disabled: 194 ms, 2.57M msg/s, no window at all.

Three hypotheses died against measurement before the cause appeared:

| Hypothesis | Test | Result |
|---|---|---|
| socket backpressure | direct fd write + flush `owed` | unchanged |
| starving the connection task | unconditional `yield_now()` per cycle | gaps identical (29 954 / 60 177 ms) |
| lost gate wakeup | per-second tally of the worker | drain ran ZERO cycles — not blocked, nothing to deliver |

The tally named it: `pub:batch` was running on shard 0's thread while the
stream lived on shard 8. The publisher had been pinned by round-robin, which
knows nothing about where its data is. Every publish then crossed a bounded
mpsc between two `current_thread` runtimes, neither able to absorb the
other's burst, and the pair quantised into timer-length stalls.

**A guessed pin is worse than no pin.** Only a shard's own listener states
where a connection's data lives; the bootstrap port states nothing.

### Ceremony vs work — `benches/owned_shape.rs`

Identical work at the bottom (same `MemoryStore::append_batch`, same
`HashMap` remove), so what separates them is ceremony:

| batch | operation | owned | crossing / passes | factor |
|---|---|---|---|---|
| 1 | publish | **46.5 ns** | 151.2 ns | **3.3×** |
| 1 | ack | 18.0 ns | 17.8 ns | **1.0×** |
| 256 | publish | **59.5 ns/msg** | 95.4 ns/msg | 1.6× |
| 256 | ack | **9.2 ns/ack** | 15.9 ns/ack | 1.7× |

**That 1.0× contradicts the argument it was built to support.** For a SINGLE
ack the six passes cost nothing — six loops of one are absorbed. The cost is
real only for `BatchAck`, which is where acks actually arrive in volume.

### End to end

Fanout with explicit acks — the only section that exercises the ack path:

| | median | range |
|---|---|---|
| before | 1 313 648 | 1 127 458 – 1 371 057 |
| after | **1 676 127** | 1 588 793 – 1 769 702 |

**+28%, and the ranges do not overlap.** Fanout RSS fell too: the
per-binding per-cycle `Vec<DeliveredEntry>` was an allocation on the
delivery hot path.

At 500 000 messages per iteration:

| section | 1conn | 16conn |
|---|---|---|
| `publish_batch` | 9 992 397 | **17 549 493** |
| `publish_batch_wait` | 845 409 | 7 687 590 |
| `publish_single` | 2 097 745 | **1 332 799** ← degrades |

`publish_single` degrading with concurrency is the routed path: the bench
connects to the bootstrap port, those connections are deliberately NOT
pinned, so `local::owns(idx)` is always false and **every publish crosses**.
`publish_batch` does not degrade because it amortises — one command per 256
messages.

Measured on the same tree: the routed append cost 99.34 ns/msg against 2.73
for the local door. **36×.**

### Open regressions, unexplained

Measured against the branch point `f73393e`, medians of 3 runs vs 2:

| | baseline | after `d00abcf` | Δ |
|---|---|---|---|
| `publish_batch_wait` 1conn | 1 086 061 | 781 936 | **−28%** |
| `publish_batch` 4conn | 12 658 732 | 9 947 331 | **−21%** |
| `replay_fanout` Δ RSS | +423 MB | +809 MB | **1.91×** |

None investigated. Do not quote them as acceptable.

*(The memory one is NOT `DirectEgress.owed`: dropping `DIRECT_BACKLOG_LIMIT`
from 8 MB to 256 KB — 32× — left fanout delta RSS at +799 MB, dead centre of
its range. It only cost speed.)*

---

## 8. Known problems, not solved

- **The global directory.** `ShardRouter` is `#[derive(Clone)]` and cloned
  **per connection**, carrying `shards[]`, `gates[]`, `counters[]`,
  `_shard_runtimes[]`, `shard_ports`. That is why `next_home_shard()` could
  exist at all: the connection knew no shard, but the router it carries
  knows them all. It exists because the bootstrap port lets a connection
  publish to any stream. Kill that and the directory has no reader.

- **The shared cursor.** It is per shard, not per consumer. When one
  consumer's `ack_wait` expires, the cursor rewinds **for everyone**, and
  every other consumer needs a way to say "I already acked that". That is
  the entire purpose of `ack_floor.rs` — the contiguous floor, the
  out-of-order `BTreeSet`, `OOO_CAP`. 245 lines undoing a rewind that should
  never have reached them.

- **`ConsumerSubjects` and `drain_events.rs`.** Drain-owned only because the
  ack path cannot reach them. Migrating them takes roughly 400 more lines
  whose comments are dominated by starvation, leak and reordering proofs —
  all proofs about a race between two tasks that cannot race.

- **Eleven deadlines, five mechanisms.** `ack_wait`, `nack delay`,
  `idempotency_window`, `deliver_at_ms`, cron, `drain_stall_evict_ms`,
  `EVICTION_INTERVAL`, `metrics_interval`, `jail_cooldown_ms`, TLS
  handshake, `keepalive_interval` — served by the hierarchical wheel, a flat
  wheel, a journal with its own maturation loop, `select!` sleep arms, and
  `tokio::time::timeout`. The wheel serves 2 of 11.

- **`EVICTION_INTERVAL` wakes every idle shard every 5 s, forever.** The
  wheel's arm is conditional and correctly sleeps; this one is not.
  Measured: idle shards printing `wake:evict` at ~4 950 ms intervals.

- **`store_stats().await` per message** in `v2_publish`'s quota pre-check,
  for streams with `DiscardPolicy::New`.

- **Dead `shard_for` calls** at `dispatch_v2.rs:1044`, `:1196`, `:1260` —
  an arc_swap guard computed and discarded per ack frame. The compiler
  already says so: `warning: unused variable: shard`.

- **`binding_id: 0` for `NackDelay`** is a sentinel, the thing this document
  argues against. It holds only because `NackDelay` never reads it. Making
  the type enforce it means moving the field into the enum variant, which
  changes the struct size.

---

## 9. Method traps fallen into this session

Recorded because each one produced a wrong claim that survived until
measured.

- **Comparing two sides with different configuration.** One side ran with
  `ARBITRO_COMMAND_PATH=queue`, the other without, and the flag's run sat at
  the optimistic end of every spread. Always `env -u ARBITRO_COMMAND_PATH`
  on both sides.

- **Shell variables that never reached WSL.** `for p in queue direct` inside
  a quoted `wsl bash -lc '...'` expanded to empty, so four runs wrote one
  file and none carried the variable. Use a heredoc, and encode the config
  in the OUTPUT FILENAME so a failure is visible.

- **`git checkout` failing silently in a bisect.** The worktree stores a
  Windows path that git inside WSL cannot resolve, so every checkout failed
  and every probe measured the same commit. Consistent numbers were read as
  confirmation. Check the exit code, and verify the commit after switching.

- **Dividing a whole arena by the live count.** Reported 478 B/job when the
  marginal cost was 0 B. Measure the STEP over a warm arena, not the total
  over the tenants.

- **A saturating subtraction hiding the defect under test.** The precision
  table used `saturating_sub`, so an early firing would have shown as a
  perfect 0 ms. Assert the direction; do not clamp it.

- **Killing a background task without killing its child.** `TaskStop` ended
  the wrapper; the `cargo` beneath kept the `target/` lock, and the next
  command blocked. Always verify the process table, always use a timeout.

- **Asserting a property the design does not have.** Memory was asserted to
  return to baseline; it converges to a plateau instead, because buckets and
  slabs keep their capacity on purpose. The right assertion is boundedness,
  not return-to-zero.

---

## 10. Order of work

1. **Publish path** — connection → `Shard::publish` → store. Deletes
   `PublishCmd`, `PublishReply`, `PublishEntryOwned`, `dedup_routed`.
2. **Ack path** — `Shard::ack` → `engine.ack`. Deletes `command.rs`,
   `handle.rs`, `commands.rs`.
3. **One source of truth for counters** — the engine's, deleting
   `SharedCounters`.
4. **Per-consumer cursor** — deletes `ack_floor.rs`.
5. **`ConsumerSubjects` into the engine** — deletes `drain_events.rs`,
   `pending_drain_acks`, `pending_consumer_remove`.
6. **The scheduler replaces the five timer mechanisms.**
7. **Ingress owned** — a connection reaches only the shard that owns its
   streams, which deletes `ShardRouter`'s directory and the routed path
   entirely.

Each step keeps the suite green and lands as its own commit. Steps 1 and 2
are the ones with measured numbers behind them; the rest are the same
argument applied further.
