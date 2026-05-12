---
description: Workflow for auditing Ingress/Egress role separation, ownership fences, and latency isolation
arguments: []
---

# INGRESS/EGRESS ROLE AUDIT WORKFLOW

This workflow ensures that the Ingress (Command-driven) and Egress (Data-driven) flows are strictly segregated and do not interfere with each other.

## 0. Abstract Purpose & Boundaries

### INGRESS: The Gatekeeper of Persistence
The purpose of Ingress is **Admission Control and Reliable Persistence**.
- **Does NOT care about**: Delivery, consumers, or who (if anyone) will read the data.
- **DOES care about**:
    - **Resource Constraints**: Is the stream full? (`max_bytes`, `max_msgs` limits).
    - **Physical Existence**: Does the target `StreamId` exist and is it writable?
    - **Admission Integrity**: Is the frame valid, aligned, and safe to persist?
- **Responsibility**: Once Ingress replies `RepOk`, the message is legally committed to the persistence pipeline.

### EGRESS: The Dispatcher of Data
The purpose of Egress is **Intelligent Delivery and Load Balancing**.
- **Does NOT care about**: How the data was written or protocol handshake details.
- **DOES care about**:
    - **Consumer Capacity**: `max_inflight`, `max_subject_inflight`.
    - **Filtering**: `subject_hash` matching against active bindings.
    - **State Recovery**: Retrying NACKs and managing pending queues.

---

## 1. Ownership Fence Audit
- [ ] **Primitive Ownership**: Verify that only Ingress roles (`Publisher`, `Acker`, `Admin`) mutate the Engine and Store state, and only the `Drainer` (Egress) performs deliveries.
- [ ] **Gate Protocol**: Assert that the `Gate` is opened (`release()`) by Ingress roles and closed (`lock()`) ONLY by the `Drainer`.
- [ ] **Disk I/O Isolation**: Verify that `Publisher` and `Accumulator` never call `fsync` or block on disk writes; they must enqueue to the async `Disk Writer`.
- [ ] **Store Read/Write**: Ensure `Drainer` and `Fetcher` use the store's cached backend and do not block the shard thread on synchronous disk reads.

## 2. Ingress Integrity (Command-driven)
- [ ] **No Speculative Delivery**: Assert that `handle_publish` and `handle_ack` never build or send `Deliver` frames.
- [ ] **Admin/Hot isolation**: Verify that `Admin` operations (structural changes) do not block the hot path beyond the necessary mutex/snapshot update.
- [ ] **Reply Consistency**: Ensure every Ingress command receives a `oneshot` reply and never "hangs" waiting for Egress work.

## 3. Egress Integrity (Data-driven)
- [ ] **No Command Handling**: Verify that the `Drainer` loop never processes `ShardCommand` variants or sends command replies.
- [ ] **Push/Pull Segregation**: Assert that the `Drainer` only sees `push_subs` and is structurally unaware of `pull_subs` (handled by `PullHandler`).
- [ ] **Seeder Logic**: Ensure the `Seeder` is triggered only by Ingress (`Subscribe`) and does not run concurrently with the `Drainer` on the same stream.

## 4. Latency Isolation & Backpressure
- [ ] **Cross-Flow Interference**: Measure Publish latency during a massive Drain backlog. Assert that Ingress remains responsive (ns/µs range).
- [ ] **Gate Coalescing**: Verify that multiple `Gate.release()` calls from Ingress are correctly coalesced by the atomic bit-OR, preventing redundant wakeups.
- [ ] **Shared Primitives**: Audit the `Fetcher`. Ensure it is a pure read function used by both flows without introducing shared-state locks.

## 5. Verification via Benchmarks
- [ ] **Throughput Bench**: Run `throughput.rs` with mixed Publish/Deliver load. Verify that the combined throughput follows a predictable sharding model without global contention.
- [ ] **Chaos Audit**: Run `chaos.rs`. Simulate slow disk writes and verify that Ingress continues to queue messages while Egress backpressure handles the delivery slowdown.
