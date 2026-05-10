# Arbitro

> [!WARNING]
> **Project Status: In Development**. Arbitro is currently in active development. APIs and wire protocols are subject to change. Not yet recommended for production use without prior testing.

**Arbitro** is a Stateful Flow Broker designed for ultra-high concurrency and sub-microsecond predictability. It isn't just a message pipe; it's a reactive engine that uses KV state to control message flow with hardware-level efficiency.

Built in Rust with a **Zero-Allocation, Zero-Copy** architecture, Arbitro follows the principle of **Hardware Sympathy** to maximize L1/L2 cache locality and eliminate heap fragmentation.

## Star Feature: `MaxSubjectInflight`

Arbitro's unique power is not "streams + consumers" — that's table stakes. It is **per-subject, per-consumer flow control** with wildcard patterns, resolved at delivery time with O(1) atomics on the hot path.

```rust
let consumer_cfg = ConsumerConfig::new(b"gateway", b"ORDERS")
    .filter(b"orders.>")
    .ack_policy(AckPolicy::Explicit)
    .max_inflight(10000)
    .max_subject_inflight(b"orders.premium.>", 30)   // 30 per unique premium.*
    .max_subject_inflight(b"orders.basic.>", 10)     // 10 per unique basic.*
    .max_subject_inflight(b"orders.freemium.>", 1)   // 1 per unique freemium.*
    .build();
```

One rule isolates an unbounded number of subjects. A saturated `orders.freemium.u_12345` does **not** impact `orders.freemium.u_12346` — each unique subject is an independent credit pool.

## Key Features

- **Massive Subject Partitioning** — millions of unique subjects, one rule.
- **Ultra-High Throughput** — 14.2M+ msg/s ingest, 4.3M+ msg/s replay drain.
- **Predictable Latency** — sub-microsecond internal dispatch, zero GC pauses.
- **Crash-Safe Persistence** — Magic Byte (0xAF) validation survives `SIGKILL`.
- **Reactive Model** — callback + pull subscription modes.
- **Shard-Parallel Architecture** — lock-free drain + command threads per shard.
- **Ack Timeout & Nack Delay** — per-consumer timing wheel auto-nacks stale deliveries and supports delayed requeue.

## Performance (E2E Throughput)

Arbitro is built for **Hardware Sympathy**. Benchmarks represent the full end-to-end cycle (TCP + Protocol + Engine) on a single server instance (WSL, 64B payload, Memory backend).

| Mode | Throughput (1K → 1M msgs) | Latency / unit |
|------|----------------------------|----------------|
| **Publish (ingest)** | 6.3M — 14.2M msg/s | ~70 ns |
| **Cycle fire-and-forget** | 6.5M — 15.1M msg/s | ~66 ns |
| **Cycle explicit ack** | 2.1M — 2.52M msg/s | ~390 ns |
| **VIP subject isolation** | Independent of noise | 31 ns (L3) — 84 µs (net) |

## Endurance & Stability (1-minute sustained)

| Scenario | Throughput | CPU | RSS | Stability |
|----------|-----------|-----|-----|-----------|
| Memory hot-path | ~2.8M msg/s | ~10.8% | ~2.99 GB | O(1) hybrid index |
| Disk persistence | ~35.4k msg/s | ~3.2% | ~120 MB | Crash-safe (0xAF) |
| Chaos resilience | 10 s stress | Isolated PIDs | Stable | 100% recovery proof |

> [!IMPORTANT]
> **Performance Consistency**: Introducing *Dynamic Subject Isolation* resulted in **0% regression** in global throughput.

## Architecture Overview

Arbitro is a workspace of seven crates:

```
arbitro-proto    # wire protocol (zerocopy-backed, repr(C))
arbitro-engine   # single-threaded oracle (catalog + matcher + inflight)
arbitro-common   # Gate, NameRegistry, IdPool
arbitro-store    # journal trait + Memory / Tolerant backends
arbitro-server   # shard orchestration + transport + persistence
arbitro-client   # client SDK
arbitro-e2e      # integration tests + benchmarks
```

Each **shard** owns one engine + one store and runs two dedicated OS threads:

- **Drain thread** — linear walk of the store, atomic reads of counters, dispatch to TCP. Zero locks on engine.
- **Command thread** — mutates engine via `&mut self`, updates atomics, swaps snapshots.

Communication across threads is **lock-free**: atomics for counters, `arc_swap`-style snapshot pointers for structural state, and mpsc channels for drain → command notifications.

Full architectural details, sharding strategy, and data-structure trade-offs live in [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md).

## Quick Start

### Build and Run (WSL only — 9P on `/mnt` is slow)

```bash
# Compile from the Windows source
cargo build --release -p arbitro-server

# MUST copy to /tmp to avoid 9P filesystem bottleneck
mkdir -p /tmp/arbitro && cp -a ./target/release/arbitro-server /tmp/arbitro/
cd /tmp/arbitro && ./arbitro-server
```

### Docker

```bash
docker compose up -d   # default port: 4222
```

## Environment

| Variable | Default | Description |
|----------|---------|-------------|
| `ARBITRO_LISTEN` | `0.0.0.0:9898` | TCP listen address |
| `ARBITRO_MAX_CONNECTIONS` | `10000` | Max concurrent TCP connections |
| `ARBITRO_WRITE_BUFFER_CAP` | `8192` | Write channel capacity per connection |
| `ARBITRO_IDLE_TIMEOUT` | `300` | Idle timeout (s) |
| `ARBITRO_KEEPALIVE_INTERVAL` | `30` | Keepalive ping interval (s) |

## Usage

### Callback subscription (zero-latency)

```rust
let _handle = consumer.subscribe_callback(Some(b"orders.premium.>"), move |msg| {
    println!("VIP logic fired: {:?}", msg.subject);
    msg.ack();
}).await?;
```

### Pull subscription (worker-paced)

```rust
let mut sub = consumer.subscribe(Some(b"orders.basic.>")).await?;
while let Some(msg) = sub.next().await {
    // Process at your own speed
    msg.ack();
}
```

### Negative acknowledgement with delay

```rust
while let Some(msg) = sub.next().await {
    match process(&msg) {
        Ok(_) => msg.ack(),
        Err(_) => msg.nack_delay(5000), // retry after 5 seconds
    }
}
```

### Publish

```rust
// Single fire-and-forget
client.publish(b"ORDERS", b"orders.freemium.u1", payload).await?;

// High-density batch (14.2M msg/s)
client.publish_batch(b"ORDERS", &[
    (b"orders.premium.u1", &payload),
    (b"orders.premium.u2", &payload),
]).await?;
```

## Roadmap

### Phase 1 — Core Engine (done)
- [x] Zero-copy hot path
- [x] Dynamic subject isolation (`MaxSubjectInflight`)
- [x] Atomic state management
- [x] Linear-ingestion store
- [x] Shard-parallel drain/command split
- [x] Ack timeout (per-consumer timing wheel, auto-nack on expiry)
- [x] Nack with delay (delayed redelivery via timing wheel)

### Phase 2 — Persistence & Connectivity (done)
- [x] Disk persistence (TolerantStore)
- [x] Crash-safe journaling (Magic Byte 0xAF)
- [x] Sync acks (`AckSync`)
- [x] Per-entry `consumer_id` routing (broadcast collapse)
- [ ] Subject scavenging (TTL-based inactive-slot cleanup)
- [ ] Multi-language clients (TypeScript, Go)

### Phase 3 — Observability & Scale (planned)
- [ ] Prometheus-native metrics
- [ ] Clustering (Raft) for stream state replication
- [ ] Adaptive subject prioritization
- [ ] Cross-shard subject aggregation for global limits

## Next Session Context

If a new session is picking up this project, start with [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md). It documents:

- Every data structure and whether it's sharded
- BucketArray vs HashMap decision matrix
- Thread ownership model
- Lock-free synchronization primitives
- Open work ordered by priority
- Testing + benchmark rules

## License

MIT
