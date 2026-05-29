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
- **Shard-Parallel Architecture** — split-phase drain (store read + lock-free delivery) + command threads per shard. Publish never blocks on drain.
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
arbitro-client   # Rust client SDK (tokio, optional TLS)
arbitro-e2e      # integration tests + benchmarks
```

Each **shard** owns one engine + one store and runs two dedicated OS threads:

- **Drain thread** — split-phase: reads from store (brief Mutex), then delivers to TCP lock-free. Zero locks on engine.
- **Command thread** — mutates engine via `&mut self`, updates atomics, swaps snapshots.

Communication across threads is **lock-free**: atomics for counters, `arc_swap`-style snapshot pointers for structural state, and a SPSC ring for drain → command notifications. The store Mutex is held only during the linear walk (~10 µs), not during TCP delivery (~400 µs) — publish proceeds concurrently with delivery.

Full architectural details, sharding strategy, and data-structure trade-offs live in [`docs/ARCHITECTURE.md`](./docs/ARCHITECTURE.md).

## Quick Start

### Install from source

```bash
# Direct install of the broker binary from this repo
cargo install --git https://github.com/zenozaga/arbitro-io arbitro-server

# Or for the in-process client lib (add to your own Cargo.toml):
#   arbitro-client-tokio = { git = "https://github.com/zenozaga/arbitro-io" }
```

### Build and Run from source (WSL only — 9P on `/mnt` is slow)

```bash
# Compile from the Windows source
cargo build --release -p arbitro-server

# MUST copy to /tmp to avoid 9P filesystem bottleneck
mkdir -p /tmp/arbitro && cp -a ./target/release/arbitro-server /tmp/arbitro/
cd /tmp/arbitro && ./arbitro-server
```

### Docker

```bash
docker compose up -d   # default port: 9898
```

## Environment

| Variable | Default | Description |
|----------|---------|-------------|
| `ARBITRO_LISTEN` | `0.0.0.0:9898` | TCP listen address |
| `ARBITRO_MAX_CONNECTIONS` | `10000` | Max concurrent TCP connections |
| `ARBITRO_WRITE_BUFFER_CAP` | `8192` | Write channel capacity per connection |
| `ARBITRO_IDLE_TIMEOUT` | `300` | Idle timeout (s) |
| `ARBITRO_KEEPALIVE_INTERVAL` | `30` | Keepalive ping interval (s) |
| `ARBITRO_METRICS_INTERVAL` | `5` | Periodic metrics log interval (s). `0` disables. |
| `ARBITRO_AUTH_TOKEN` | _unset_ | If set, every connection must send `Auth` (shared bearer token) before any other frame. |
| `ARBITRO_DATA_DIR` | _unset_ | Directory for the persistent journal + command log. Disables in-memory store when set. |
| `ARBITRO_TLS_CERT` | _unset_ | Path to a PEM cert. Enables TLS; `ARBITRO_TLS_KEY` required. |
| `ARBITRO_TLS_KEY` | _unset_ | Path to the matching PEM private key. |
| `ARBITRO_SHARDS` | `num_cpus` | Number of shard workers (one OS thread each). |
| `ARBITRO_SHUTDOWN_TIMEOUT` | `10` | Grace period (s) for in-flight writes before force-close on shutdown. |
| `ARBITRO_CHANNEL_CAPACITY` | `4096` | Per-shard command channel capacity. |
| `ARBITRO_MAX_FEED_PER_CYCLE` | `256` | Max store entries fed into the drain per cycle. |
| `ARBITRO_DRAIN_BATCH_SIZE` | `256` | Entries per `RepBatch` frame emitted by the drain. |
| `ARBITRO_MAX_FRAME_SIZE` | `67108864` | Max frame body bytes (64 MiB). Rejects oversized frames. |
| `ARBITRO_MAX_OPS_PER_SEC` | `0` | Max frames/sec per connection (`0` = unlimited). |
| `ARBITRO_FSYNC_POLICY` | `every` | Metadata fsync policy: `every` (default) or `none`. |

## Observability

On startup, the broker logs a single summary line of the recovered state:

```
INFO arbitro_server::server: listening addr=0.0.0.0:9898
INFO arbitro_server::server: broker state ready streams=4 consumers=12 messages=18302 bytes=4823104
```

Every `ARBITRO_METRICS_INTERVAL` seconds it then emits a metrics line with:

- **Gauges** (current state): `connections`, `streams`, `consumers`, `consumers_paused`,
  `ack_pending` (total in-flight unacked), `max_ack_pending` (worst-loaded consumer),
  `stream_messages`, `stream_bytes`.
- **Deltas this tick**: `published`, `delivered`, `acked`, `nacked`, `pub_no_match`,
  `held_inflight`, `held_subject`.

```
INFO arbitro_server::server: metrics interval_s=5 connections=2 streams=4 consumers=12
     consumers_paused=0 ack_pending=87 max_ack_pending=43 stream_messages=18302
     stream_bytes=4823104 published=4128 delivered=4115 acked=4072 nacked=0
     pub_no_match=0 held_inflight=12 held_subject=4
```

Clients can also query a single consumer's pending-ack count over the wire
via the `ConsumerStats` action — see the Rust/TypeScript clients for
`get_pending(consumer_id)` / `getPending(consumerId)` APIs.

### Operator endpoints + signals

| Surface | Trigger | Output |
|---------|---------|--------|
| `/health` (HTTP) | `ARBITRO_HEALTH_LISTEN=0.0.0.0:9090` | `200 OK` / `503` based on shard liveness. |
| `/metrics` (HTTP) | `ARBITRO_METRICS_LISTEN=0.0.0.0:9091` | Prometheus text-format counters + gauges: `arbitro_publish_total`, `arbitro_deliver_total`, `arbitro_ack_total`, `arbitro_nack_total`, `arbitro_streams`, `arbitro_consumers`, `arbitro_connections`, `arbitro_ack_pending`, `arbitro_silent_drops_*`. |
| `SIGUSR1` (Unix) | `kill -USR1 <pid>` | Writes `/tmp/arbitro-dump-<pid>.json` with a flat diagnostic snapshot (gauges, silent drops, per-stream messages/bytes). |
| `SIGHUP` (Unix) | `kill -HUP <pid>` | Re-reads the log filter from `ARBITRO_LOG` (live log-level reload, no restart). |
| `arbitroctl` (CLI) | `cargo install --git ... arbitroctl` | `list-streams`, `list-consumers`, `create-stream`, `delete-stream`, `purge-stream`, `drain-subject`, `consumer-pending`. Talks to `ARBITRO_ADDR` (default `127.0.0.1:9898`). |

For backup procedures, see [`docs/BACKUP.md`](./docs/BACKUP.md).

## Usage

### Callback subscription (zero-latency)

```rust
let _handle = consumer.subscribe_callback(Some(b"orders.premium.>"), move |msg| {
    println!("VIP logic fired: {:?}", msg.subject);
    msg.ack();
}).await?;
```

### Worker-paced consumption (pull semantics)

Arbitro does not have a separate `Pull` action on the wire. Pull-style
flow control is an emergent property of the existing primitives:

```rust
// Create the consumer with explicit acks + a bounded inflight cap.
// `max_inflight = N` means the broker will deliver up to N messages,
// then stop until the consumer acks — exactly the "fetch N, process,
// fetch N more" loop you'd expect from a pull API.
let consumer = ConsumerBuilder::new(b"worker")
    .filter(b"orders.basic.>")
    .max_inflight(10)
    .ack_policy(AckPolicy::Explicit)
    .create(&client, stream_id).await?;

let mut sub = client.subscribe(stream_id, consumer, b"").await?;
while let Some(msg) = sub.recv().await {
    // Process at your own speed. The broker stops pushing once
    // `max_inflight` is reached and resumes as you ack.
    msg.ack();
}
```

The `recv()` call drains a client-side buffer that the broker pushes
into; flow control is enforced server-side by `max_inflight + Ack`.
Set `max_inflight = u32::MAX` for firehose / pure-push behaviour.

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
- [x] Per-entry `consumer_id` routing (broadcast collapse)
- [x] TypeScript client (`@arbitro/client`)
- [x] Client TLS (`tokio-rustls`, behind `tls` feature flag)

### Phase 3 — Observability & Operability (done)
- [x] Prometheus-native `/metrics` endpoint
- [x] `/health` HTTP endpoint + k8s probes
- [x] `arbitroctl` CLI
- [x] `cargo audit` / `cargo deny` in CI
- [x] Docker image gated on e2e tests
- [x] Configurable rate-limit, fsync policy, MAX_FRAME_SIZE
- [x] `--version` / `--help` flags + config validation at startup
- [x] Protocol hardening (AckPolicy::None limits, stale config, namespaced consumers)

### Phase 4 — Scale (planned)
- [ ] Subject scavenging (TTL-based inactive-slot cleanup)
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
