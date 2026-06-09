# Arbitro

> [!WARNING]
> **Project Status: In Development**. Arbitro is currently in active development. APIs and wire protocols are subject to change. Not yet recommended for production use without prior testing.

**Arbitro** is a Stateful Flow Broker designed for high concurrency and low-latency message delivery. It is not just a message pipe; it is a reactive engine that uses KV state to control message flow efficiently.

## Star Feature: `MaxSubjectInflight`

Arbitro's unique power is not "streams + consumers" -- that is table stakes. It is **per-subject, per-consumer flow control** with wildcard patterns, resolved at delivery time with O(1) atomics on the hot path.

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

One rule isolates an unbounded number of subjects. A saturated `orders.freemium.u_12345` does **not** impact `orders.freemium.u_12346` -- each unique subject is an independent credit pool.

## Key Features

- **Massive Subject Partitioning** -- millions of unique subjects, one rule.
- **High Throughput** -- optimized hot path, no GC pauses.
- **Crash-Safe Persistence** -- Magic Byte (0xAF) validation survives `SIGKILL`.
- **Reactive Model** -- callback + pull subscription modes.
- **Parallel Architecture** -- publish never blocks on delivery.
- **Ack Timeout & Nack Delay** -- per-consumer timing wheel auto-nacks stale deliveries and supports delayed requeue.

## Quick Start

### Install from source

```bash
# Direct install of the broker binary from this repo
cargo install --git https://github.com/zenozaga/arbitro-io arbitro-server

# Or for the in-process client lib (add to your own Cargo.toml):
#   arbitro-client-tokio = { git = "https://github.com/zenozaga/arbitro-io" }
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
| `ARBITRO_CLUSTER_PEERS` | _unset_ | Comma-separated peer list: `1@host:port,2@host:port,3@host:port`. Enables Raft clustering (requires `cluster` feature). |
| `ARBITRO_CLUSTER_NODE_ID` | `1` | This node's peer ID in the cluster. |
| `ARBITRO_CLUSTER_LISTEN` | `0.0.0.0:9900` | TCP address for Raft inter-node traffic. |

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
via the `ConsumerStats` action -- see the Rust/TypeScript clients for
`get_pending(consumer_id)` / `getPending(consumerId)` APIs.

### Operator endpoints + signals

| Surface | Trigger | Output |
|---------|---------|--------|
| `/health` (HTTP) | `ARBITRO_HEALTH_LISTEN=0.0.0.0:9090` | `200 OK` / `503` based on shard liveness. |
| `/metrics` (HTTP) | `ARBITRO_METRICS_LISTEN=0.0.0.0:9091` | Prometheus text-format counters + gauges: `arbitro_publish_total`, `arbitro_deliver_total`, `arbitro_ack_total`, `arbitro_nack_total`, `arbitro_streams`, `arbitro_consumers`, `arbitro_connections`, `arbitro_ack_pending`, `arbitro_silent_drops_*`. |
| `SIGUSR1` (Unix) | `kill -USR1 <pid>` | Writes `/tmp/arbitro-dump-<pid>.json` with a flat diagnostic snapshot (gauges, silent drops, per-stream messages/bytes). |
| `SIGHUP` (Unix) | `kill -HUP <pid>` | Re-reads the log filter from `ARBITRO_LOG` (live log-level reload, no restart). |
| `arbitroctl` (CLI) | `cargo install --git ... arbitroctl` | `list-streams`, `list-consumers`, `create-stream`, `delete-stream`, `purge-stream`, `drain-subject`, `delete-message`, `consumer-pending`. Talks to `ARBITRO_ADDR` (default `127.0.0.1:9898`). |

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

// High-density batch
client.publish_batch(b"ORDERS", &[
    (b"orders.premium.u1", &payload),
    (b"orders.premium.u2", &payload),
]).await?;
```

## Cron Scheduling

Register distributed cron jobs directly on the broker. Multiple workers can register the same name -- only one receives each fire (queue semantics). Crons survive reconnects automatically.

```rust
let cron = client.cron(b"billing-monthly")
    .every(b"0 0 1 * *")
    .tz(b"America/New_York")
    .run(|ctx| async move {
        process_billing(ctx.fire_count).await;
    })
    .await?;

// Later:
cron.stop().await?;
```

```typescript
const cron = await client.cron("billing-monthly")
    .every("0 0 1 * *")
    .tz("America/New_York")
    .run(async (ctx) => {
        await processBilling(ctx.fireCount);
    });

cron.stop();
```

Cron jobs live in memory -- if the broker restarts, clients re-register automatically on reconnect.

## Delayed Publish

Schedule message delivery for the future. Messages are persisted immediately -- if the broker restarts, delayed messages survive and deliver on time.

```rust
// Deliver this message 5 seconds from now
client.publish_delayed(stream_id, b"orders.reminder", payload, 5000).await?;
```

```typescript
await client.publishDelayed("ORDERS", "orders.reminder", payload, 5000);
```

## Workflow Orchestration

Workflows are **entirely client-side**. The broker provides streams, consumer groups, and idempotent publish -- zero workflow-specific code runs in the broker. This means any language client can implement workflows using the same primitives.

### Example (Rust)

```rust
let wf = client.workflow(b"order-process")
    .trigger(b"orders.created")
    .trigger_stream(orders_stream_id) // auto-subscribe for trigger
    .step(b"validate", |ctx| async move {
        let order = validate(ctx.context)?;
        Ok(StepResult { context: order })
    })
    .compensate(b"validate", |ctx| async move {
        rollback_validation(ctx.context).await;
        Ok(StepResult { context: ctx.context })
    })
    .step(b"charge", |ctx| async move {
        let receipt = charge(ctx.context).await?;
        Ok(StepResult { context: receipt })
    })
    .compensate(b"charge", |ctx| async move {
        refund(ctx.context).await;
        Ok(StepResult { context: ctx.context })
    })
    .step(b"ship", |ctx| async move {
        let tracking = ship(ctx.context).await?;
        Ok(StepResult { context: tracking })
    })
    .max_retries(3)
    .max_context_size(256 * 1024)
    .ack_wait_ms(30_000)
    .max_inflight(10)
    .start().await?;

// Trigger manually (auto-trigger also works via trigger_stream)
let instance_id = wf.trigger(&client, b"initial context").await?;

// Stop processing
wf.stop();
```

### Features

| Feature | Description |
|---------|-------------|
| **Auto-trigger** | `.trigger_stream(id)` subscribes to an external stream and creates workflow instances on match. |
| **Saga / Compensation** | `.compensate()` registers rollback handlers per step. On permanent failure, compensations run in reverse for all completed steps. |
| **Dead Letter Queue** | After `max_retries` exhausted, task + error go to `_wf_{name}_dlq` stream. |
| **Context guard** | `max_context_size` (default 256 KB) rejects oversized payloads. |
| **Multi-worker distribution** | Consumer group with round-robin delivery. Each process gets its own consumer in the shared group. |
| **Idempotent transitions** | `publish_with_id` deduplicates step publishes. Survives broker restart. |

## Message Replication

Arbitro supports async message replication on a per-stream basis. Replication is controlled by the `replicas` parameter at stream creation time: `replicas=1` (default) means no replication; `replicas=3` replicates to two followers.

```rust
// Rust — create a stream with 3 replicas
client.create_stream(b"orders", b"orders.>", 0, 0, 0, 3, 0, 0, 0, 0).await?;
```

Replication is transparent to clients. Publish, subscribe, ack, and nack work identically regardless of the replica count.

## License

MIT
