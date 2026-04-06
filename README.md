# Arbitro

High-performance message broker written in Rust. Zero-copy wire protocol, batch delivery, async drain architecture.

## Performance

Benchmarked on a single server instance (loopback TCP, 64B payload):

| Workload | 1K msgs | 1M msgs |
|----------|---------|---------|
| Publish | 410us (2.4M/s) | 68ms (14.6M/s) |
| Cycle fire-forget | 311us (3.2M/s) | 64ms (15.6M/s) |
| Cycle explicit ack | 425us (2.35M/s) | 425ms (est.) (2.3M/s) |

## Architecture

```
Client ──TCP──> Server ──> Engine ──> Store
                             │
                        DrainSignal (Gate/Notify)
                             │
                         DrainTask ──> Transport ──TCP──> Client
```

**Publish path:** parse frame -> validate -> append to store (one shard lock) -> signal drain -> RepOk to client (outside lock).

**Delivery path:** async drain task per stream waits on Gate (tokio::Notify). Wakes on publish/ack/subscribe. Collects entries into batch RepBatch frames (up to 256 per frame), sends one frame per consumer per cycle.

**Ack path:** client accumulates acks, flushes as BatchAck frames. Server releases credit in one shard lock, signals drain for pending deliveries.

## Wire Protocol

Binary, little-endian, zero-copy (zerocopy overlays on `&[u8]`).

Every frame starts with a 16-byte envelope:

```
[2 action][1 flags][1 rsv][4 stream_id][4 msg_len][4 env_seq]
```

Key actions:

| Code | Action | Direction |
|------|--------|-----------|
| 0x0101 | Publish | client -> server |
| 0x0205 | RepBatch | server -> client (batch delivery) |
| 0x0206 | BatchAck | client -> server (batch ack) |
| 0x0201 | Ack | client -> server (single ack) |
| 0x0203 | RepOk | server -> client |
| 0x0204 | RepError | server -> client |

## Workspace Crates

| Crate | Description |
|-------|-------------|
| `arbitro-proto` | Wire protocol structs, action codes, config types, zero-copy views |
| `arbitro-store` | Storage trait + MemoryStore implementation with callback-based reads |
| `arbitro-common` | Shared utilities: subject matching, credit map, flusher |
| `arbitro-engine` | Core engine: publish, drain, subscribe, stream management |
| `arbitro-client` | Async Rust client with auto-reconnect, batch publish/ack |
| `arbitro-server` | TCP server: Gate, drain tasks, TokioTransport |
| `arbitro-benches` | Criterion benchmarks: e2e throughput, drain steps, store comparison |

## Quick Start

### Build and run

```bash
cargo build --release -p arbitro-server
./target/release/arbitro-server
```

### Docker

```bash
docker compose up -d
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ARBITRO_LISTEN` | `0.0.0.0:4222` | TCP listen address |
| `ARBITRO_MAX_CONNECTIONS` | `10000` | Max concurrent connections |
| `ARBITRO_WRITE_BUFFER_CAP` | `8192` | Write channel capacity per connection |
| `ARBITRO_IDLE_TIMEOUT` | `300` | Idle timeout in seconds |
| `ARBITRO_KEEPALIVE_INTERVAL` | `30` | Keepalive ping interval in seconds |
| `ARBITRO_SHUTDOWN_TIMEOUT` | `10` | Graceful shutdown timeout in seconds |

### Client Usage

```rust
use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig, StreamConfig};

#[tokio::main]
async fn main() {
    let client = Client::connect("127.0.0.1:4222").await.unwrap();

    // Create a stream
    let stream_cfg = StreamConfig::new(b"ORDERS").build();
    client.create_stream(&stream_cfg).await.unwrap();

    // Publish
    client.publish(b"ORDERS", b"orders.created", b"{}").await.unwrap();

    // Batch publish
    let entries = vec![
        (b"orders.created".as_slice(), b"msg1".as_slice()),
        (b"orders.updated".as_slice(), b"msg2".as_slice()),
    ];
    client.publish_batch(b"ORDERS", &entries).await.unwrap();

    // Create consumer (fire-and-forget)
    let consumer_cfg = ConsumerConfig::new(b"my-consumer", b"ORDERS")
        .filter(b"orders.>")
        .ack_policy(AckPolicy::None)
        .build();
    let consumer = client.create_consumer(&consumer_cfg).await.unwrap();

    // Subscribe and receive
    let mut sub = consumer.subscribe(None).await.unwrap();
    while let Some(msg) = sub.next().await {
        println!("seq={} subject={:?}", msg.seq, msg.subject);
    }
}
```

### Explicit Ack

```rust
let consumer_cfg = ConsumerConfig::new(b"ack-consumer", b"ORDERS")
    .filter(b"orders.>")
    .ack_policy(AckPolicy::Explicit)
    .max_inflight(1000)
    .ack_wait_ms(30_000)
    .build();
let consumer = client.create_consumer(&consumer_cfg).await.unwrap();
let mut sub = consumer.subscribe(None).await.unwrap();

while let Some(msg) = sub.next().await {
    // Process message...
    msg.ack(); // Batched automatically by the client
}
```

## Design Rules

See `.agent/rules/performance.md` for the full list. Key principles:

- **Zero-copy hot path** -- zerocopy overlays, one copy max (into journal)
- **No allocations on publish/deliver** -- pre-allocated scratch buffers, reused across cycles
- **Single lock per stream** -- append under shard lock, release fast, signal drain
- **Batch everything** -- batch publish, batch delivery (RepBatch), batch ack (BatchAck)
- **No channels on hot path** -- Gate (tokio::Notify) replaces channels for drain signaling
- **No Instant::now() on hot path** -- coarse timestamps passed from caller

## Tests

```bash
cargo test --workspace
```

## Benchmarks

```bash
cargo bench --bench e2e_throughput
cargo bench --bench drain_steps
```

## License

MIT
