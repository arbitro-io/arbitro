# Arbitro

**Arbitro** is a Stateful Flow Broker designed for ultra-high concurrency and sub-microsecond predictability. It isn't just a message pipe; it's a reactive engine that uses KV state to control message flow with hardware-level efficiency.

Built in Rust with a **Zero-Allocation, Zero-Copy** architecture, Arbitro follows the principle of **Hardware Sympathy** to maximize L1/L2 cache locality and eliminate heap fragmentation.

## Key Features

- **Subject-Based Flow Control**: Fine-grained `subject_limits` to prevent noisy neighbors and manage heterogeneous consumer speeds.
- **Zero-Copy Engine**: Wire protocol and internal delivery path use `repr(C)` composite headers for O(1) frame construction.
- **Linear Byte Log**: Arena-based `MemoryStore` for contiguous, cache-friendly message ingestion and draining.
- **High Concurrency**: Sharded stream architecture with sub-nanosecond synchronization primitives.

## Performance

> [!IMPORTANT]
> **WSL / Native Linux Mandatory**: To achieve these numbers, Arbitro **must** be compiled and run inside WSL or Native Linux. Running from `/mnt/` (9P Windows Bridge) is 2-10x slower.

Benchmarked on a single server instance (loopback, 64B payload):

| Workload | 1K msgs | 1M msgs |
|----------|---------|---------|
| Publish (Ingest) | 410us (2.4M/s) | 68ms (**14.6M/s**) |
| Cycle Fire-and-Forget | 311us (3.2M/s) | 64ms (**15.6M/s**) |
| Cycle Explicit Ack | 425us (2.35M/s) | 425ms (est.) (2.3M/s) |

## Quick Start

### Build and Run (WSL Only)

```bash
# Compile from the Windows source (it's fine to compile on /mnt/)
cargo build --release -p arbitro-server

# MUST copy to /tmp to avoid 9P filesystem bottleneck during execution
mkdir -p /tmp/arbitro && cp -a ./target/release/arbitro-server /tmp/arbitro/
cd /tmp/arbitro && ./arbitro-server
```

### Docker

```bash
# Default port: 4222
docker compose up -d
```

## Unique Functionality: Subject Limits

Arbitro's unique power is controlling flow at the subject level. This prevents one slow subscriber from blocking an entire stream.

```rust
use arbitro_client::Client;
use arbitro_proto::config::{AckPolicy, ConsumerConfig};

let client = Client::connect("127.0.0.1:9898").await.unwrap();

// Create a consumer with specific credit limits per subject pattern
let consumer_cfg = ConsumerConfig::new(b"gateway", b"ORDERS")
    .filter(b"orders.>")
    .ack_policy(AckPolicy::Explicit)
    .max_inflight(1000)
    // ONLY 2 messages allowed in-flight for legacy subjects
    .subject_limit(b"orders.legacy.>", 2) 
    // 100 messages for new orders
    .subject_limit(b"orders.v2.>", 100)
    .build();

let consumer = client.create_consumer(&consumer_cfg).await.unwrap();
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ARBITRO_LISTEN` | `0.0.0.0:9898` | TCP listen address (**Default Port: 9898**) |
| `ARBITRO_MAX_CONNECTIONS` | `10000` | Max concurrent TCP connections |
| `ARBITRO_WRITE_BUFFER_CAP` | `8192` | Write channel capacity per connection |
| `ARBITRO_IDLE_TIMEOUT` | `300` | Idle timeout in seconds |
| `ARBITRO_KEEPALIVE_INTERVAL` | `30` | Keepalive ping interval in seconds |

## Design Rules

See `.agent/rules/performance.md` for the full list. Key principles:

- **Zero-copy hot path**: `zerocopy` overlays, one copy max (into Arena).
- **No allocations on critical path**: Pre-allocated scratch buffers, reused across cycles.
- **O(1) Dispatch**: Match-based jump tables for protocol frame routing.
- **No channels on hot path**: `Gate` (tokio::Notify) replaces MPSC for drain signaling.

## License

MIT
