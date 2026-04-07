# Arbitro

> [!WARNING]
> **Project Status: In Development**. Arbitro is currently in active development. APIs and wire protocols are subject to change. Not yet recommended for production use without prior testing.

**Arbitro** is a Stateful Flow Broker designed for ultra-high concurrency and sub-microsecond predictability. It isn't just a message pipe; it's a reactive engine that uses KV state to control message flow with hardware-level efficiency.

Built in Rust with a **Zero-Allocation, Zero-Copy** architecture, Arbitro follows the principle of **Hardware Sympathy** to maximize L1/L2 cache locality and eliminate heap fragmentation.

## Key Features

- **Massive Subject Partitioning**: Independently control flow for millions of unique subjects with zero configuration.
- **Ultra-High Throughput**: 14.2M+ messages per second ingestion on commodity hardware.
- **Predictable Latency**: Sub-microsecond internal dispatch with deterministic performance.
- **Crash-Safe Persistence**: Zero-Copy indexing with **Magic Byte (0xAF)** validation to guarantee recovery after abrupt process failure (`SIGKILL`).
- **Reactive Model**: Efficient non-blocking delivery for both callback-based and pull-based consumers.

## Performance (E2E Throughput)

Arbitro is built for **Hardware Sympathy**. These benchmarks represent the full end-to-end cycle (TCP + Protocol + Engine) on a single server instance (WSL, 64B payload, Local Memory Persistence).

| Mode | Throughput Range (1K - 1M msgs) | Latency / Unit |
| :--- | :--- | :--- |
| **Publish (Ingest)** | **6.3M — 14.2M msg/s** | ~70ns per msg |
| **Cycle Fire-and-Forget** | **6.5M — 15.1M msg/s** | ~66ns per msg |
| **Cycle Explicit Ack** | **2.1M — 2.52M msg/s** | ~390ns per msg |
| **VIP Subject Isolation** | **Independent of Noise** | **31ns (L3) — 84µs (Network)** |

## Endurance & Stability (1-Minute Sustained)

Arbitro isn't just fast in bursts; it's designed for **Thermal and Memory Stability** under zero-truce pressure. These metrics are captured by the engine's **Integrated Process Radar** (`/proc/self`).

| Scenario | Throughput (Avg) | CPU Load | RAM (RSS) | Stability |
| :--- | :--- | :--- | :--- | :--- |
| **Memory Hot-Path** | **~2.8M msg/s** | **~10.8%** | **~2.99 GB** | **O(1) Hybrid Index** |
| **Disk Persistence (Tolerant)** | **~35,400 msg/s** | **~3.2%** | **~120 MB** | **Crash-Safe (AF)** |
| **Chaos Resilience** | **10s Stress** | **Isolated PIDs** | **Stable** | **100% Recovery Proof** |

> [!TIP]
> **Zero-Allocation Telemetry**: The internal metrics radar reads directly from the kernel interface, ensuring that monitoring the engine doesn't pollute the engine's own Performance Profile.

> [!IMPORTANT]
> **Performance Consistency**: Introducing *Dynamic Subject Isolation* with hashing and state recycling resulted in **0% regression** in global throughput.

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

## Power Feature: Dynamic Subject Isolation

Arbitro's unique power isn't just limiting a stream; it's **Stateful Flow Partitioning**. With a single wildcard rule, Arbitro dynamically isolates credits for an infinite number of unique subjects matching your patterns.

### The "Freemium" Isolation Logic
Traditional brokers apply limits at the wildcard level. Arbitro applies them to the **resolved subject**. This creates an **independent credit pool** for every unique entity in parallel.

> [!NOTE]
> **Global Ceiling**: The subject limits are partitions of the **Global Credit Limit** (`max_inflight`). The global limit acts as a hard ceiling for the consumer's total pending ACKs, while subject limits ensure fair distribution within that ceiling.

```text
Active Subject Slots (CreditMap - Atomic Hashing)
-------------------------------------------------------------------
[Slot A] orders.freemium.u_1  | [ 1 / 1  ] | (BLOCKED - 1 at a time)
[Slot B] orders.freemium.u_2  | [ 0 / 1  ] | (FLOWING - Independent!)
[Slot C] orders.basic.u_3     | [ 5 / 10 ] | (FLOWING - 10 allowed)
[Slot D] orders.premium.u_4   | [ 1 / 30 ] | (FLOWING - 30 allowed)

Outcome: A freemium user saturating their credit does NOT impact any other user, 
even if they share the same rule. 1 Rule -> 1,000,000+ Subjects Managed.
```

### Multi-Tenant Example (1 rule -> Many Subjects)
Govern a massive tenant base with just 3 hierarchical policies.

```rust
let consumer_cfg = ConsumerConfig::new(b"gateway", b"ORDERS")
    .filter(b"orders.>")
    .ack_policy(AckPolicy::Explicit)
    .max_inflight(10000)
    // 30 credits for EACH unique premium user
    .subject_limit(b"orders.premium.>", 30)
    // 10 credits for EACH unique basic user
    .subject_limit(b"orders.basic.>", 10)
    // ONLY 1 credit for EACH unique freemium user (1 at a time)
    .subject_limit(b"orders.freemium.>", 1)
    .build();
```

## Usage Paradigms: Choosing Your Flow

Arbitro offers multiple subscription models tailored for performance. Whether you need reactive low-latency closures or heavy-duty pull workers, the engine is optimized for zero-copy delivery.

### 1. Reactive Callbacks (Zero-Latency Flow)
The most efficient way to process messages. Closures are executed directly by the engine for ultra-low latency.

```rust
// Reactive, non-blocking flow
let _handle = consumer.subscribe_callback(Some(b"orders.premium.>"), move |msg| {
    println!("VIP logic fired for subject: {:?}", msg.subject);
    msg.ack(); // Instant credit release
}).await?;
```

### 2. Massive Fanout (Optimized Delivery)
Arbitro ensures that if you have 100+ local subscribers on the same stream, delivery is handled with extreme efficiency, minimizing CPU overhead and network noise.

```rust
// 100 subscribers, minimal CPU noise
for i in 0..100 {
    consumer.subscribe_callback(None, move |msg| {
        // Parallel reactive processing
    }).await?;
}
```

### 3. Manual Pull / Fetch (Total Control)
For workers that need to manage their own pace or perform batching. Use the async iterator pattern to pull messages when the worker is ready.

```rust
let mut subscription = consumer.subscribe(Some(b"orders.basic.>")).await?;

while let Some(msg) = subscription.next().await {
    // Process at your own speed
    msg.ack();
}
```

### 4. Atomic Publish (High-Density Ingest)
Ingest millions of messages with hardware-level throughput.

```rust
// Single fire-and-forget ingestion
client.publish(b"ORDERS", b"orders.freemium.u1", payload).await?;

// Or use high-density batches for 14.2M msg/s throughput
client.publish_batch(b"ORDERS", &[
    (b"orders.premium.u1", &payload),
    (b"orders.premium.u2", &payload),
]).await?;
```

| Variable | Default | Description |
|----------|---------|-------------|
| `ARBITRO_LISTEN` | `0.0.0.0:9898` | TCP listen address (**Default Port: 9898**) |
| `ARBITRO_MAX_CONNECTIONS` | `10000` | Max concurrent TCP connections |
| `ARBITRO_WRITE_BUFFER_CAP` | `8192` | Write channel capacity per connection |
| `ARBITRO_IDLE_TIMEOUT` | `300` | Idle timeout in seconds |
| `ARBITRO_KEEPALIVE_INTERVAL` | `30` | Keepalive ping interval in seconds |


## Roadmap

### Phase 1: Core Engine (Completed)
- [x] **Zero-Copy Hot Path**: Optimized frame dispatch and delivery.
- [x] **Dynamic Subject Isolation**: Intelligent credit partitioning per entity.
- [x] **Atomic State Management**: Efficient resource cleanup after processing.
- [x] **High-Speed Storage**: Optimized linear ingestion store.

### Phase 2: Persistence & Connectivity (Validated)
- [x] **Disk Persistence**: High-performance AEP/NVMe storage backend (**TolerantStore**).
- [x] **Crash-Safe Journaling**: **Magic Byte (0xAF)** validation for recovery after `SIGKILL`.
- [x] **Sync Acks**: Guaranteed delivery points with explicit `AckSync` protocol.
- [ ] **Subject Scavenging**: Automatic expiration of inactive subject slots.
- [ ] **Multi-Language Clients**: Official TypeScript (`arbitro-ts`) and Go (`arbitro-go`) support.

### Phase 3: Observability & Scale (Planned)
- [ ] **Prometheus Integration**: Native metrics for subject pressure and throughput.
- [ ] **Clustering (Raft)**: Distributed consensus for stream and consumer state.
- [ ] **Adaptive Flow Control**: Machine learning-assisted subject prioritization.

## License

MIT
