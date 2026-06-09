# Benchmarks

All benchmarks run on the same machine with the broker on localhost. Payload: 64 bytes. Journal: memory mode.

## Broker Throughput (Rust)

Measured with `cargo bench --bench throughput`. 25,000 messages per iteration, 5 iterations averaged.

### Publish (single message, sync)

| Config | Avg time | Throughput | Per-conn |
|--------|----------|------------|----------|
| 1 conn / 1 stream | 26.07 ms | 958,860 msg/s | 958,860 msg/s |
| 2 conn / 2 stream | 18.92 ms | 1,321,366 msg/s | 660,683 msg/s |
| 4 conn / 4 stream | 11.83 ms | 2,112,646 msg/s | 528,162 msg/s |
| 8 conn / 8 stream | 11.09 ms | 2,253,560 msg/s | 281,695 msg/s |
| 16 conn / 16 stream | 17.11 ms | 1,460,287 msg/s | 91,268 msg/s |

### Publish (batch=256)

| Config | Avg time | Throughput | Per-conn |
|--------|----------|------------|----------|
| 1 conn / 1 stream | 1.09 ms | 22,922,741 msg/s | 22,922,741 msg/s |
| 2 conn / 2 stream | 586.44 us | 42,630,107 msg/s | 21,315,054 msg/s |
| 4 conn / 4 stream | 420.96 us | 59,388,065 msg/s | 14,847,016 msg/s |
| 8 conn / 8 stream | 299.40 us | 83,500,334 msg/s | 10,437,542 msg/s |
| 16 conn / 16 stream | 524.24 us | 47,672,822 msg/s | 2,979,551 msg/s |

### Publish (batch=256, sync / server-confirmed)

| Config | Avg time | Throughput | Per-conn |
|--------|----------|------------|----------|
| 1 conn / 1 stream | 11.98 ms | 2,086,808 msg/s | 2,086,808 msg/s |
| 2 conn / 2 stream | 5.12 ms | 4,879,706 msg/s | 2,439,853 msg/s |
| 4 conn / 4 stream | 2.74 ms | 9,130,686 msg/s | 2,282,671 msg/s |
| 8 conn / 8 stream | 2.37 ms | 10,528,532 msg/s | 1,316,067 msg/s |
| 16 conn / 16 stream | 2.26 ms | 11,068,790 msg/s | 691,799 msg/s |

### Replay (500,000 messages pre-loaded, DeliverPolicy::All)

| Config | Avg time | Throughput |
|--------|----------|------------|
| 1 conn / 1 stream / 500K msgs | 343.54 ms | 1,455,429 msg/s |

### Fanout Replay (500,000 msgs, 3 clients x 3 consumers)

| Config | Avg time | Throughput | Per-consumer |
|--------|----------|------------|--------------|
| 3 cli x 3 cons / 500K msgs | 2.47 s | 1,823,857 msg/s | 202,651 msg/s |

## Go Client

Measured with `go test -bench=. -benchmem -tags=integration`. i9-12900K, 24 threads, 64-byte payload.

| Benchmark | ns/op | MB/s | msgs/s | allocs/op |
|-----------|------:|-----:|--------:|----------:|
| PublishSync | 33,735 | 3.8 | 30K | 7 |
| PublishAsync | 398 | 322 | 2.5M | 2 |
| PublishFireAndForget | 493 | 130 | 2.0M | 1 |
| PublishBatchAsync (x256) | 33,508 | 489 | 7.7M | 260 |
| ParallelFireAndForget (24 cores) | 391 | 164 | 2.6M | 1 |
