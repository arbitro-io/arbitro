# Durability Guarantees

## Overview

Arbitro provides two persistence tiers. The guarantees differ between
**metadata** (stream/consumer definitions) and **message payloads**.

## Metadata durability

When `ARBITRO_DATA_DIR` is set:

| Operation | Guarantee |
|-----------|-----------|
| CreateStream | `fdatasync` before RepOk is sent |
| DeleteStream | `fdatasync` before RepOk is sent |
| CreateConsumer | `fdatasync` before RepOk is sent |
| DeleteConsumer | `fdatasync` before RepOk is sent |

The command log uses CRC32 per record and tolerates trailing truncation
on crash recovery (incomplete final write is skipped).

**Without `ARBITRO_DATA_DIR`**: all state is in-memory. A restart loses
everything — streams, consumers, and messages.

## Message durability

Messages are stored in an in-memory ring (`MemoryStore`) per stream.
**There is no message-level disk persistence.** A server restart or OOM
kill loses all buffered messages.

Implications:
- Publish acknowledgment (`RepOk`) means "engine accepted the message
  into the memory store" — NOT "message is durable on disk."
- Consumers that disconnect and reconnect within the server's lifetime
  can resume from their last ack position (the ring retains messages
  until eviction).
- After a server restart, message history is empty regardless of
  `ARBITRO_DATA_DIR`.

## Consumer delivery semantics

| Mode | Guarantee |
|------|-----------|
| At-most-once | Message delivered once; no redelivery on ack timeout |
| At-least-once | Message redelivered after ack timeout until acked or max retries |

The ack timeout and max retries are per-consumer configuration set at
creation time.

## Planned improvements

- **Configurable fsync policy** (`ARBITRO_FSYNC_POLICY`): batch
  metadata writes for higher throughput at the cost of a small
  durability window.
- **Message persistence** (segment-based append log): durable message
  storage with configurable retention.

## Recommendations for production

1. Always set `ARBITRO_DATA_DIR` — metadata survives restarts.
2. Deploy with a process supervisor (systemd, K8s) that restarts the
   server quickly after crashes.
3. Treat arbitro as a **hot-path transport layer**, not a durable queue.
   If you need cross-restart message durability, persist at the
   publisher or use an external durable store as a fallback.
4. Monitor the metrics log (`ARBITRO_METRICS_INTERVAL`) for memory
   pressure — evicted messages are gone.
