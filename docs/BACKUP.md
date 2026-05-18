# Arbitro Backup & Restore

This document covers how to back up an Arbitro broker's on-disk state
and what consistency guarantees you get from each approach.

## What lives on disk

When `ARBITRO_DATA_DIR` is set, the broker writes:

```
<data_dir>/
├── shards.toml            # M1 marker — sharding configuration
├── metadata.log           # CommandLog: stream/consumer create/delete records
└── shards/
    └── <N>/               # one directory per shard
        ├── 0000001.log    # immutable, sealed segment
        ├── 0000002.log    # immutable, sealed segment
        └── 0000003.log    # ACTIVE — has writes in flight
```

Sealed segments are **append-once, then read-only**. The only segment
that the broker mutates is the highest-numbered one in each shard
directory (the active segment).

## Online backup — tar while running

You can back up a live broker by tarring `data_dir/` while it serves
traffic, with one important caveat: the active segment in each shard
may have writes mid-flight at the moment `tar` reads it.

```bash
tar -czf arbitro-backup-$(date +%F).tar.gz "$ARBITRO_DATA_DIR"
```

What you get:

- **Sealed segments** — bit-exact, fully consistent. These are
  immutable once closed.
- **Active segment** — consistent up to the last `fsync()` that
  completed before tar opened the file. Any writes flushed *after*
  tar's read pointer passes that offset are lost from the backup but
  retained by the running broker.
- **`metadata.log`** — consistent up to the last `fsync()`. The
  CommandLog appends and `fsync`s synchronously on every metadata
  mutation, so all stream/consumer definitions visible to a client
  before the backup started are guaranteed to be in the tar.

After restore, the broker's recovery path rescans each segment for the
0xAF magic-byte and tolerates trailing partial writes. A backup taken
mid-publish therefore restores cleanly — you simply lose the not-yet-
fsynced tail of the active segment.

## Quiesced backup — drain first for bit-exact tails

If you need a guaranteed consistent snapshot (e.g. for migration to
new hardware), drain the broker first, then tar:

```bash
# 1. Drain producers — refuse new publishes upstream of the broker.
# 2. Wait for consumers to ack everything in flight.
# 3. Optionally purge streams that don't need to migrate:
arbitroctl purge-stream high-volume-events

# 4. Stop the broker cleanly so the active segment is sealed.
systemctl stop arbitro    # or `kill -TERM <pid>` and wait

# 5. tar — every segment is sealed, every byte is durable.
tar -czf arbitro-quiesced-$(date +%F).tar.gz "$ARBITRO_DATA_DIR"

# 6. Restart.
systemctl start arbitro
```

## Restore

```bash
# Stop the target broker.
systemctl stop arbitro

# Replace the data dir.
rm -rf "$ARBITRO_DATA_DIR"
tar -xzf arbitro-backup-2026-05-17.tar.gz -C /

# Start — the broker replays metadata.log and recovers segments.
systemctl start arbitro
```

The startup log will show one line per recovered stream:

```
INFO arbitro_server::server: stream ready stream=orders messages=18302 bytes=4823104
INFO arbitro_server::server: broker state ready streams=4 consumers=12 messages=18302 bytes=4823104
```

If the shard count of the running broker doesn't match `shards.toml`
in the backup, startup aborts with an `M1: shard_count mismatch`
error — set `ARBITRO_SHARDS` to match the recorded value or rebuild
the dataset with the new shard count.

## What backups do NOT cover

- **In-flight TCP frames.** Clients holding `publish_sync` futures
  during the snapshot will see their own outcome (Ok or
  Disconnected). Restoration replays only what the broker had
  fsynced before tar read the bytes.
- **TLS material.** Cert + key files referenced by
  `ARBITRO_TLS_CERT` / `ARBITRO_TLS_KEY` are typically outside the
  data dir; back them up separately.
- **Per-consumer cursor position for in-flight (unacked)
  deliveries.** The broker's redelivery wheel reconstructs these
  from the unacked set after restart — recovery is idempotent but
  the wheel timers reset from `now()`, not from the original
  delivery time.
