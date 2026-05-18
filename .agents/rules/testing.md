---
trigger: always_on
description: Mandatory rules for compiling and running benchmarks and tests
---

# BUILD, TEST & BENCH — MANDATORY

All builds, tests, and benchmarks must run inside WSL.

Source code may live under `/mnt/`, but performance execution must not. Windows-mounted paths use the 9P bridge and can distort TCP, disk, memory, and latency results.

## Compile

Compile from the project source directory when needed:

```bash
wsl bash -lc "cd /mnt/d/.../arbitro && cargo bench --bench <name> --no-run 2>&1"
Run Performance Benchmarks

For TCP, disk, memory, or latency benchmarks, compile first, move/copy the compiled benchmark executable to native WSL storage, then run it from /tmp/arbitro.

wsl bash -lc "
  mkdir -p /tmp/arbitro &&
  cp -a <compiled-bench-executable> /tmp/arbitro/ &&
  cd /tmp/arbitro &&
  timeout 120 ./<compiled-bench-executable-name> --bench 2>&1 | tee /tmp/bench.log
"
Rules
WSL is mandatory for build, test, bench, and execution.
Source under /mnt/ is allowed for compilation.
Never run performance benchmarks from /mnt/.
Performance benchmarks must run from /tmp/arbitro.
Move/copy the compiled benchmark executable before running it.
Use cp -a, never rsync.
Always use timeout 120.
Always log with tee /tmp/bench.log.
Run one benchmark at a time.
Never run benchmarks in background.

## Feature flags — never bench with `--features lifecycle_trace`

F30: the `lifecycle_trace!` macro in `arbitro-server/src/lifecycle_trace.rs`
takes a global Mutex on every hot-path call site when the feature is
**on** (`record()` writes to a shared trace buffer). Compiles to a
zero-instruction no-op when **off**. Enabling it costs ~50% of throughput
on the publish/ack hot path.

Bench rule: every `cargo bench` invocation MUST run without the
`lifecycle_trace` feature. The feature is for one-shot end-to-end
profiling of a single message — not steady-state measurement.

```bash
# CORRECT — feature off
cargo bench --bench throughput --no-run

# WRONG — locks every hot-path call site, numbers are useless
cargo bench --bench throughput --no-run --features lifecycle_trace
```

If you need lifecycle traces, run them as a separate one-shot test or
unit test, never as a benchmark.
