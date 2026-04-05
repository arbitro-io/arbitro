---
description: Mandatory rules for compiling and running benchmarks and tests
---

# BUILD & RUN — MANDATORY

All compilation and execution happens in WSL. No exceptions.

## Compile

```bash
wsl bash -lc "cd /mnt/d/.../arbitro && cargo bench --bench <name> --no-run 2>&1"
```

## Run

Copy the compiled binary to `/tmp/arbitro` and run from there. This avoids the 9P filesystem bridge (`/mnt/`) which is 2-10x slower and causes memory/TCP limitations.

```bash
wsl bash -lc "
  mkdir -p /tmp/arbitro &&
  cp -a target/release/deps/<bench-binary> /tmp/arbitro/ &&
  cd /tmp/arbitro &&
  timeout 120 ./<bench-binary> --bench 2>&1 | tee /tmp/bench.log
"
```

## Rules

1. **Always compile from `/mnt/`** — source lives on Windows, that's fine for compilation.
2. **Always run from `/tmp/arbitro`** — never run benchmarks or tests from `/mnt/`. Copy binary with `cp -a` first.
3. **Never use `rsync`** — banned. Only `cp -a`.
4. **Always `timeout 120`** — every execution must have a timeout.
5. **Always `tee /tmp/bench.log`** — log output for inspection.
6. **One bench at a time** — never run the full suite. Run specific bench groups.
7. **Never run in background** — benches run in foreground so hangs are detected.
