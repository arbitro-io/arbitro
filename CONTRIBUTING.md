# Contributing to arbitro

## Workspace layout

`arbitro` is part of a multi-repo workspace. The Cargo workspace at the
root of this repo depends on a sibling crate `arbitro-kit` that lives
**outside** this repository:

```
arbitro-io/                       # parent directory you must create
├── arbitro/                      # this repo
│   ├── Cargo.toml                # workspace root
│   ├── crates/
│   │   ├── arbitro-client-tokio
│   │   ├── arbitro-common
│   │   ├── arbitro-engine
│   │   ├── arbitro-e2e
│   │   ├── arbitro-proto
│   │   ├── arbitro-server
│   │   └── arbitro-store
│   └── ...
└── arbitro-kit/                  # sibling — pulled in via `path = "../arbitro-kit"`
    └── ...
```

`arbitro-kit` is referenced from `Cargo.toml` as `path = "../arbitro-kit"`.
Without it checked out as a sibling, `cargo build` / `cargo test` fail
with:

```
error: failed to read `.../arbitro-kit/Cargo.toml`
```

### Getting both repos

```bash
mkdir arbitro-io && cd arbitro-io
git clone https://github.com/zenozaga/arbitro-io       arbitro
git clone https://github.com/zenozaga/arbitro-kit      arbitro-kit
cd arbitro
cargo build --workspace
```

CI handles this with a second `actions/checkout@v4` step pointing at
`arbitro-kit`. Local contributors must do the equivalent manually.

## Building and testing

All builds, tests, and benchmarks must run **inside WSL** on Windows
(per `.agents/rules/testing.md`). Source can live under `/mnt/` for
compilation, but performance benchmarks must run from `/tmp/arbitro/`
because the 9P bridge distorts TCP / disk / memory results.

```bash
# Build
cargo build --workspace --release

# Lib tests
cargo test --workspace --lib

# E2E (TCP) tests — must run serially because each test binds a port
cargo test -p arbitro-e2e -- --test-threads=1

# Bench (per testing.md)
wsl bash -lc "cargo bench --bench <name> --no-run"
wsl bash -lc "cp -a target/release/deps/<name>-* /tmp/arbitro/ && \
  cd /tmp/arbitro && timeout 120 ./<name>-* --bench 2>&1 | tee /tmp/bench.log"
```

## Inviolable rules

Before touching code, read the files in `.agents/rules/` — these are
non-negotiable architectural constraints (zero-copy on hot path, dense
IDs use `Vec<T>` not HashMap, no async in `arbitro-engine`, etc.). The
engine crate has its own stricter rules under
`crates/arbitro-engine/.agent/rules/`.

## Commits

- One concern per commit. Use the `feat:` / `fix:` / `perf:` / `docs:`
  / `test:` / `chore:` prefix.
- Reference TODO IDs (B#, H#, M#, L#, T#) and OPTIMIZATION IDs (F#, S#)
  when closing items.
- Co-author trailer is welcome but not required.
- Never `--no-verify`. Fix the hook, don't bypass it.
