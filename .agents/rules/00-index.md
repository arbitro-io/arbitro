# Rules Index — read before modifying arbitro/

## Server rules (`arbitro/.agents/rules/`)

| File | Contains |
|------|----------|
| `arch-boundaries.md` | Crate DAG, module ownership, who depends on whom |
| `concurrency.md` | Shard model, Gate/park, channel patterns, shutdown protocol |
| `engine-contract.md` | Server-engine API contract, allowed calls, result copying |
| `features-invariants.md` | Identity model, feature matrix, per-feature invariants |
| `performance.md` | Dense ID storage, zero-copy, cache-line, lock discipline |
| `roles.md` | Shard worker roles (Publisher/Drainer/Acker/Admin), ownership |
| `testing.md` | WSL only, timeout 120, copy binaries to /tmp, no lifecycle_trace |

## Engine rules (`arbitro/crates/arbitro-engine/.agent/rules/`)

| File | Contains |
|------|----------|
| `arch-modules.md` | Module DAG (8 levels), max 400 LOC/file, 60 LOC/fn |
| `arch-structs.md` | Newtypes, 64B alignment, repr(C), power-of-2 sizing |
| `code-anti-patterns.md` | Banned hot-path patterns, banned deps, code style bans |
| `code-concurrency.md` | Engine is &mut self, NO locks/async/threads/I-O inside |
| `code-hot-cold-path.md` | Hot = 0 alloc/0 lock/0 dispatch; budget table per-op |
| `code-zero-copy.md` | zerocopy wire codec, ScratchReply, 3-copy budget |
| `performance.md` | Vec for dense, HashMap+foldhash for sparse, O(1) targets |
| `testing.md` | Build in WSL from /mnt/d, NEVER rsync source, max 1000 msgs |

## Quick constraints (always apply)

- Engine: pure sync `&mut self`, no async, no I/O, no allocs on hot path
- Hot path: 0 allocations, 0 locks, 0 virtual dispatch, 0 syscalls
- IDs: dense → Vec, sparse → HashMap+foldhash
- Testing: WSL, timeout 120, one bench at a time, foreground
- Workspace: only modify inside `arbitro-io/`, docs in English
- Never: spin loops (use Gate), rsync source, `--features lifecycle_trace` in bench
