# arbitro

## Rules
Read all `.agents/rules/*.md` before writing or modifying any code. Rules are INVIOLABLE.

## Skills
Read `.agents/skills/*.md` for available skills.

## Testing
Read `.agents/rules/testing.md` before any `cargo test` or `cargo bench`.

## Performance
Read `.agents/rules/performance.md` before writing hot-path code or modifying publish/ack/deliver paths.

## Architecture
Read `.agents/rules/arch-boundaries.md` before modifying module structure, crossing crate boundaries, or importing types between crates. This file defines which crate owns which feature — never duplicate.

## Engine Contract
Read `.agents/rules/engine-contract.md` before adding new commands, modifying the shard worker, or calling ArbitroEngine methods.

## Roles
Read `.agents/rules/roles.md` before adding, moving, or modifying any handler in `shard.rs`. Defines the hot/cold path boundaries and which role (publisher, accumulator, acker, drainer, admin, seeder) owns which primitive. INVIOLABLE.

## Concurrency
Read `.agents/rules/concurrency.md` before touching channels, thread spawning, shutdown, or async boundaries.
