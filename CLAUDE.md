# arbitro

## Rules
Read all `.agent/rules/*.md` before writing or modifying any code. Rules are INVIOLABLE.

## Skills
Read `.agent/skills/*.md` for available skills.

## Testing
Read `.agent/rules/testing.md` before any `cargo test` or `cargo bench`.

## Performance
Read `.agent/rules/performance.md` before writing hot-path code or modifying publish/ack/deliver paths.

## Architecture
Read `.agent/rules/arch-boundaries.md` before modifying module structure, crossing crate boundaries, or importing types between crates. This file defines which crate owns which feature — never duplicate.

## Engine Contract
Read `.agent/rules/engine-contract.md` before adding new commands, modifying the shard worker, or calling ArbitroEngine methods.

## Roles
Read `.agent/rules/roles.md` before adding, moving, or modifying any handler in `shard.rs`. Defines the hot/cold path boundaries and which role (publisher, accumulator, acker, drainer, admin, seeder) owns which primitive. INVIOLABLE.

## Concurrency
Read `.agent/rules/concurrency.md` before touching channels, thread spawning, shutdown, or async boundaries.
