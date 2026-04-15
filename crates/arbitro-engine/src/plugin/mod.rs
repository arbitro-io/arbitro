//! Core subsystems historically exposed as "plugins".
//!
//! Level 4 — depends on Level 0-3.
//!
//! Despite the name, the three types in this module (`CreditPlugin`,
//! `EventBus`, `Scheduler`) are **not** dynamically registered. They live
//! as direct fields on [`EngineContext`] and are accessed monomorphically
//! (`ctx.credit`, `ctx.events`, `ctx.scheduler`). The previous
//! `PluginRegistry` — a TypeId-keyed `HashMap<TypeId, Box<dyn Any>>` —
//! has been deleted: hot-path access in `claim` and `ack` cannot afford
//! a hash lookup plus `Box<dyn Any>` downcast (`performance.md` §11,
//! `code-anti-patterns.md` §HOT PATH BANS).
//!
//! The module name is kept to avoid churning every import in the tree;
//! conceptually these are "core subsystems", not plugins.

pub mod event_bus;
pub mod scheduler;
pub mod credit;
