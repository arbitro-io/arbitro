//! Edge storage primitives — typed secondary indexes over the graph.
//!
//! Level 3 — depends on `types`, `error`. Does NOT know about specific edges.
//!
//! Edges are shortcut indexes: given a parent ID, find all child IDs in O(1).
//! The graph slabs are the source of truth; edges are derived state.
//!
//! The **built-in** edges live in [`builtin::BuiltinEdges`] — a concrete
//! struct with direct fields for every index the engine needs. Field access
//! is monomorphic: no HashMap lookup, no `TypeId` hash, no `Box<dyn Any>`
//! downcast. The previous `EdgeRegistry` has been deleted (`performance.md` §11
//! — slab over HashMap for hot-path lookups).
//!
//! Storage primitives [`HashEdge`] and [`UniqueEdge`] live in [`storage`];
//! [`BuiltinEdges`] composes them as fields.

pub mod builtin;
pub mod pending_edge;
pub mod plugin;
pub mod storage;

pub use storage::{ConsumerSeqEdge, HashEdge, UniqueEdge};
pub use pending_edge::PendingEdge;
pub use builtin::BuiltinEdges;
