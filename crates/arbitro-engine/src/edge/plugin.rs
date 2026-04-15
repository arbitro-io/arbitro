//! PluginEdge trait — allows plugins to register custom edge indexes
//! that auto-cleanup when entities are removed via notify_removed.
//!
//! Level 3 — depends on `types`. Does NOT know about specific plugins.

use crate::types::SlabKey;

/// Trait for plugin-provided custom edge indexes.
///
/// When an entity is removed from the graph, this trait allows plugins to
/// clean up their custom shortcut indexes. Note: plugin-edge notification is
/// not currently wired through `BuiltinEdges` — the trait is retained for
/// future integration.
///
/// Example: a metrics plugin might track PendingNode counts per subject.
/// When a PendingNode is removed, the plugin's edge receives the notification
/// and decrements its counter.
pub trait PluginEdge: Send + 'static {
    /// Called when an entity is removed from the graph.
    ///
    /// `entity_type` is the `TypeId` of the removed entity (e.g. PendingNode).
    /// `key` is the slab key of the removed entity.
    fn on_entity_removed(&mut self, entity_type: std::any::TypeId, key: SlabKey);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::TypeId;

    struct TestPluginEdge {
        removed_count: u32,
    }

    impl PluginEdge for TestPluginEdge {
        fn on_entity_removed(&mut self, _entity_type: TypeId, _key: SlabKey) {
            self.removed_count += 1;
        }
    }

    #[test]
    fn plugin_edge_notification() {
        let mut edge = TestPluginEdge { removed_count: 0 };
        edge.on_entity_removed(TypeId::of::<u32>(), SlabKey::new(0, 0));
        edge.on_entity_removed(TypeId::of::<u32>(), SlabKey::new(1, 0));
        assert_eq!(edge.removed_count, 2);
    }
}
