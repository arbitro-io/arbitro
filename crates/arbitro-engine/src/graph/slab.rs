//! TypedSlab<T> — generational arena with O(1) insert/get/remove.
//!
//! Level 2 — depends only on `types`, `error`.

use crate::error::{EngineError, EngineResult, ErrorCode};
use crate::types::SlabKey;

/// Entry in the slab: either occupied with a value or vacant (part of free list).
enum SlabEntry<T> {
    Occupied(T),
    /// Points to the next free slot (or u32::MAX if end of free list).
    Vacant(u32),
}

/// A generational arena providing O(1) insert, get, and remove.
///
/// Each slot has a generation counter to prevent ABA problems: after removal,
/// the generation increments, so stale `SlabKey`s are detected on access.
pub struct TypedSlab<T> {
    entries: Vec<SlabEntry<T>>,
    generations: Vec<u32>,
    free_head: u32,
    len: u32,
}

impl<T> TypedSlab<T> {
    /// Create an empty slab.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            generations: Vec::new(),
            free_head: u32::MAX,
            len: 0,
        }
    }

    /// Create a slab with pre-allocated capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            entries: Vec::with_capacity(cap),
            generations: Vec::with_capacity(cap),
            free_head: u32::MAX,
            len: 0,
        }
    }

    /// Number of active (occupied) entries.
    #[inline]
    pub fn len(&self) -> u32 { self.len }

    /// Whether the slab is empty.
    #[inline]
    pub fn is_empty(&self) -> bool { self.len == 0 }

    /// Total allocated slots (occupied + vacant).
    #[inline]
    pub fn capacity(&self) -> usize { self.entries.len() }

    /// Insert a value, returning its `SlabKey`.
    ///
    /// O(1): pops from free list or appends.
    pub fn insert(&mut self, value: T) -> SlabKey {
        if self.free_head != u32::MAX {
            let idx = self.free_head as usize;
            match self.entries[idx] {
                SlabEntry::Vacant(next) => {
                    self.free_head = next;
                    self.entries[idx] = SlabEntry::Occupied(value);
                    self.len += 1;
                    SlabKey::new(idx as u32, self.generations[idx])
                }
                SlabEntry::Occupied(_) => unreachable!("free list points to occupied slot"),
            }
        } else {
            let idx = self.entries.len() as u32;
            self.entries.push(SlabEntry::Occupied(value));
            self.generations.push(0);
            self.len += 1;
            SlabKey::new(idx, 0)
        }
    }

    /// Get a shared reference by key. Returns error on stale generation.
    ///
    /// O(1): array index + generation check.
    #[inline]
    pub fn get(&self, key: SlabKey) -> EngineResult<&T> {
        let idx = key.index as usize;
        if idx >= self.entries.len() {
            return Err(EngineError::StaleKey {
                code: ErrorCode::SlotVacant,
                entity: std::any::type_name::<T>(),
                index: key.index,
                expected_gen: key.generation,
                actual_gen: 0,
            });
        }
        if self.generations[idx] != key.generation {
            return Err(EngineError::StaleKey {
                code: ErrorCode::StaleGeneration,
                entity: std::any::type_name::<T>(),
                index: key.index,
                expected_gen: key.generation,
                actual_gen: self.generations[idx],
            });
        }
        match &self.entries[idx] {
            SlabEntry::Occupied(v) => Ok(v),
            SlabEntry::Vacant(_) => Err(EngineError::StaleKey {
                code: ErrorCode::SlotVacant,
                entity: std::any::type_name::<T>(),
                index: key.index,
                expected_gen: key.generation,
                actual_gen: self.generations[idx],
            }),
        }
    }

    /// Get a mutable reference by key.
    ///
    /// O(1): array index + generation check.
    #[inline]
    pub fn get_mut(&mut self, key: SlabKey) -> EngineResult<&mut T> {
        let idx = key.index as usize;
        if idx >= self.entries.len() {
            return Err(EngineError::StaleKey {
                code: ErrorCode::SlotVacant,
                entity: std::any::type_name::<T>(),
                index: key.index,
                expected_gen: key.generation,
                actual_gen: 0,
            });
        }
        if self.generations[idx] != key.generation {
            return Err(EngineError::StaleKey {
                code: ErrorCode::StaleGeneration,
                entity: std::any::type_name::<T>(),
                index: key.index,
                expected_gen: key.generation,
                actual_gen: self.generations[idx],
            });
        }
        match &mut self.entries[idx] {
            SlabEntry::Occupied(v) => Ok(v),
            SlabEntry::Vacant(_) => Err(EngineError::StaleKey {
                code: ErrorCode::SlotVacant,
                entity: std::any::type_name::<T>(),
                index: key.index,
                expected_gen: key.generation,
                actual_gen: self.generations[idx],
            }),
        }
    }

    /// Remove an entry by key, returning the value.
    ///
    /// O(1): push to free list, bump generation.
    pub fn remove(&mut self, key: SlabKey) -> EngineResult<T> {
        let idx = key.index as usize;
        if idx >= self.entries.len() {
            return Err(EngineError::StaleKey {
                code: ErrorCode::SlotVacant,
                entity: std::any::type_name::<T>(),
                index: key.index,
                expected_gen: key.generation,
                actual_gen: 0,
            });
        }
        if self.generations[idx] != key.generation {
            return Err(EngineError::StaleKey {
                code: ErrorCode::StaleGeneration,
                entity: std::any::type_name::<T>(),
                index: key.index,
                expected_gen: key.generation,
                actual_gen: self.generations[idx],
            });
        }

        // Swap in a Vacant entry, bump generation
        let old = std::mem::replace(&mut self.entries[idx], SlabEntry::Vacant(self.free_head));
        match old {
            SlabEntry::Occupied(v) => {
                self.generations[idx] = self.generations[idx].wrapping_add(1);
                self.free_head = idx as u32;
                self.len -= 1;
                Ok(v)
            }
            SlabEntry::Vacant(_) => {
                // Put back
                self.entries[idx] = old;
                Err(EngineError::StaleKey {
                    code: ErrorCode::SlotVacant,
                    entity: std::any::type_name::<T>(),
                    index: key.index,
                    expected_gen: key.generation,
                    actual_gen: self.generations[idx],
                })
            }
        }
    }

    /// Check if a key is valid (correct generation, occupied).
    #[inline]
    pub fn contains(&self, key: SlabKey) -> bool {
        let idx = key.index as usize;
        if idx >= self.entries.len() { return false; }
        if self.generations[idx] != key.generation { return false; }
        matches!(&self.entries[idx], SlabEntry::Occupied(_))
    }

    /// Iterate over all occupied entries with their keys.
    pub fn iter(&self) -> impl Iterator<Item = (SlabKey, &T)> {
        self.entries.iter().enumerate().filter_map(|(i, entry)| {
            if let SlabEntry::Occupied(v) = entry {
                Some((SlabKey::new(i as u32, self.generations[i]), v))
            } else {
                None
            }
        })
    }

    /// Clear all entries. Resets generations so all existing keys become stale.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.generations.clear();
        self.free_head = u32::MAX;
        self.len = 0;
    }
}

impl<T> Default for TypedSlab<T> {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut slab = TypedSlab::new();
        let k = slab.insert(42u64);
        assert_eq!(*slab.get(k).unwrap(), 42);
        assert_eq!(slab.len(), 1);
    }

    #[test]
    fn remove_and_reuse() {
        let mut slab = TypedSlab::new();
        let k1 = slab.insert("a");
        let _k2 = slab.insert("b");

        let v = slab.remove(k1).unwrap();
        assert_eq!(v, "a");
        assert_eq!(slab.len(), 1);

        // Free slot reused
        let k3 = slab.insert("c");
        assert_eq!(k3.index, k1.index);
        // Generation bumped
        assert_eq!(k3.generation, k1.generation + 1);
        assert_eq!(*slab.get(k3).unwrap(), "c");
    }

    #[test]
    fn stale_generation_detected() {
        let mut slab = TypedSlab::new();
        let k1 = slab.insert(100u32);
        slab.remove(k1).unwrap();
        let _k2 = slab.insert(200u32);

        // Old key has stale generation
        let err = slab.get(k1).unwrap_err();
        assert_eq!(err.code(), ErrorCode::StaleGeneration);
    }

    #[test]
    fn double_remove_fails() {
        let mut slab = TypedSlab::new();
        let k = slab.insert("x");
        slab.remove(k).unwrap();
        assert!(slab.remove(k).is_err());
    }

    #[test]
    fn contains_check() {
        let mut slab = TypedSlab::new();
        let k = slab.insert(1);
        assert!(slab.contains(k));
        slab.remove(k).unwrap();
        assert!(!slab.contains(k));
    }

    #[test]
    fn iteration() {
        let mut slab = TypedSlab::new();
        let k1 = slab.insert(10);
        let _k2 = slab.insert(20);
        let _k3 = slab.insert(30);
        slab.remove(k1).unwrap();

        let items: Vec<_> = slab.iter().map(|(_, &v)| v).collect();
        assert_eq!(items.len(), 2);
        assert!(items.contains(&20));
        assert!(items.contains(&30));
    }

    #[test]
    fn with_capacity() {
        let slab = TypedSlab::<u32>::with_capacity(64);
        assert!(slab.capacity() == 0); // no entries yet, but Vec has capacity
        assert!(slab.is_empty());
    }

    #[test]
    fn many_inserts_and_removes() {
        let mut slab = TypedSlab::new();
        let mut keys = Vec::new();
        for i in 0..1000u32 {
            keys.push(slab.insert(i));
        }
        assert_eq!(slab.len(), 1000);

        // Remove even indices
        for i in (0..1000).step_by(2) {
            slab.remove(keys[i]).unwrap();
        }
        assert_eq!(slab.len(), 500);

        // Reinsert — should reuse freed slots
        for i in 0..500u32 {
            slab.insert(i + 2000);
        }
        assert_eq!(slab.len(), 1000);
    }

    #[test]
    fn get_mut_works() {
        let mut slab = TypedSlab::new();
        let k = slab.insert(10u32);
        *slab.get_mut(k).unwrap() = 20;
        assert_eq!(*slab.get(k).unwrap(), 20);
    }
}
