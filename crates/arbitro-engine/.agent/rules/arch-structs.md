---
description: Struct design rules — newtypes, cache-line discipline, hot/cold split, power-of-2 sizing, repr(C)
---

# STRUCT DESIGN RULES

---

## 1. NEWTYPES OVER PRIMITIVES

Every domain scalar that could be confused with another must be a newtype.

```rust
// ✅ Newtypes — mix-up caught at compile time
pub struct PendingId(pub u32);
pub struct ConsumerId(pub u32);
pub struct QueueId(pub u32);
pub struct ConnectionId(pub u64);
pub struct SubscriptionId(pub u32);
pub struct BindingId(pub u32);
pub struct StreamId(pub u32);

// ❌ Primitive obsession — any u32 can be passed wrong
fn release(pending: u32, consumer: u32, queue: u32) { ... }
```

---

## 2. ENUMS OVER BOOLEANS FOR BEHAVIOR FLAGS

```rust
// ✅ Self-documenting, exhaustive match
pub enum DrainMode { ReleaseAndRequeue, ReleaseAndDrop, ReleaseAndRetryScheduled { retry_at: u64 } }
pub enum AckPolicy { None, Explicit }
pub enum DeliverMode { Fanout, Queue }

// ❌ Boolean blindness
fn drain(requeue: bool, drop: bool) { ... }
```

---

## 3. CACHE-LINE ALIGNMENT FOR HOT STRUCTS

Hot structs accessed in tight loops must respect cache-line boundaries (64 bytes).

```rust
// ✅ Exactly 64 bytes — one cache line, no false sharing
#[repr(C, align(64))]
pub struct InFlightCounter {
    pub count: AtomicU32,   // 4
    _pad: [u8; 60],         // 60
}
// const _: () = assert!(size_of::<InFlightCounter>() == 64);

// ✅ PendingNode — 96 bytes, predictable layout
#[repr(C)]
pub struct PendingNode {
    pub pending_id: PendingId,          // 4
    pub seq: u64,                       // 8
    pub queue_id: QueueId,              // 4
    pub consumer_id: ConsumerId,        // 4
    pub subscription_id: SubscriptionId, // 4
    pub binding_id: BindingId,          // 4
    pub connection_id: ConnectionId,    // 8
    pub subject_hash: u32,             // 4
    pub credits: [CreditEntry; 3],     // 24 (3 × 8)
    pub credit_count: u8,              // 1
    pub deadline_id: u32,              // 4
    pub delivered_at: u64,             // 8
    pub ack_wait_ns: u64,             // 8
    // total: ~85 bytes + padding to 96
}
```

---

## 4. POWER-OF-2 STRUCT SIZING

Hot structs in arrays must have power-of-2 byte sizes to avoid cache-line straddle.

| Size | Lines touched | Use case |
|---|---|---|
| 8 B | ½ line | dense atomic arrays, slab entries |
| 16 B | ¼ line | small hot structs, protocol IDs |
| 32 B | ½ line | protocol headers, edge entries |
| 64 B | 1 line | per-consumer hot state, counters |
| 128 B | 2 lines | PendingNode (round up from 96) |

After finalizing a hot struct's fields, compute `size_of::<T>()`. If not power of 2, add `_pad` and document why.

---

## 5. `repr(C)` FOR WIRE TYPES, `repr(C, align(64))` FOR HOT SHARED STATE

### Wire types — zerocopy mandatory

All wire types (types that cross the encode/decode boundary) MUST:

```rust
// ✅ Wire struct — complete zerocopy compliance
#[derive(IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C)]  // deterministic layout — MANDATORY for wire types
pub struct FanoutEntry {
    pub connection_id: ConnectionId,  // 8
    pub seq: u64,                     // 8
    pub subject_hash: u32,            // 4
    _pad: u32,                        // 4 — explicit, no holes
}
const _: () = assert!(size_of::<FanoutEntry>() == 24);

// ✅ Wire enum — TryFromBytes (not all u8 values valid)
#[derive(IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(u8)]  // 1 byte, explicit discriminants
pub enum AckResult {
    Acked = 0,
    NotFound = 1,
    AlreadyAcked = 2,
}

// ✅ ID newtypes — transparent wrapper
#[derive(IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(transparent)]
pub struct ConsumerId(pub u32);
```

Wire type rules:
- `#[repr(C)]` on structs — deterministic field order
- `#[repr(u8)]` on enums — 1 byte, explicit discriminants
- `#[repr(transparent)]` on ID newtypes — zero-cost wrapper
- Explicit `_pad` fields — no implicit padding holes
- `FromBytes` for structs where all bit patterns valid
- `TryFromBytes` for enums (not all 256 u8 values valid)
- Size assertion at definition — `const _: () = assert!(size_of::<T>() == N)`
- No `repr(packed)` — unaligned loads are UB on some targets

### Hot shared state (boundary only)

```rust
#[repr(C, align(64))]
pub struct ShardCounter { ... }
```

---

## 6. FIELD COHERENCE — ONE STRUCT, ONE CONCEPT

A struct that needs a comment to explain two groups of fields is two structs.

```rust
// ❌ Mixed concerns
struct ConsumerState {
    name: String,           // identity
    max_inflight: u32,   // flow control
    filter: Vec<u8>,        // routing
}

// ✅ Each concept owns its fields
struct ConsumerNode { id: ConsumerId, queue_id: QueueId, max_inflight: u32, ... }
struct SubscriptionFilter { patterns: Vec<Vec<u8>> }
```

---

## 7. INLINE PARENT IDS — NO POINTER CHASING

Every entity node stores ALL parent IDs inline. The release protocol reads one struct, not a chain.

```rust
// ✅ PendingNode — everything needed for release is HERE
pub struct PendingNode {
    pub queue_id: QueueId,
    pub consumer_id: ConsumerId,
    pub subscription_id: SubscriptionId,
    pub binding_id: BindingId,
    pub connection_id: ConnectionId,
    pub subject_hash: u32,
    pub credits: [CreditEntry; 3],  // inline array, no heap pointer
    pub deadline_id: u32,
}

// ❌ Pointer chasing — extra slab lookups during release
pub struct PendingNode {
    pub consumer_id: ConsumerId,
    // queue_id? have to look up consumer to find it
    // subject_hash? have to look up subscription to find it
}
```

---

## 8. `Arc<str>` OVER `String` FOR SHARED NAMES

For names shared across threads, use `Arc<str>` — one allocation, cheap clone.

```rust
// ✅
struct ConsumerCold { name: Arc<str> }

// ❌
struct ConsumerCold { name: String }  // clone allocates
```

---

## 9. NO MAGIC NUMBERS

Every constant is named. Protocol constants live in `types.rs`.

```rust
// ✅ Named
pub const MAX_CREDITS_PER_SCOPE: usize = 3;
pub const TIMER_WHEEL_SLOTS: usize = 65536;

// ❌ Raw literal
if pending.credit_count > 3 { ... }
```

---

## 10. SIZE ASSERTIONS AT TYPE DEFINITION

```rust
const _: () = assert!(std::mem::size_of::<PendingNode>() <= 128);
const _: () = assert!(std::mem::size_of::<InFlightCounter>() == 64);
const _: () = assert!(std::mem::size_of::<CreditEntry>() == 8);
```
