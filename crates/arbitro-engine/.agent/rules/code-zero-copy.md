---
description: Zero-copy discipline — zerocopy wire codec, ScratchReply, PayloadRef, inline IDs, Bytes transport. INVIOLABLE.
---

# ZERO-COPY RULES

ArbitroDB is a **pure synchronous state engine** (`&mut self`). It owns no I/O,
no threads, no async. Zero-copy is enforced at two levels: (1) the wire codec
uses the `zerocopy` crate for pointer-cast encode/decode, and (2) hot path
replies borrow from pre-allocated scratch buffers inside `EngineContext`.

**These rules are INVIOLABLE. Zero-copy and hardware sympathy are non-negotiable.**

---

## 1. ZEROCOPY WIRE CODEC — THE ONLY APPROVED ENCODE/DECODE

All transport structs use `zerocopy` crate derives for zero-copy wire conversion.
Encode = pointer cast (`as_bytes()`, ~400ps). Decode = alignment check
(`wire::decode_slice()`, ~700ps). Sub-nanosecond, O(1) regardless of slice size.

### 1.1 Wire type requirements

```rust
// ✅ All wire types MUST derive these traits
#[derive(IntoBytes, FromBytes, Immutable, KnownLayout)]
#[repr(C)]  // deterministic field order — MANDATORY
pub struct FanoutEntry {
    pub connection_id: ConnectionId,  // 8
    pub seq: u64,                     // 8
    pub subject_hash: u32,            // 4
    _pad: u32,                        // 4 — explicit padding, no holes
}
const _: () = assert!(size_of::<FanoutEntry>() == 24);

// ✅ Enums use TryFromBytes (not all 256 u8 values are valid)
#[derive(IntoBytes, TryFromBytes, Immutable, KnownLayout)]
#[repr(u8)]  // 1 byte, explicit discriminants
pub enum AckResult {
    Acked = 0,
    NotFound = 1,
    AlreadyAcked = 2,
}
```

### 1.2 Wire encode (producer side)

```rust
let bytes: &[u8] = reply.as_bytes();  // ~400ps pointer cast
// bytes ready for wire — no serialization, no allocation
```

### 1.3 Wire decode (consumer side)

```rust
let entries: &[FanoutEntry] = wire::decode_slice(bytes)?;  // ~700ps
let publish: &RepPublish = wire::decode_ref(bytes)?;        // ~700ps
// Direct field access — no deserialization
```

### 1.4 Wire type rules — MANDATORY

| Rule | Reason |
|---|---|
| `#[repr(C)]` on all wire structs | Deterministic layout across compilations |
| `#[repr(u8)]` on wire enums | 1 byte, explicit discriminants |
| `#[repr(transparent)]` on ID newtypes | Zero-cost wrapper, same layout as inner |
| Explicit `_pad` fields | No implicit padding holes — every byte accounted for |
| `FromBytes` for structs | All bit patterns valid — safe pointer cast |
| `TryFromBytes` for enums | Not all 256 u8 values are valid variants |
| `const _: () = assert!(size_of::<T>() == N)` | Size assertion at definition — no surprises |

### 1.5 Banned alternatives

- `transmute` — UB risk, use `zerocopy` traits
- Raw pointer casts (`as *const T`) — unsafe, use `zerocopy`
- `serde` / `serde_json` — too slow, allocates. Wire codec is zerocopy
- Manual byte-by-byte scalar reads — error-prone, slower
- Re-overlaying `ref_from_prefix` per accessor — overlay ONCE, store reference

---

## 2. ScratchReply<T> — PRE-ALLOCATED HOT PATH REPLIES

Hot path replies are `&ScratchReply<T>` — a reference to a pre-allocated buffer
inside `EngineContext`. Zero allocation per reply.

```rust
pub struct ScratchReply<T> {
    pub op: OperationKind,
    pub accepted: u32,
    pub rejected: u32,
    buf: Vec<T>,           // pre-allocated at init, recycled with reset()
}

impl<T> ScratchReply<T> {
    pub fn reset(&mut self)           // clear buf, zero counters — O(1)
    pub fn accept(&mut self, entry: T) // push to buf, inc accepted
    pub fn entries(&self) -> &[T]     // borrow the buffer
}

impl<T: IntoBytes + Immutable> ScratchReply<T> {
    pub fn as_bytes(&self) -> &[u8]   // zerocopy pointer cast — ~400ps
}
```

### 2.1 Borrow discipline

`ScratchReply` borrows from `&mut EngineContext`. The caller MUST drop the
reference before calling the engine again:

```rust
// ✅ Scoped borrow
let (accepted, seqs) = {
    let reply = engine.claim(&batch);
    (reply.accepted, reply.entries().iter().map(|e| e.seq).collect::<Vec<_>>())
};
// reply dropped — engine available
engine.ack(&ack_batch);

// ❌ Borrow conflict
let reply = engine.claim(&batch);
engine.ack(&ack_batch);  // ERROR: engine still borrowed by reply
```

### 2.2 Scratch buffer lifecycle

- Pre-allocated at `ArbitroEngine::new()` with capacity 64 each
- `reset()` clears buf to len=0 without deallocating — O(1)
- Capacity grows monotonically — never shrinks after warmup
- After warmup: zero allocations on hot path

```rust
// EngineContext pre-allocates 3 scratch buffers
pub struct EngineContext {
    pub reply_claim: ScratchReply<ClaimedEntry>,  // capacity 64
    pub reply_ack: ScratchReply<AckResult>,       // capacity 64
    pub reply_nack: ScratchReply<NackResult>,     // capacity 64
}
```

---

## 3. BYTES IS THE PAYLOAD TRANSPORT TYPE

`bytes::Bytes` is the required type for payloads that need owned data.

- `.clone()` is an Arc refcount bump (~3ns) — **always prefer** over `copy_from_slice`
- `.slice(start..end)` returns into the same Arc — O(1), zero allocation
- `Bytes::copy_from_slice` is allowed **ONLY at ingress** (kernel read → owned buffer)
- After ingress, payloads travel as `Bytes` clones or slices — never re-copied

---

## 4. PayloadRef — THE PAYLOAD CONTAINER

`PayloadRef` is the zero-copy payload type in batch entries:

```rust
pub enum PayloadRef<'a> {
    Borrowed(&'a [u8]),     // batch parsing: borrows from wire buffer
    Owned(Bytes),           // store: Arc-backed, clone is free
}
```

- At ingress: `PayloadRef::Borrowed` — borrows from the wire buffer, zero copy
- After store: `PayloadRef::Owned(Bytes)` — Arc-backed, clone = 3ns
- **Never** convert `PayloadRef::Owned(b)` to `Vec<u8>` — use `.as_ref()` for reads

---

## 5. SLAB ENTITIES — INLINE IDs, NO POINTER CHASING

Every entity (PendingNode, ConnectionNode, etc.) stores parent IDs **inline**.
The release protocol never walks the ownership graph — one slab remove gives
all information needed for cleanup.

```rust
// ✅ PendingNode — all parent IDs inline
pub struct PendingNode {
    pub queue_id: QueueId,
    pub consumer_id: ConsumerId,
    pub connection_id: ConnectionId,
    pub subject_hash: u32,
    pub credits: [CreditEntry; 3],  // inline array, no heap
    pub deadline_id: u32,
    // ... all data for release_pending is HERE
}

// ❌ Following pointers to find parent
fn release(&mut self, pid: PendingId) {
    let pending = self.pending.get(pid);
    let consumer = self.consumers.get(pending.consumer_id);  // extra lookup
    let queue = consumer.queue_id;  // chasing pointers — BANNED
}
```

---

## 6. SUBJECTS ARE `&[u8]` + `subject_hash: u32` — NO SubjectId

Subjects are identified by `subject_hash: u32` (FNV-1a of raw bytes).
There is NO `SubjectId` type — the hash IS the identity.

```rust
// ✅ Hot path — route by hash
match_table.lookup(subject_hash)  // O(1) array index

// ✅ Raw bytes only for first-time pattern resolution
fn resolve_subject(subject: &[u8]) -> &[MatchEntry]

// ❌ SubjectId indirection — REMOVED from model
// ❌ from_utf8 on hot path — O(N) scan
// ❌ Vec::collect() in matching — allocation
```

---

## 7. FanoutDrain — RAII ZERO-COPY BUFFER

```rust
let drain: FanoutDrain<'_> = engine.drain_fanout();
// drain.entries() → &[FanoutEntry], reference to internal buffer
// drain.as_bytes() → &[u8], pointer cast (~400ps)
drop(drain);  // RAII reset — buffer ready for next publish, no dealloc
```

`FanoutDrain` is a RAII guard. When dropped, it resets the fanout buffer
to len=0 without deallocating. Next publish reuses the same memory.

---

## COMPLETE COPY BUDGET

ArbitroDB allows exactly **3 copies** in the entire message lifecycle:

| # | Where | What | Why unavoidable |
|---|---|---|---|
| 1 | Ingress | TCP read → owned buffer | Kernel → userspace boundary |
| 2 | Store append | Buffer → store's internal Vec | Persistence requires owned data |
| 3 | Wire encode | Header on stack → write buffer | 32B header, stack not writable by scatter-gather |

Everything else is `Bytes::clone()` (Arc bump, ~3ns) or `Bytes::slice()` (O(1) sub-view).

---

## HOT-PATH AUDIT CHECKLIST

Before every change touching the hot path:

- [ ] Any `Bytes::copy_from_slice` after ingress? → REMOVE, use `.clone()` or `.slice()`
- [ ] Any `Vec::new()` / `Vec::with_capacity()` inside a loop? → PRE-ALLOCATE before loop
- [ ] Any `from_utf8` on subject? → REMOVE, use `&[u8]`
- [ ] Any `transmute` or raw pointer cast? → USE `zerocopy` traits
- [ ] Any `String::from()` or `to_string()` on hot path? → REMOVE
- [ ] PendingNode chasing parent pointers? → USE inline IDs
- [ ] Match table evaluated at deliver time? → USE precomputed table
- [ ] Any `Vec::collect()` in matching? → USE iterator loop
- [ ] Wire types missing `#[repr(C)]`? → ADD repr and size assertion
- [ ] Wire types using `serde` instead of `zerocopy`? → REPLACE with zerocopy derives
- [ ] Reply allocating per call? → USE `ScratchReply<T>` from EngineContext
- [ ] Wire enum using `FromBytes`? → USE `TryFromBytes` (not all u8 values valid)
- [ ] Padding holes implicit? → ADD explicit `_pad` fields
