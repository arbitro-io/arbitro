//! Protocol v2 — pure zerocopy, domain-grouped.
//!
//! Layout philosophy:
//!   * `Header` (16 B) is the only shared on-wire struct — every frame starts
//!     with it. It carries **only frame-level** data: action, flags, version,
//!     body length, sequence. No routing identifiers (those live in the body).
//!
//!   * Each domain (`ingress`, `store`, `egress`) defines its own frames as
//!     DSTs (`#[repr(C, packed)]` with a trailing `tail: [u8]`). A frame is
//!     a single typed view over a contiguous byte slice — zero allocations,
//!     zero copies, zero intermediate buffers.
//!
//!   * Domains share a byte-identical layout whenever possible so that the
//!     same bytes can be **re-interpreted** without moving data
//!     (ingress→store→egress). Promotion = overwrite action/seq in-place.
//!
//! Not a replacement for `crate::wire` (the legacy transport). Both live
//! side-by-side until the migration completes.
//!
//! Reference bench: `arbitro-e2e/benches/proto_v2_uring.rs`.

pub mod header;
pub mod magic;

pub mod ingress;
pub mod store;
pub mod egress;
pub mod manager;

pub use header::{Header, HEADER_SIZE};
pub use magic::{ARBITRO_MAGIC_V2, MAGIC_SIZE};
