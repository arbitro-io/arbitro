//! Egress — frames sent from server to clients (DELIVER, REP_OK, REP_ERR).
//!
//! Deliver reuses the store record's subject+payload bytes with **zero copy**
//! via `writev` — the egress builds only its own header + body and points at
//! the shared payload region.

pub mod deliver_frame;
pub mod rep_frame;

pub use deliver_frame::{
    DeliverFrame, DeliverBody, DELIVER_BODY_FIXED,
    DeliverBatchHeader, DELIVER_BATCH_HEADER_FIXED,
    DeliverBatchEntry, DELIVER_BATCH_ENTRY_FIXED,
};
pub use rep_frame::{RepOkFrame, RepOkBody, REP_OK_BODY_SIZE, RepErrFrame, RepErrBody, REP_ERR_BODY_SIZE};
