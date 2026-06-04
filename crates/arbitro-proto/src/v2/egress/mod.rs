//! Egress — frames sent from server to clients (DELIVER, REP_OK, REP_ERR).
//!
//! Deliver reuses the store record's subject+payload bytes with **zero copy**
//! via `writev` — the egress builds only its own header + body and points at
//! the shared payload region.

pub mod deliver_frame;
pub mod rep_frame;

pub use deliver_frame::{
    DeliverBatchEntry, DeliverBatchHeader, DeliverBody, DeliverFrame, DELIVER_BATCH_ENTRY_FIXED,
    DELIVER_BATCH_HEADER_FIXED, DELIVER_BODY_FIXED,
};
pub use rep_frame::{
    RepErrBody, RepErrFrame, RepOkBody, RepOkFrame, REP_ERR_BODY_SIZE, REP_OK_BODY_SIZE,
};
