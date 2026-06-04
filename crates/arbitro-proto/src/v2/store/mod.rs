//! Store — persisted batches of records.
//!
//! Storage unit: `Batch` = `[BatchHeader 16B][records...][padding → 4 KiB]`
//!   * `BatchHeader` starts with the `ARB2` magic for format identification
//!     and crash-recovery resynchronization.
//!   * `batch_len` is always a multiple of `PAGE_SIZE` (4096) so each batch
//!     aligns on a page boundary → `O_DIRECT` + `io_uring` write friendly.
//!   * `content_len` is the exact bytes of records (without padding).
//!   * Records are packed back-to-back; iterate via `header.msg_len` per record.
//!
//! Record unit: `Record` = `[Header 16B][RecordBody 12B][subject][payload]`.
//!
//! Promotion ingress → store: copy body bytes, overwrite the first 16 B
//! with a new `Header { action = Store, seq = global_seq }`, write
//! `subject_hash`/`stream_id` into the leading `RecordBody`.
//! The subject+payload bytes are **never touched**.

pub mod batch;
pub mod record;

pub use batch::{align_up_to_page, BatchHeader, BATCH_HEADER_SIZE, PAGE_SIZE};
pub use record::{Record, RecordBody, RECORD_BODY_FIXED};
