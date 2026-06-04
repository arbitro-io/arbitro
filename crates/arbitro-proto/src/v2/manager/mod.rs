//! Manager frames — stream + consumer lifecycle (cold path).
//!
//! Same v2 contract as ingress/egress: every frame = `Header(16B) + body`.
//! Manager bodies contain a fixed `#[repr(C)]` prefix + variable name/filter
//! tail. Decoder uses a single `ref_from_bytes` over the body.
//!
//! Replies for create/delete/get use the existing `egress::rep_frame::RepOkFrame`
//! / `RepErrFrame`. Lists use `egress::list_reply::ListReplyFrame`.

pub mod consumer_mgmt;
pub mod stream_mgmt;

pub use consumer_mgmt::{
    subject_limits_tail_len,
    CreateConsumerBody,
    CreateConsumerFrame,
    // ListConsumers + ConsumerStats + Delete/Get/Pause/Resume migrated to v2::cold.
    SubjectLimit,
    CREATE_CONSUMER_BODY_FIXED,
    SUBJECT_LIMIT_HEADER_SIZE,
};
pub use stream_mgmt::{
    CreateStreamBody,
    CreateStreamFrame,
    CREATE_STREAM_BODY_FIXED,
    // ListStreams + Delete/Get/Purge/DrainSubject migrated to v2::cold.
};
