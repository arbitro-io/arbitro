//! Manager frames — stream + consumer lifecycle (cold path).
//!
//! Same v2 contract as ingress/egress: every frame = `Header(16B) + body`.
//! Manager bodies contain a fixed `#[repr(C)]` prefix + variable name/filter
//! tail. Decoder uses a single `ref_from_bytes` over the body.
//!
//! Replies for create/delete/get use the existing `egress::rep_frame::RepOkFrame`
//! / `RepErrFrame`. Lists use `egress::list_reply::ListReplyFrame`.

pub mod stream_mgmt;
pub mod consumer_mgmt;

pub use stream_mgmt::{
    CreateStreamBody, CreateStreamFrame, CREATE_STREAM_BODY_FIXED,
    // Delete/Get/Purge/DrainSubject migrated to v2::cold (serde_json).
    ListStreamsBody, ListStreamsFrame, LIST_STREAMS_BODY_SIZE,
};
pub use consumer_mgmt::{
    CreateConsumerBody, CreateConsumerFrame, CREATE_CONSUMER_BODY_FIXED,
    // Delete/Get/Pause/Resume migrated to v2::cold (serde_json).
    ListConsumersBody, ListConsumersFrame, LIST_CONSUMERS_BODY_SIZE,
    ConsumerStatsBody, ConsumerStatsFrame, CONSUMER_STATS_BODY_SIZE,
    SubjectLimit, SUBJECT_LIMIT_HEADER_SIZE, subject_limits_tail_len,
};
