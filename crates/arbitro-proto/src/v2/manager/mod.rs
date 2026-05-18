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
    DeleteStreamBody, DeleteStreamFrame, DELETE_STREAM_BODY_FIXED,
    GetStreamBody, GetStreamFrame, GET_STREAM_BODY_FIXED,
    PurgeStreamBody, PurgeStreamFrame, PURGE_STREAM_BODY_FIXED,
    DrainSubjectBody, DrainSubjectFrame, DRAIN_SUBJECT_BODY_FIXED,
    ListStreamsBody, ListStreamsFrame, LIST_STREAMS_BODY_SIZE,
};
pub use consumer_mgmt::{
    CreateConsumerBody, CreateConsumerFrame, CREATE_CONSUMER_BODY_FIXED,
    DeleteConsumerBody, DeleteConsumerFrame, DELETE_CONSUMER_BODY_SIZE,
    GetConsumerBody, GetConsumerFrame, GET_CONSUMER_BODY_FIXED,
    ListConsumersBody, ListConsumersFrame, LIST_CONSUMERS_BODY_SIZE,
    ConsumerStatsBody, ConsumerStatsFrame, CONSUMER_STATS_BODY_SIZE,
    // Pause/Resume migrated to v2::cold (serde_json).
    SubjectLimit, SUBJECT_LIMIT_HEADER_SIZE, subject_limits_tail_len,
};
