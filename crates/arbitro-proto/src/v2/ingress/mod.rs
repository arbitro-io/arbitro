//! Ingress — frames arriving from clients.
//!
//! Every ingress frame after handshake = `[Header 16B][ingress body]`. The
//! body layout is determined by `header.action`. Variant-specific actions
//! (Publish, PublishWithReply) carry a body shape
//! that maps 1:1 to the action — no discriminator byte inside the body,
//! no inner branching.
//!
//! Pre-handshake: `HelloFrame` (8B, starts with magic) — see `hello.rs`.
//!
//! Hot paths: `PubFrame`, `BatchPubFrame`, `AckFrame`, `BatchAckFrame`.

pub mod ack_frame;
pub mod batch_pub_frame;
pub mod hello;
pub mod nack_frame;
pub mod pub_frame;
// pub_with_headers deleted — §5.1 (frame without dispatcher).
pub mod pub_with_reply;
// sub_frame removed — Subscribe migrated to `v2::cold::Subscribe`.

pub use ack_frame::{
    ACK_BODY_SIZE, AckBody, AckFrame, BATCH_ACK_BODY_FIXED, BATCH_ACK_ENTRY_SIZE, BatchAckBody,
    BatchAckEntry, BatchAckFrame,
};
pub use nack_frame::{
    NACK_BODY_SIZE, NackBody, NackFrame, BATCH_NACK_BODY_FIXED, BATCH_NACK_ENTRY_SIZE,
    BatchNackBody, BatchNackEntry, BatchNackFrame,
};
pub use batch_pub_frame::{
    BATCH_PUB_BODY_FIXED, BATCH_PUB_ENTRY_HEADER_SIZE, BatchPubBody, BatchPubEntryHeader,
    BatchPubEntryView, BatchPubFrame, BatchPubIter,
};
pub use hello::{HELLO_FRAME_SIZE, HelloFrame, Role};
pub use pub_frame::{PUB_BODY_FIXED, PubBody, PubFrame};
pub use pub_with_reply::{PUB_WITH_REPLY_BODY_FIXED, PubWithReplyBody, PubWithReplyFrame};
