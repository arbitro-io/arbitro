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
pub mod pub_delayed_frame;
pub mod pub_frame;
// pub_with_headers deleted — §5.1 (frame without dispatcher).
pub mod pub_with_reply;
// sub_frame removed — Subscribe migrated to `v2::cold::Subscribe`.

pub use ack_frame::{
    AckBody, AckFrame, BatchAckBody, BatchAckEntry, BatchAckFrame, ACK_BODY_SIZE,
    BATCH_ACK_BODY_FIXED, BATCH_ACK_ENTRY_SIZE,
};
pub use batch_pub_frame::{
    BatchPubBody, BatchPubEntryHeader, BatchPubEntryView, BatchPubFrame, BatchPubIter,
    BATCH_PUB_BODY_FIXED, BATCH_PUB_ENTRY_HEADER_SIZE,
};
pub use hello::{HelloFrame, Role, HELLO_FRAME_SIZE};
pub use nack_frame::{
    BatchNackBody, BatchNackEntry, BatchNackFrame, NackBody, NackFrame, BATCH_NACK_BODY_FIXED,
    BATCH_NACK_ENTRY_SIZE, NACK_BODY_SIZE,
};
pub use pub_delayed_frame::{PubDelayedBody, PubDelayedFrame, PUB_DELAYED_BODY_FIXED};
pub use pub_frame::{PubBody, PubFrame, PUB_BODY_FIXED};
pub use pub_with_reply::{PubWithReplyBody, PubWithReplyFrame, PUB_WITH_REPLY_BODY_FIXED};
