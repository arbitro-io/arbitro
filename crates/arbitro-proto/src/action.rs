/// Action codes — identifies what each frame does.
///
/// Layout: `0xFFGG` where FF = family, GG = variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum Action {
    // 0x01xx — Publish family. Specific actions per body shape — no
    // discriminator byte inside the payload, no inner branching.
    // TODO §5.1: 0x0102 (PublishAccumulate) deleted — v1-only, never
    // dispatched by the v2 server. Reserved; do not reuse the slot.
    Publish                  = 0x0101,
    PublishBatch             = 0x0103,
    PublishWithReply         = 0x0104, // + reply_to (RPC)
    // 0x0105, 0x0106 reserved (deleted PublishWithHeaders /
    // PublishBatchWithHeaders) — §5.1: frame defs existed but no
    // dispatcher was ever wired. Reserved; do not reuse the slots.

    // 0x00xx — Handshake / control (pre-Header). Hello is *not* a v2 frame:
    // its on-wire representation is a 8B HelloFrame starting with the v2
    // magic, sent as the first bytes of every connection.
    Hello                    = 0x0001,
    /// Auth frame — Header + raw token bytes. Must be the first frame
    /// after Hello when the server requires authentication.
    Auth                     = 0x0002,

    // 0x02xx — Delivery
    Deliver       = 0x0200,
    Ack           = 0x0201,
    Nack          = 0x0202,
    RepOk         = 0x0203,
    RepError      = 0x0204,
    RepBatch      = 0x0205,
    BatchAck      = 0x0206,
    FanoutBatch   = 0x0207,
    // TODO §5.1: 0x0208 (AckSync) and 0x0209 (BatchAckSync) deleted —
    // collapsed into Ack / BatchAck (both wait for store fsync before
    // replying). Reserved; do not reuse the slots.
    BatchNack     = 0x020A,

    // 0x03xx — Subscription
    Subscribe     = 0x0301,
    Unsubscribe   = 0x0302,

    // 0x04xx — Stream management
    CreateStream  = 0x0401,
    DeleteStream  = 0x0402,
    GetStream     = 0x0403,
    ListStreams   = 0x0404,
    PurgeStream   = 0x0405,
    DrainSubject  = 0x0406,

    // 0x05xx — Consumer management
    CreateConsumer = 0x0501,
    DeleteConsumer = 0x0502,
    GetConsumer    = 0x0503,
    ListConsumers  = 0x0504,
    /// Query a single consumer's live pending-ack count. Reply is a
    /// standard `RepOk` whose `ref_seq` field carries the count as a u64.
    ConsumerStats  = 0x0505,
    /// M11: pause delivery to a consumer. Body = u32 consumer_id.
    /// Reply is `RepOk` (ref_seq mirrors req_seq).
    PauseConsumer  = 0x0506,
    /// M11: resume delivery to a previously paused consumer.
    ResumeConsumer = 0x0507,

    // 0x06xx — System
    Ping          = 0x0601,
    Pong          = 0x0602,
    // L1: 0x0603 (Connect) / 0x0604 (Connected) deleted — v2 uses
    // HelloFrame for the handshake, so these wire codes have no
    // dispatcher. Reserved; do not reuse the slots.
    Disconnect    = 0x0605,

    // L1: 0x07xx (Stats / StatsReply) deleted — metrics travel via
    // the per-shard `Metrics` command in arbitro-server, never as a
    // wire frame. Reserved; do not reuse the family yet.
}

impl Action {
    /// Decode from wire u16. Returns None for unknown codes.
    #[inline(always)]
    pub const fn from_u16(v: u16) -> Option<Self> {
        match v {
            0x0001 => Some(Self::Hello),
            0x0002 => Some(Self::Auth),

            0x0101 => Some(Self::Publish),
            // 0x0102 reserved (deleted PublishAccumulate) — TODO §5.1.
            0x0103 => Some(Self::PublishBatch),
            0x0104 => Some(Self::PublishWithReply),
            // 0x0105, 0x0106 reserved (deleted §5.1).

            0x0200 => Some(Self::Deliver),
            0x0201 => Some(Self::Ack),
            0x0202 => Some(Self::Nack),
            0x0203 => Some(Self::RepOk),
            0x0204 => Some(Self::RepError),
            0x0205 => Some(Self::RepBatch),
            0x0206 => Some(Self::BatchAck),
            0x0207 => Some(Self::FanoutBatch),
            // 0x0208, 0x0209 reserved (deleted AckSync/BatchAckSync) — TODO §5.1.
            0x020A => Some(Self::BatchNack),

            0x0301 => Some(Self::Subscribe),
            0x0302 => Some(Self::Unsubscribe),

            0x0401 => Some(Self::CreateStream),
            0x0402 => Some(Self::DeleteStream),
            0x0403 => Some(Self::GetStream),
            0x0404 => Some(Self::ListStreams),
            0x0405 => Some(Self::PurgeStream),
            0x0406 => Some(Self::DrainSubject),

            0x0501 => Some(Self::CreateConsumer),
            0x0502 => Some(Self::DeleteConsumer),
            0x0503 => Some(Self::GetConsumer),
            0x0504 => Some(Self::ListConsumers),
            0x0505 => Some(Self::ConsumerStats),
            0x0506 => Some(Self::PauseConsumer),
            0x0507 => Some(Self::ResumeConsumer),

            0x0601 => Some(Self::Ping),
            0x0602 => Some(Self::Pong),
            // 0x0603, 0x0604 reserved (deleted Connect/Connected) — L1.
            0x0605 => Some(Self::Disconnect),
            // 0x0701, 0x0702 reserved (deleted Stats/StatsReply) — L1.

            _ => None,
        }
    }

    #[inline(always)]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    /// Hot-path actions: publish family, ack, nack.
    #[inline(always)]
    pub const fn is_hot(self) -> bool {
        matches!(
            self,
            Self::Publish
                | Self::PublishBatch
                | Self::PublishWithReply
                | Self::Ack
                | Self::Nack
                | Self::BatchAck
                | Self::BatchNack
        )
    }

    /// Actions that carry a subject for routing.
    #[inline(always)]
    pub const fn has_subject(self) -> bool {
        matches!(
            self,
            Self::Publish
                | Self::PublishBatch
                | Self::PublishWithReply
                | Self::Subscribe
        )
    }

    /// Whether the action is a member of the publish family.
    #[inline(always)]
    pub const fn is_publish(self) -> bool {
        matches!(
            self,
            Self::Publish
                | Self::PublishBatch
                | Self::PublishWithReply
        )
    }
}
