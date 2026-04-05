/// Action codes — identifies what each frame does.
///
/// Layout: `0xFFGG` where FF = family, GG = variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum Action {
    // 0x01xx — Publish
    Publish       = 0x0101,

    // 0x02xx — Delivery
    Ack           = 0x0201,
    Nack          = 0x0202,
    RepOk         = 0x0203,
    RepError      = 0x0204,

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

    // 0x06xx — System
    Ping          = 0x0601,
    Pong          = 0x0602,
    Connect       = 0x0603,
    Connected     = 0x0604,
    Disconnect    = 0x0605,

    // 0x07xx — Metrics
    Stats         = 0x0701,
    StatsReply    = 0x0702,
}

impl Action {
    /// Decode from wire u16. Returns None for unknown codes.
    #[inline(always)]
    pub const fn from_u16(v: u16) -> Option<Self> {
        match v {
            0x0101 => Some(Self::Publish),

            0x0201 => Some(Self::Ack),
            0x0202 => Some(Self::Nack),
            0x0203 => Some(Self::RepOk),
            0x0204 => Some(Self::RepError),

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

            0x0601 => Some(Self::Ping),
            0x0602 => Some(Self::Pong),
            0x0603 => Some(Self::Connect),
            0x0604 => Some(Self::Connected),
            0x0605 => Some(Self::Disconnect),

            0x0701 => Some(Self::Stats),
            0x0702 => Some(Self::StatsReply),

            _ => None,
        }
    }

    #[inline(always)]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    /// Hot-path actions: publish, ack, nack.
    #[inline(always)]
    pub const fn is_hot(self) -> bool {
        matches!(self, Self::Publish | Self::Ack | Self::Nack)
    }

    /// Actions that carry a subject for routing.
    #[inline(always)]
    pub const fn has_subject(self) -> bool {
        matches!(self, Self::Publish | Self::Subscribe)
    }
}
