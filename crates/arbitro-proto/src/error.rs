/// Wire error codes — sent in RepErrorAction.error_code.
///
/// All errors travel through the same channel: RepError frame.
/// The client reads error_code (u16) and knows what happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ErrorCode {
    // 0x00xx — Protocol (frame arrived malformed)
    UnknownAction = 0x0001,
    BufferTooShort = 0x0002,
    InvalidLength = 0x0003,
    InvalidEntryCount = 0x0004,

    // 0x01xx — Auth
    AuthRequired = 0x0101,
    AuthFailed = 0x0102,

    // 0x02xx — Stream
    StreamNotFound = 0x0201,
    StreamAlreadyExists = 0x0202,
    StreamFull = 0x0203,
    // 0x0204 reserved (deleted StreamFilterOverlap) — §5.2.
    // 0x0205 reserved (deleted SubjectNotFound) — §5.2.
    /// Publish carried a `msg_id` that the broker has already seen for
    /// this stream within `idempotency_window_ms`. The message was not
    /// stored. Safe for the client to treat as a successful publish
    /// (same logical effect — the original write is what's stored).
    IdempotencyDuplicate = 0x0206,

    // 0x03xx — Consumer
    ConsumerNotFound = 0x0301,
    ConsumerAlreadyExists = 0x0302,
    // 0x0303 reserved (deleted ConsumerFilterOverlap) — §5.2.
    /// The CreateConsumer request carries mutually incompatible fields
    /// (e.g. `AckPolicy::None` + `max_inflight`, or `Fanout` + non-empty
    /// `group`). The consumer was NOT created.
    InvalidConsumerConfig = 0x0304,

    // 0x04xx — Delivery
    // 0x0401 reserved (deleted InvalidSequence) — §5.2.
    // 0x0402 reserved (deleted MaxInflightReached) — §5.2.
    // 0x0403 reserved (deleted AckTimeout) — §5.2.

    // 0x05xx — System
    ServerShuttingDown = 0x0501,
    InternalError = 0x0502,
    /// The broker recognised the wire action code but no handler is wired
    /// (yet) — distinguishes "I don't know this code" (UnknownAction) from
    /// "I know it but won't service it in this build". Used for protocol
    /// surface that is reserved but not implemented. L2.
    Unimplemented = 0x0503,
}

impl ErrorCode {
    #[inline(always)]
    pub const fn from_u16(v: u16) -> Option<Self> {
        match v {
            0x0001 => Some(Self::UnknownAction),
            0x0002 => Some(Self::BufferTooShort),
            0x0003 => Some(Self::InvalidLength),
            0x0004 => Some(Self::InvalidEntryCount),

            0x0101 => Some(Self::AuthRequired),
            0x0102 => Some(Self::AuthFailed),

            0x0201 => Some(Self::StreamNotFound),
            0x0202 => Some(Self::StreamAlreadyExists),
            0x0203 => Some(Self::StreamFull),
            // 0x0204, 0x0205 reserved (deleted §5.2).
            0x0206 => Some(Self::IdempotencyDuplicate),

            0x0301 => Some(Self::ConsumerNotFound),
            0x0302 => Some(Self::ConsumerAlreadyExists),
            // 0x0303 reserved (deleted §5.2).
            0x0304 => Some(Self::InvalidConsumerConfig),

            // 0x0401..=0x0403 reserved (deleted §5.2).
            0x0501 => Some(Self::ServerShuttingDown),
            0x0502 => Some(Self::InternalError),
            0x0503 => Some(Self::Unimplemented),

            _ => None,
        }
    }

    #[inline(always)]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
}

/// Local parse/decode error — never goes on the wire.
/// Used internally when reading frames from a buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtoError {
    BufferTooShort { need: u32, have: u32 },
    UnknownAction(u16),
    InvalidLength,
    InvalidEntryCount,
    AlignmentError,
}
