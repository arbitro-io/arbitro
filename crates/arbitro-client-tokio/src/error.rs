//! Error types for the tokio client.
//!
//! Reuses `arbitro_proto::error::{ErrorCode, ProtoError}` directly
//! instead of duplicating an enum here. The wire-level taxonomy (auth,
//! stream, consumer, delivery, system) is owned by `arbitro-proto` —
//! this module only adds *transport-side* variants the proto layer
//! cannot express (IO failure, timeout, disconnect, channel closed).

use std::io;

use arbitro_proto::error::{ErrorCode, ProtoError};

/// All errors a client operation can surface.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// Underlying TCP / IO failure.
    #[error("io: {0}")]
    Io(#[from] io::Error),

    /// Frame parse failed locally (malformed reply from broker).
    #[error("proto: {0:?}")]
    Proto(ProtoError),

    /// Broker returned an explicit error reply for a request.
    /// `code` is the canonical wire code; map via `ErrorCode::from_u16`
    /// when you need the typed variant.
    #[error("broker error: {code:?}")]
    Broker { code: ErrorCode },

    /// Broker returned an unknown error code (forward-compatibility).
    #[error("broker error: unknown code 0x{0:04x}")]
    BrokerUnknown(u16),

    /// A sync request did not get a reply within its budget.
    #[error("timeout")]
    Timeout,

    /// The client is not currently connected (and reconnection is in
    /// progress or has been exhausted).
    #[error("disconnected")]
    Disconnected,

    /// Internal channel closed (writer task gone, runtime shutting down).
    #[error("channel closed")]
    ChannelClosed,
}

impl From<ProtoError> for ClientError {
    fn from(e: ProtoError) -> Self {
        Self::Proto(e)
    }
}

impl ClientError {
    /// Build from a wire error code, preserving unknown values.
    #[inline]
    pub fn from_wire_code(code: u16) -> Self {
        match ErrorCode::from_u16(code) {
            Some(c) => Self::Broker { code: c },
            None => Self::BrokerUnknown(code),
        }
    }
}

/// Result type used internally to deliver request replies through
/// `kit::OneShotAsync<RequestResult>`.
pub type RequestResult = Result<bytes::Bytes, ClientError>;
