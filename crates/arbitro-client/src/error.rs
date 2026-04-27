//! Client error types.

use std::fmt;

use arbitro_proto::error::ErrorCode;

/// All client-visible errors.
#[derive(Debug)]
pub enum ClientError {
    /// TCP / IO error.
    Io(std::io::Error),
    /// Server returned an error code.
    Broker(ErrorCode),
    /// Request timed out.
    Timeout,
    /// Not connected.
    Disconnected,
    /// Write ring is saturated; the writer task can't drain as fast as
    /// the publisher produces (typically TCP backpressure). The frame
    /// was NOT enqueued — caller decides retry / drop / circuit-break.
    Backpressure,
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "io: {e}"),
            Self::Broker(code) => write!(f, "broker: {code:?}"),
            Self::Timeout => write!(f, "request timed out"),
            Self::Disconnected => write!(f, "not connected"),
            Self::Backpressure => write!(f, "write ring saturated; retry"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<std::io::Error> for ClientError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}
