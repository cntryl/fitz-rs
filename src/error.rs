use std::io::ErrorKind;

/// Stable error categories exposed by the SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FitzErrorKind {
    Authentication,
    Canceled,
    Timeout,
    Connection,
    Transport,
    Protocol,
    Backpressure,
    StaleHandle,
    Domain,
    Closed,
    ConnectionClosed,
}

/// An error returned by Fitz.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FitzError {
    #[error("authentication rejected: {message}")]
    Authentication { message: String },
    #[error("operation canceled")]
    Canceled,
    #[error("operation timed out")]
    Timeout,
    #[error("connection error: {0}")]
    Connection(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("protocol error: {0}")]
    Protocol(String),
    #[error("codec error: {0}")]
    Codec(String),
    #[error("backpressure limit reached: {0}")]
    Backpressure(String),
    #[error("handle belongs to an earlier connection session")]
    StaleHandle,
    #[error("domain error {code}: {message}")]
    Domain { code: u32, message: String },
    #[error("domain error: {0}")]
    DomainError(String),
    #[error("authentication rejected: {0}")]
    AuthFailed(String),
    #[error("connection is closed")]
    ConnectionClosed,
    #[error("frame too large: {0}")]
    FrameTooLarge(usize),
    #[error("serialization error: {0}")]
    SerializationError(String),
    #[error("client is closed")]
    Closed,
}

impl FitzError {
    #[must_use]
    pub const fn kind(&self) -> FitzErrorKind {
        match self {
            Self::Authentication { .. } | Self::AuthFailed(_) => FitzErrorKind::Authentication,
            Self::Canceled => FitzErrorKind::Canceled,
            Self::Timeout => FitzErrorKind::Timeout,
            Self::Connection(_) => FitzErrorKind::Connection,
            Self::Transport(_) => FitzErrorKind::Transport,
            Self::Protocol(_)
            | Self::Codec(_)
            | Self::FrameTooLarge(_)
            | Self::SerializationError(_) => FitzErrorKind::Protocol,
            Self::Backpressure(_) => FitzErrorKind::Backpressure,
            Self::StaleHandle => FitzErrorKind::StaleHandle,
            Self::Domain { .. } | Self::DomainError(_) => FitzErrorKind::Domain,
            Self::Closed => FitzErrorKind::Closed,
            Self::ConnectionClosed => FitzErrorKind::ConnectionClosed,
        }
    }

    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Timeout
            | Self::Connection(_)
            | Self::Transport(_)
            | Self::Backpressure(_)
            | Self::ConnectionClosed => true,
            Self::Domain { code, .. } => matches!(code, 1004 | 4005 | 5001 | 6001..=6004),
            _ => false,
        }
    }

    #[must_use]
    pub fn domain_message(&self) -> Option<&str> {
        match self {
            Self::Domain { message, .. } | Self::DomainError(message) => Some(message),
            _ => None,
        }
    }

    #[must_use]
    pub fn is_auth_failure(&self) -> bool {
        matches!(self, Self::Authentication { .. } | Self::AuthFailed(_))
    }
}

impl From<std::io::Error> for FitzError {
    fn from(error: std::io::Error) -> Self {
        if error.kind() == ErrorKind::TimedOut {
            Self::Timeout
        } else {
            Self::Transport(error.to_string())
        }
    }
}

pub type Result<T> = std::result::Result<T, FitzError>;
