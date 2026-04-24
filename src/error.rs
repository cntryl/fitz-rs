use std::io::ErrorKind;

/// Fitz client error categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FitzErrorKind {
    Connection,
    Transport,
    Codec,
    Protocol,
    Domain,
    Auth,
    Timeout,
    ConnectionClosed,
    FrameTooLarge,
    Jwt,
    Io,
    Serialization,
}

/// Fitz client error types.
#[derive(Debug, thiserror::Error)]
pub enum FitzError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Transport error: {0}")]
    Transport(String),

    #[error("Codec error: {0}")]
    Codec(String),

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Domain error: {0}")]
    DomainError(String),

    #[error("Auth failed: {0}")]
    AuthFailed(String),

    #[error("Timeout")]
    Timeout,

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Frame too large: {0}")]
    FrameTooLarge(usize),

    #[error("JWT error: {0}")]
    JwtError(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(String),
}

impl FitzError {
    pub fn kind(&self) -> FitzErrorKind {
        match self {
            Self::Connection(_) => FitzErrorKind::Connection,
            Self::Transport(_) => FitzErrorKind::Transport,
            Self::Codec(_) => FitzErrorKind::Codec,
            Self::Protocol(_) => FitzErrorKind::Protocol,
            Self::DomainError(_) => FitzErrorKind::Domain,
            Self::AuthFailed(_) => FitzErrorKind::Auth,
            Self::Timeout => FitzErrorKind::Timeout,
            Self::ConnectionClosed => FitzErrorKind::ConnectionClosed,
            Self::FrameTooLarge(_) => FitzErrorKind::FrameTooLarge,
            Self::JwtError(_) => FitzErrorKind::Jwt,
            Self::Io(_) => FitzErrorKind::Io,
            Self::SerializationError(_) => FitzErrorKind::Serialization,
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Timeout | Self::ConnectionClosed | Self::Transport(_) | Self::Connection(_) => {
                true
            }
            Self::Io(err) => matches!(
                err.kind(),
                ErrorKind::Interrupted
                    | ErrorKind::TimedOut
                    | ErrorKind::WouldBlock
                    | ErrorKind::ConnectionReset
                    | ErrorKind::ConnectionAborted
                    | ErrorKind::BrokenPipe
                    | ErrorKind::UnexpectedEof
                    | ErrorKind::NotConnected
            ),
            _ => false,
        }
    }

    pub fn is_auth_failure(&self) -> bool {
        matches!(self, Self::AuthFailed(_))
    }

    pub fn domain_message(&self) -> Option<&str> {
        match self {
            Self::DomainError(message) => Some(message.as_str()),
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, FitzError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_classify_retryable_transport_errors() {
        assert!(FitzError::Timeout.is_retryable());
        assert!(FitzError::ConnectionClosed.is_retryable());
        assert!(!FitzError::AuthFailed("nope".into()).is_retryable());
        assert!(!FitzError::DomainError("bad request".into()).is_retryable());
    }

    #[test]
    fn should_expose_domain_message() {
        let err = FitzError::DomainError("duplicate key".into());
        assert_eq!(err.domain_message(), Some("duplicate key"));
        assert_eq!(err.kind(), FitzErrorKind::Domain);
        assert!(!err.is_auth_failure());
    }
}
