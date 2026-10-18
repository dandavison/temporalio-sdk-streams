use crate::proto::service::StreamFailureType;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A stream operation failed for a reason the stream service names.
    #[error("{}: {message}", failure_type.as_str_name())]
    Stream {
        failure_type: StreamFailureType,
        message: String,
    },
    /// A failure that a retry may not repeat, such as a dropped connection. Retried until the
    /// retry deadline, then reported as [`Error::Unavailable`].
    #[error("transient: {0}")]
    Transient(String),
    /// A transient failure that lasted past the retry deadline.
    #[error("unavailable: {0}")]
    Unavailable(String),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("internal: {0}")]
    Internal(String),
}

impl Error {
    pub fn stream(failure_type: StreamFailureType, message: impl Into<String>) -> Self {
        Self::Stream {
            failure_type,
            message: message.into(),
        }
    }

    /// The name an SDK maps to its error type: a StreamFailureType value's name, or the kind of
    /// a non-stream failure.
    pub fn type_name(&self) -> &str {
        match self {
            Self::Stream { failure_type, .. } => failure_type.as_str_name(),
            Self::Transient(_) | Self::Unavailable(_) => "UNAVAILABLE",
            Self::InvalidArgument(_) => "INVALID_ARGUMENT",
            Self::Internal(_) => "INTERNAL",
        }
    }
}

impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Self {
        Self::InvalidArgument(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
