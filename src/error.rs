use thiserror::Error;

#[derive(Debug, Error)]
pub enum ShellError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Actor not found: {0}")]
    ActorNotFound(String),

    #[error("Session not found: {0}")]
    SessionNotFound(String),

    #[error("Router error: {0}")]
    Router(String),

    #[error("Timer error: {0}")]
    Timer(String),

    #[error("Codec error: {0}")]
    Codec(String),

    #[error("Connection limit reached")]
    ConnectionLimit,

    #[error("Protocol error: {0}")]
    Protocol(String),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Channel closed")]
    ChannelClosed,

    #[error("{0}")]
    Custom(String),
}

impl From<bincode::Error> for ShellError {
    fn from(value: bincode::Error) -> Self {
        ShellError::Serialization(value.to_string())
    }
}

impl From<serde_json::Error> for ShellError {
    fn from(value: serde_json::Error) -> Self {
        ShellError::Serialization(value.to_string())
    }
}

pub type ShellResult<T> = Result<T, ShellError>;
