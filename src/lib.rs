pub mod config;
pub mod core;
pub mod error;
pub mod net;

// Re-exports for convenience
pub use config::AppConfig;
pub use core::actor::{Actor, ActorConfig, ActorRef};
pub use core::app::{App, AppContext};
pub use core::message::{ActorMessage, Message, MessageHandler, MsgId, SessionId};
pub use core::router::Router;
pub use core::timer::TimerService;
pub use error::{ShellError, ShellResult};
pub use net::session::{Session, SessionManager};
pub use tokio::time::Duration;
