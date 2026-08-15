use std::sync::Arc;

use tokio::sync::mpsc;

use crate::{
    core::message::{ActorMessage, Message, MessageHandler, SessionId},
    error::{ShellError, ShellResult},
};

#[derive(Debug, Clone)]
pub struct ActorConfig {
    pub name: String,
    pub channel_size: usize,
}

impl Default for ActorConfig {
    fn default() -> Self {
        Self {
            name: "unnamed".to_string(),
            channel_size: 4096,
        }
    }
}

#[derive(Clone)]
pub struct ActorRef {
    pub name: String,
    pub tx: mpsc::Sender<ActorMessage>,
}

impl ActorRef {
    pub async fn timer_tick(&self, timer_id: u64) -> ShellResult<()> {
        self.tx
            .send(ActorMessage::TimerTick { timer_id })
            .await
            .map_err(|_| ShellError::ChannelClosed)
    }

    pub async fn notify(&self, msg: Message) -> ShellResult<()> {
        self.tx
            .send(ActorMessage::Notify(msg))
            .await
            .map_err(|_| ShellError::ChannelClosed)
    }

    pub async fn call(&self, msg: Message) -> ShellResult<Message> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

        self.tx
            .send(ActorMessage::Call {
                msg,
                reply_tx: reply_tx,
            })
            .await
            .map_err(|_| ShellError::ChannelClosed)?;

        reply_rx.await.map_err(|_| ShellError::ChannelClosed)
    }

    pub async fn stop(&self) -> ShellResult<()> {
        self.tx
            .send(ActorMessage::Stop)
            .await
            .map_err(|_| ShellError::ChannelClosed)
    }

    pub async fn notify_new_session(&self, session_id: SessionId) -> ShellResult<()> {
        self.tx
            .send(ActorMessage::NewSession(session_id))
            .await
            .map_err(|_| ShellError::ChannelClosed)
    }

    pub async fn notify_close_session(&self, session_id: SessionId) -> ShellResult<()> {
        self.tx
            .send(ActorMessage::CloseSession(session_id))
            .await
            .map_err(|_| ShellError::ChannelClosed)
    }
}

pub struct Actor {
    pub config: ActorConfig,
    pub actor_ref: ActorRef,
    handler: Arc<dyn MessageHandler>,
    rx: mpsc::Receiver<ActorMessage>,
}

impl Actor {
    pub fn new(config: ActorConfig, handler: Arc<dyn MessageHandler>) -> Self {
        let (tx, rx) = mpsc::channel(config.channel_size);
        let actor_ref = ActorRef {
            name: config.name.clone(),
            tx,
        };

        Self {
            config,
            actor_ref,
            handler,
            rx,
        }
    }

    pub async fn run(mut self) {
        tracing::info!("[Actor:{}] started", self.config.name);

        while let Some(msg) = self.rx.recv().await {
            match msg {
                ActorMessage::Call { msg, reply_tx } => {
                    let handler = self.handler.clone();

                    tokio::spawn(async move {
                        if let Some(response) = handler.on_call(msg).await {
                            let _ = reply_tx.send(response);
                        }
                    });
                }
                ActorMessage::Notify(msg) => {
                    let handler = self.handler.clone();
                    tokio::spawn(async move {
                        handler.on_message(msg).await;
                    });
                }
                ActorMessage::TimerTick { timer_id } => {
                    let handler = self.handler.clone();
                    tokio::spawn(async move {
                        handler.on_timer(timer_id).await;
                    });
                }
                ActorMessage::NewSession(session_id) => {
                    let handler = self.handler.clone();
                    tokio::spawn(async move {
                        handler.on_session_open(session_id).await;
                    });
                }
                ActorMessage::CloseSession(session_id) => {
                    let handler = self.handler.clone();
                    tokio::spawn(async move {
                        handler.on_session_close(session_id).await;
                    });
                }
                ActorMessage::Stop => {
                    tracing::info!("[Actor:{}] received stop signal", self.config.name);
                    break;
                }
            }
        }

        tracing::info!("[Actor:{}] stopped", self.config.name);
    }
}
