use std::net::SocketAddr;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_util::codec::Framed;

use crate::{
    core::{
        app::AppContext,
        message::{Message, SessionId},
    },
    net::codec::MessageCodec,
};

pub struct TcpServer {
    ctx: AppContext,
}

impl TcpServer {
    pub fn new(ctx: AppContext) -> Self {
        Self { ctx }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        let addr = &self.ctx.config.tcp.addr;
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("[TCP] server listening on {}", addr);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let ctx = self.ctx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, addr, ctx).await {
                            tracing::error!("[TCP] connection error from {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("[TCP] accept error: {}", e);
                }
            }
        }
    }

    async fn handle_connection(
        stream: TcpStream,
        addr: SocketAddr,
        ctx: AppContext,
    ) -> anyhow::Result<()> {
        // Set TCP options
        stream.set_nodelay(true)?;

        let framed = Framed::new(stream, MessageCodec);
        let (mut sink, mut stream_rx) = framed.split();

        // Create send channel for this connection
        let (send_tx, mut send_rx) = mpsc::channel::<Bytes>(256);

        let session = match ctx.session_manager.create_session(addr, send_tx) {
            Some(s) => s,
            None => {
                tracing::warn!("[TCP] connection rejected (limit reached): {}", addr);
                return Ok(());
            }
        };

        let session_id = session.id;

        // Notify actors about new session
        Self::notify_session_open(&ctx, session_id).await;

        // Spawn write task
        let write_handle = tokio::spawn(async move {
            while let Some(data) = send_rx.recv().await {
                if let Err(e) = sink.send(Message::new(0, 0, data.to_vec())).await {
                    tracing::error!("[TCP] write error: {}", e);
                    break;
                }
            }
        });

        // Read loop
        while let Some(result) = stream_rx.next().await {
            match result {
                Ok(msg) => {
                    let msg = Message { session_id, ..msg };

                    if let Err(e) = ctx.router.dispatch(msg).await {
                        tracing::warn!("[TCP] route error: {}", e);
                    }
                }
                Err(e) => {
                    tracing::debug!("[TCP] read error from {}: {}", addr, e);
                    break;
                }
            }
        }

        // Cleanup
        write_handle.abort();
        ctx.session_manager.remove_session(session_id);
        Self::notify_session_close(&ctx, session_id).await;

        tracing::info!("[TCP] connection closed: {} (session={})", addr, session_id);
        Ok(())
    }

    async fn notify_session_open(ctx: &AppContext, session_id: SessionId) {
        // Notify all registered actors
        for entry in ctx.router.actors.iter() {
            let _ = entry.value().notify_new_session(session_id).await;
        }
    }

    async fn notify_session_close(ctx: &AppContext, session_id: SessionId) {
        for entry in ctx.router.actors.iter() {
            let _ = entry.value().notify_close_session(session_id).await;
        }
    }
}
