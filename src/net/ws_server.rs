use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_tungstenite::tungstenite::protocol::Message as WsMessage;

use crate::core::{app::AppContext, message::Message};

pub struct WsServer {
    ctx: AppContext,
}

impl WsServer {
    pub fn new(ctx: AppContext) -> Self {
        WsServer { ctx }
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let addr = &self.ctx.config.websocket.addr;
        let listener = TcpListener::bind(addr).await?;
        tracing::info!("[WebSocket] server listening on {}", addr);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let ctx = self.ctx.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, addr, ctx).await {
                            tracing::error!("[WebSocket] error from {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("[WebSocket] accept error: {}", e);
                }
            }
        }
    }

    async fn handle_connection(
        stream: TcpStream,
        addr: SocketAddr,
        ctx: AppContext,
    ) -> anyhow::Result<()> {
        let ws_stream = tokio_tungstenite::accept_async(stream).await?;
        let (mut ws_sink, mut ws_stream_rx) = ws_stream.split();

        let (send_tx, mut send_rx) = mpsc::channel::<Bytes>(256);

        // Create session
        let session = match ctx.session_manager.create_session(addr, send_tx) {
            Some(s) => s,
            None => {
                tracing::warn!("[WS] connection rejected (limit reached): {}", addr);
                return Ok(());
            }
        };

        let session_id = session.id;

        // Notify session open
        for entry in ctx.router.actors.iter() {
            let _ = entry.value().notify_new_session(session_id).await;
        }

        // Spawn write task
        let write_handle = tokio::spawn(async move {
            while let Some(data) = send_rx.recv().await {
                let ws_msg = WsMessage::Binary(data.to_vec());
                if let Err(e) = ws_sink.send(ws_msg).await {
                    tracing::error!("[WS] write error: {}", e);
                    break;
                }
            }
            let _ = ws_sink.close().await;
        });

        while let Some(result) = ws_stream_rx.next().await {
            match result {
                Ok(ws_msg) => match ws_msg {
                    WsMessage::Binary(data) => {
                        let mut buf = BytesMut::from(&data[..]);
                        if let Some(mut msg) = Message::decode(&mut buf) {
                            msg.session_id = session_id;
                            if let Err(e) = ctx.router.dispatch(msg).await {
                                tracing::warn!("[WS] route error: {}", e);
                            }
                        }
                    }
                    WsMessage::Text(text) => {
                        // Try to parse as JSON message
                        tracing::debug!("[WS] text message: {}", text);
                    }
                    WsMessage::Ping(data) => {
                        // Auto pong handled by tungstenite
                        let _ = data;
                    }
                    WsMessage::Close(_) => {
                        tracing::debug!("[WS] close frame received from {}", addr);
                        break;
                    }
                    _ => {}
                },
                Err(e) => {
                    tracing::debug!("[WS] read error from {}: {}", addr, e);
                    break;
                }
            }
        }

        // Cleanup
        write_handle.abort();
        ctx.session_manager.remove_session(session_id);
        for entry in ctx.router.actors.iter() {
            let _ = entry.value().notify_close_session(session_id).await;
        }

        tracing::info!("[WS] connection closed: {} (session={})", addr, session_id);

        Ok(())
    }
}
