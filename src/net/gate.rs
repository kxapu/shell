use crate::{
    core::app::AppContext,
    net::{tcp_server::TcpServer, ws_server::WsServer},
};

pub struct Gate {
    ctx: AppContext,
}

impl Gate {
    pub fn new(ctx: AppContext) -> Self {
        Self { ctx }
    }

    pub async fn start(self) -> anyhow::Result<()> {
        let mut handles = Vec::new();

        // Start TCP server
        if self.ctx.config.tcp.enabled {
            let tcp_server = TcpServer::new(self.ctx.clone());
            let handle = tokio::spawn(async move {
                if let Err(e) = tcp_server.start().await {
                    tracing::error!("[Gate] TCP server error: {}", e);
                }
            });
            handles.push(handle);
        }

        // Start WebSocket server
        if self.ctx.config.websocket.enabled {
            let ws_server = WsServer::new(self.ctx.clone());
            let handle = tokio::spawn(async move {
                if let Err(e) = ws_server.start().await {
                    tracing::error!("[Gate] WebSocket server error: {}", e);
                }
            });
            handles.push(handle);
        }

        if handles.is_empty() {
            tracing::warn!("[Gate] no network server enabled!");
            return Ok(());
        }

        // Wait for all servers
        for handle in handles {
            let _ = handle.await;
        }

        Ok(())
    }
}
