use std::sync::Arc;

use shell::{App, AppConfig, Message, MessageHandler, SessionId};
use tracing_subscriber::EnvFilter;

/// chat handler
struct ChatHandler {
    ctx: shell::AppContext,
}

#[async_trait::async_trait]
impl MessageHandler for ChatHandler {
    async fn on_message(&self, msg: Message) {
        match msg.msg_id {
            1 => {
                // chat
                let text = String::from_utf8_lossy(&msg.data);

                if text == "call" {
                    let msg_clone = msg.clone();
                    match self.ctx.router.call("game", msg_clone).await {
                        Ok(resp) => {
                            let reply = Message::new(2, msg.session_id, resp.data);
                            self.ctx
                                .session_manager
                                .send(msg.session_id, reply.encode().freeze())
                                .await;
                        }
                        Err(e) => {
                            tracing::error!("Failed to call game: {}", e);
                        }
                    }
                    return;
                }

                tracing::info!("[Chat] session={} says: {}", msg.session_id, text);
                let broadcast_msg = Message::new(
                    1,
                    msg.session_id,
                    format!("Player#{}: {}", msg.session_id, text).into_bytes(),
                );

                let encoded = broadcast_msg.encode().freeze();

                self.ctx
                    .session_manager
                    .broadcast_except(msg.session_id, encoded)
                    .await;

                let reply = Message::new(2, msg.session_id, b"Message sent!".to_vec());
                self.ctx
                    .session_manager
                    .send(msg.session_id, reply.encode().freeze())
                    .await;
            }

            _ => {
                tracing::warn!("[Chat] unknown msg_id: {}", msg.msg_id);
            }
        }
    }

    async fn on_session_open(&self, session_id: SessionId) {
        tracing::info!("🟢 Player connected: {}", session_id);
        let notify = Message::new(
            3,
            session_id,
            format!("Player#{} joined the chat!", session_id).into_bytes(),
        );
        let encoded = notify.encode().freeze();

        self.ctx
            .session_manager
            .broadcast_except(session_id, encoded)
            .await;
    }

    async fn on_session_close(&self, session_id: SessionId) {
        tracing::info!("🔴 Player disconnected: {}", session_id);
        // Remove from all rooms
        let notify = Message::new(
            3,
            session_id,
            format!("Player#{} left the chat.", session_id).into_bytes(),
        );
        let encoded = notify.encode().freeze();
        self.ctx.session_manager.broadcast(encoded).await;
    }

    async fn on_timer(&self, _timer_id: u64) {
        tracing::info!("on timer: {}", _timer_id);
    }
}

///game handler
struct GameHandler {
    ctx: shell::AppContext,
}

#[async_trait::async_trait]
impl MessageHandler for GameHandler {
    async fn on_message(&self, msg: Message) {
        match msg.msg_id {
            10 => {
                // move message
                tracing::info!("[Game] move request from session={}", msg.session_id);
                let reply = Message::new(11, msg.session_id, b"move_ok".to_vec());
                self.ctx
                    .session_manager
                    .send(msg.session_id, reply.encode().freeze())
                    .await;
            }
            20 => {
                // atack
                tracing::info!("[Game] attack from session={}", msg.session_id);
                let reply = Message::new(21, msg.session_id, b"attack_ok".to_vec());
                self.ctx
                    .session_manager
                    .send(msg.session_id, reply.encode().freeze())
                    .await;
            }
            _ => {}
        }
    }

    async fn on_timer(&self, _timer_id: u64) {
        tracing::info!("on timer: {}", _timer_id);
    }

    async fn on_call(&self, msg: Message) -> Option<Message> {
        tracing::info!("game on_call: {}", msg.session_id);
        let replay = Message::new(
            0,
            0,
            format!("RPC response from game. and you '{}'", msg.session_id).into_bytes(),
        );
        Some(replay)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .with_thread_ids(true)
        .with_line_number(true)
        .init();

    let config = if std::path::Path::new("config.toml").exists() {
        AppConfig::load("config.toml")?
    } else {
        tracing::warn!("config.toml not found, using defaults");
        AppConfig::default_config()
    };

    let mut app = App::new(config);

    // Register chat actor
    let chat_handler = Arc::new(ChatHandler {
        ctx: app.context.clone(),
    });
    app.register_actor_with_routes(
        "chat",
        chat_handler,
        &[1], // chat msg
    );

    // Register game actor
    let game_handler = Arc::new(GameHandler {
        ctx: app.context.clone(),
    });
    app.register_actor_with_routes(
        "game",
        game_handler,
        &[10, 20], // move, attack
    );

    let timer_id = app
        .context
        .timer_service
        .add_timer("game", shell::Duration::from_secs(5), false)
        .await
        .unwrap();

    tracing::info!("Created one-shot timer: {}", timer_id);

    // Run the application
    if let Err(e) = app.run().await {
        tracing::error!("Server error: {}", e);
    }

    Ok(())
}
