use std::sync::Arc;

use tokio::sync::mpsc;

use crate::{
    config::AppConfig,
    core::{
        actor::{Actor, ActorConfig, ActorRef},
        message::MessageHandler,
        router::Router,
        timer::{TimerEvent, TimerService},
    },
    net::{gate::Gate, session::SessionManager},
};

#[derive(Clone)]
pub struct AppContext {
    pub config: Arc<AppConfig>,
    pub router: Router,
    pub session_manager: Arc<SessionManager>,
    pub timer_service: TimerService,
}

pub struct App {
    pub context: AppContext,
    actors: Vec<tokio::task::JoinHandle<()>>,
}

impl App {
    pub fn new(config: AppConfig) -> Self {
        let router = Router::new();
        let session_manager = Arc::new(SessionManager::new(
            config.tcp.max_connections + config.websocket.max_connections,
        ));

        let (timer_service, timer_rx) = TimerService::new(config.timer.tick_interval_ms);

        let context = AppContext {
            config: Arc::new(config),
            router,
            session_manager,
            timer_service,
        };

        Self::timer_handle(context.router.clone(), timer_rx);

        Self {
            context,
            actors: Vec::new(),
        }
    }

    fn timer_handle(router: Router, mut timer_rx: mpsc::Receiver<TimerEvent>) {
        tokio::spawn(async move {
            while let Some(ev) = timer_rx.recv().await {
                match router.get_actor(&ev.actor_name) {
                    Ok(actor_ref) => {
                        if let Err(e) = actor_ref.timer_tick(ev.timer_id).await {
                            tracing::error!("Failed to handle timer tick: {}", e.to_string());
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to  hande the timer: {}", e.to_string());
                    }
                }
            }
        });
    }

    pub fn register_actor(&mut self, name: &str, handler: Arc<dyn MessageHandler>) -> ActorRef {
        let config = ActorConfig {
            name: name.to_string(),
            channel_size: 4096,
        };

        let actor = Actor::new(config, handler);
        let actor_ref = actor.actor_ref.clone();

        self.context.router.register_actor(actor_ref.clone());

        let handle = tokio::spawn(async move {
            actor.run().await;
        });

        self.actors.push(handle);

        actor_ref
    }

    pub fn register_actor_with_routes(
        &mut self,
        name: &str,
        handler: Arc<dyn MessageHandler>,
        msg_ids: &[u16],
    ) -> ActorRef {
        let actor_ref = self.register_actor(name, handler);

        for &msg_id in msg_ids {
            self.context
                .router
                .register_msg_route(msg_id, actor_ref.clone());
        }

        actor_ref
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let ctx = self.context.clone();

        tracing::info!(
            "\n===================================\n\
            Shell Server Starting\n\
            Name: {}\n\
            ===================================",
            ctx.config.server.name
        );

        let gate = Gate::new(ctx.clone());
        let gate_handle = tokio::spawn(async move {
            if let Err(e) = gate.start().await {
                tracing::error!("[Gate] error: {}", e);
            }
        });

        // Waiting for all actors
        for handle in self.actors {
            let _ = handle.await;
        }

        let _ = gate_handle.await;

        Ok(())
    }
}
