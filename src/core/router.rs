use std::sync::Arc;

use dashmap::DashMap;

use crate::{
    core::{
        actor::ActorRef,
        message::{Message, MsgId},
    },
    error::{ShellError, ShellResult},
};

pub struct Router {
    msg_routes: Arc<DashMap<MsgId, ActorRef>>,
    /// ActorName -> ActorRef
    pub actors: Arc<DashMap<String, ActorRef>>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            msg_routes: Arc::new(DashMap::new()),
            actors: Arc::new(DashMap::new()),
        }
    }

    pub fn register_actor(&self, actor_ref: ActorRef) {
        let name = actor_ref.name.clone();
        tracing::info!("[Router] registered actor: {}", name);
        self.actors.insert(name, actor_ref);
    }

    pub fn register_msg_route(&self, msg_id: MsgId, actor_ref: ActorRef) {
        self.msg_routes.insert(msg_id, actor_ref);
        tracing::debug!("[Router] registered msg route: {} -> actor", msg_id);
    }

    pub fn route_by_msg_id(&self, msg_id: MsgId) -> ShellResult<ActorRef> {
        self.msg_routes
            .get(&msg_id)
            .map(|r| r.value().clone())
            .ok_or_else(|| ShellError::ActorNotFound(format!("msg_id={}", msg_id)))
    }

    pub fn get_actor(&self, name: &str) -> ShellResult<ActorRef> {
        self.actors
            .get(name)
            .map(|r| r.value().clone())
            .ok_or_else(|| ShellError::ActorNotFound(name.to_string()))
    }

    pub async fn dispatch(&self, msg: Message) -> ShellResult<()> {
        self.route_by_msg_id(msg.msg_id)?.notify(msg).await
    }

    /// rpc call
    pub async fn call(&self, name: &str, msg: Message) -> ShellResult<Message> {
        let actor_ref = self.get_actor(name)?;
        actor_ref.call(msg).await
    }

    pub async fn broadcast(&self, msg: Message) {
        for entry in self.actors.iter() {
            let actor_ref = entry.value().clone();
            let msg_clone = msg.clone();
            let _ = actor_ref.notify(msg_clone).await;
        }
    }

    pub fn actor_count(&self) -> usize {
        self.actors.len()
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for Router {
    fn clone(&self) -> Self {
        Router {
            msg_routes: self.msg_routes.clone(),
            actors: self.actors.clone(),
        }
    }
}
