use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::core::message::SessionId;

static SESSION_ID_GEN: AtomicU64 = AtomicU64::new(1);

fn next_session_id() -> SessionId {
    SESSION_ID_GEN.fetch_add(1, Ordering::Relaxed)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SessionState {
    Connected,
    Active,
    Closing,
    Closed,
}

pub struct Session {
    pub id: SessionId,
    pub addr: SocketAddr,
    pub state: SessionState,
    pub send_tx: mpsc::Sender<Bytes>,
    pub userdata: DashMap<String, String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
}

impl Session {
    pub fn new(addr: SocketAddr, send_tx: mpsc::Sender<Bytes>) -> Self {
        Self {
            id: next_session_id(),
            addr,
            state: SessionState::Connected,
            send_tx,
            userdata: DashMap::new(),
            created_at: chrono::Utc::now(),
        }
    }

    /// send data to the client
    pub async fn send(&self, data: Bytes) -> bool {
        self.send_tx.send(data).await.is_ok()
    }

    pub fn set_userdata(&self, key: &str, value: &str) {
        self.userdata.insert(key.to_string(), value.to_string());
    }

    pub fn get_userdata(&self, key: &str) -> Option<String> {
        self.userdata.get(key).map(|v| v.value().clone())
    }
}

pub struct SessionManager {
    sessions: DashMap<SessionId, Arc<Session>>,
    max_connections: usize,
}

impl SessionManager {
    pub fn new(max_connections: usize) -> Self {
        Self {
            sessions: DashMap::new(),
            max_connections,
        }
    }

    pub fn create_session(
        &self,
        addr: SocketAddr,
        send_tx: mpsc::Sender<Bytes>,
    ) -> Option<Arc<Session>> {
        if self.sessions.len() >= self.max_connections {
            tracing::warn!(
                "Connection limit reached: {}/{}",
                self.sessions.len(),
                self.max_connections
            );
            return None;
        }

        let session = Arc::new(Session::new(addr, send_tx));
        let id = session.id;
        self.sessions.insert(id, session.clone());
        tracing::info!(
            "[Session] created: id={}, addr={}, total={}",
            id,
            addr,
            self.sessions.len()
        );
        Some(session)
    }

    pub fn remove_session(&self, session_id: SessionId) -> Option<Arc<Session>> {
        let removed = self.sessions.remove(&session_id).map(|(_, s)| s);
        if removed.is_some() {
            tracing::info!(
                "[Session] removed: id={}, total={}",
                session_id,
                self.sessions.len()
            );
        }
        removed
    }

    pub fn get_session(&self, session_id: SessionId) -> Option<Arc<Session>> {
        self.sessions.get(&session_id).map(|r| r.value().clone())
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    pub async fn send(&self, session_id: SessionId, data: Bytes) {
        if let Some(session) = self.sessions.get(&session_id) {
            session.send(data.clone()).await;
        }
    }

    pub async fn broadcast(&self, data: Bytes) {
        for entry in self.sessions.iter() {
            let session = entry.value();
            let _ = session.send(data.clone()).await;
        }
    }

    pub async fn broadcast_except(&self, except_id: SessionId, data: Bytes) {
        for entry in self.sessions.iter() {
            if *entry.key() != except_id {
                let session = entry.value();
                let _ = session.send(data.clone()).await;
            }
        }
    }
}
