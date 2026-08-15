/*
 * ┌────────────┬───────────┬─────────────┬────────────────┐
 * │ Magic (2B) │ MsgID (2B)│ Len (4B)    │ Payload (N B)  │
 * └────────────┴───────────┴─────────────┴────────────────┘
 *
 * Payload = SessionID(8) + Data (N-8)
 */

use bytes::{Buf, BufMut, BytesMut};
use serde::{Deserialize, Serialize};

/// Message Header: Magic(2) +  MsgID(2) + Length(4)
pub const HEADER_SIZE: usize = 8;
const MAGIC_NUM: u16 = 0x7368; // 'sh'

/// The max message size: 64KB
pub const MAX_MSG_SIZE: usize = 65536;

/// The Message ID type
pub type MsgId = u16;

pub type SessionId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub msg_id: MsgId,
    pub session_id: SessionId,
    pub data: Vec<u8>,
}

impl Message {
    pub fn new(msg_id: MsgId, session_id: SessionId, data: Vec<u8>) -> Self {
        Self {
            msg_id,
            session_id,
            data,
        }
    }

    pub fn from_serializable<T: Serialize>(
        msg_id: MsgId,
        session_id: SessionId,
        payload: &T,
    ) -> Result<Self, bincode::Error> {
        let data = bincode::serialize(payload)?;
        Ok(Message::new(msg_id, session_id, data))
    }

    pub fn deserialize_payload<T: for<'de> Deserialize<'de>>(&self) -> Result<T, bincode::Error> {
        bincode::deserialize(&self.data)
    }

    pub fn encode(&self) -> BytesMut {
        let total_len = self.data.len() + 8; // session_id(8) + data
        let mut buf = BytesMut::with_capacity(HEADER_SIZE + total_len);

        // Header
        buf.put_u16(MAGIC_NUM);
        buf.put_u16(self.msg_id);
        buf.put_u32(total_len as u32);

        // Body
        buf.put_u64(self.session_id as u64);
        buf.put_slice(&self.data);

        buf
    }

    pub fn decode(buf: &mut BytesMut) -> Option<Self> {
        if buf.len() < HEADER_SIZE {
            return None;
        }

        let magic_num = u16::from_be_bytes([buf[0], buf[1]]);
        let msg_id = u16::from_be_bytes([buf[2], buf[3]]);
        let body_len = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;

        if magic_num != MAGIC_NUM {
            tracing::error!("Magic num is wrong: {}", magic_num);
            return None;
        }

        if body_len > MAX_MSG_SIZE {
            tracing::error!("Message too large: {} bytes", body_len);
            buf.advance(HEADER_SIZE);
            return None;
        }

        if buf.len() < HEADER_SIZE + body_len {
            return None;
        }

        // Consume header
        buf.advance(HEADER_SIZE);

        // Read body
        let body = buf.split_to(body_len);

        if body.len() < 10 {
            // Need at least session_id(8) + 1 byte data
            return None;
        }

        let session_id = u64::from_be_bytes([
            body[0], body[1], body[2], body[3], body[4], body[5], body[6], body[7],
        ]);
        let data = body[8..].to_vec();

        Some(Message {
            msg_id,
            session_id,
            data,
        })
    }
}

#[derive(Debug)]
pub enum ActorMessage {
    // rpc call
    Call {
        msg: Message,
        reply_tx: tokio::sync::oneshot::Sender<Message>,
    },

    Notify(Message),

    TimerTick {
        timer_id: u64,
    },

    NewSession(SessionId),

    CloseSession(SessionId),

    Stop,
}

#[async_trait::async_trait]
pub trait MessageHandler: Send + Sync + 'static {
    async fn on_message(&self, _msg: Message) {}
    async fn on_session_open(&self, _session_id: SessionId) {}
    async fn on_session_close(&self, _session_id: SessionId) {}
    async fn on_timer(&self, _timer_id: u64) {}
    async fn on_call(&self, _msg: Message) -> Option<Message> {
        None
    }
}

pub fn hex_dump(buf: &[u8]) -> String {
    buf.iter()
        .map(|b| format!("{:02x}", b))
        .collect::<Vec<_>>()
        .join(" ")
}
