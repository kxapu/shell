use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

use crate::core::message::{HEADER_SIZE, MAX_MSG_SIZE, Message};

pub struct MessageCodec;

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < HEADER_SIZE {
            return Ok(None);
        }

        // Peek at the length field
        let body_len = u32::from_be_bytes([src[4], src[5], src[6], src[7]]) as usize;

        if body_len > MAX_MSG_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Message too large: {} bytes", body_len),
            ));
        }

        let total_len = HEADER_SIZE + body_len;
        if src.len() < total_len {
            // Reserve more space
            src.reserve(total_len - src.len());
            return Ok(None);
        }

        // We have enough data, decode
        let msg = Message::decode(src);
        Ok(msg)
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = std::io::Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend_from_slice(&item.data);
        Ok(())
    }
}

pub fn encode_message(msg: &Message) -> bytes::Bytes {
    let buf = msg.encode();
    buf.freeze()
}
