use super::Error;
use message::Message;
use bytes::BytesMut;
use tokio_codec::{Encoder, Decoder};
use std::io::Cursor;
use bytes::{Buf, BufMut};
//use nom::Slice;

pub struct PularCodec;

impl Encoder for PularCodec {
    type Item = Message;
    type Error = Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Error> {
        let bytes = item.encode_vec()?;
        if dst.remaining_mut() >= bytes.len() {
            dst.put_slice(&bytes);
            println!("Wrote message {:?}", item);
            Ok(())
        } else {
            Err(Error::Encoding("Insufficient buffer space to encode Message".to_string()))
        }
    }
}

impl Decoder for PularCodec {
    type Item = Message;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Message>, Error> {
        if src.len() >= 4 {
            let mut buf = Cursor::new(src);
            // `messageSize` refers only to _remaining_ message size, so we add 4 to get total frame size
            let message_size = buf.get_u32_be() as usize + 4;
            let src = buf.into_inner();
            if src.len() >= message_size {
                let msg = Message::decode(src.as_ref())?;
                src.advance(message_size);
                println!("Read message {:?}", &msg);
                return Ok(Some(msg))
            }
        }
        Ok(None)
    }
}
