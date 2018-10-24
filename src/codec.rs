use super::Error;
use message::Message;
use bytes::BytesMut;
use tokio_codec::{Encoder, Decoder};
use std::io::Cursor;
use bytes::{Buf, BufMut};
//use nom::Slice;

pub struct PularCodec;
