//! low level structures used to send and process raw messages
use std::{convert::TryFrom, io::Cursor};

use bytes::{Buf, BufMut, BytesMut};
use nom::{
    bytes::streaming::take,
    combinator::{map_res, verify},
    number::streaming::{be_u16, be_u32},
    IResult,
};
use prost::{self, Message as ImplProtobuf};

use self::proto::*;
pub use self::proto::{BaseCommand, MessageMetadata as Metadata};
use crate::{connection::RequestKey, error::ConnectionError};

const CRC_CASTAGNOLI: crc::Crc<u32> = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI);

/// Pulsar binary message
///
/// this structure holds any command sent to pulsar, like looking up a topic or
/// subscribing on a topic
#[derive(Debug, Clone)]
pub struct Message {
    /// Basic pulsar command, as defined in Pulsar's protobuf file
    pub command: BaseCommand,
    /// payload for topic messages
    pub payload: Option<Payload>,
}

impl Message {
    /// returns the message's RequestKey if present
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn request_key(&self) -> Option<RequestKey> {
        match &self.command {
            BaseCommand {
                subscribe: Some(CommandSubscribe { request_id, .. }),
                ..
            }
            | BaseCommand {
                partition_metadata: Some(CommandPartitionedTopicMetadata { request_id, .. }),
                ..
            }
            | BaseCommand {
                partition_metadata_response:
                    Some(CommandPartitionedTopicMetadataResponse { request_id, .. }),
                ..
            }
            | BaseCommand {
                lookup_topic: Some(CommandLookupTopic { request_id, .. }),
                ..
            }
            | BaseCommand {
                lookup_topic_response: Some(CommandLookupTopicResponse { request_id, .. }),
                ..
            }
            | BaseCommand {
                producer: Some(CommandProducer { request_id, .. }),
                ..
            }
            | BaseCommand {
                producer_success: Some(CommandProducerSuccess { request_id, .. }),
                ..
            }
            | BaseCommand {
                unsubscribe: Some(CommandUnsubscribe { request_id, .. }),
                ..
            }
            | BaseCommand {
                seek: Some(CommandSeek { request_id, .. }),
                ..
            }
            | BaseCommand {
                close_producer: Some(CommandCloseProducer { request_id, .. }),
                ..
            }
            | BaseCommand {
                success: Some(CommandSuccess { request_id, .. }),
                ..
            }
            | BaseCommand {
                error: Some(CommandError { request_id, .. }),
                ..
            }
            | BaseCommand {
                consumer_stats: Some(CommandConsumerStats { request_id, .. }),
                ..
            }
            | BaseCommand {
                consumer_stats_response: Some(CommandConsumerStatsResponse { request_id, .. }),
                ..
            }
            | BaseCommand {
                get_last_message_id: Some(CommandGetLastMessageId { request_id, .. }),
                ..
            }
            | BaseCommand {
                get_last_message_id_response:
                    Some(CommandGetLastMessageIdResponse { request_id, .. }),
                ..
            }
            | BaseCommand {
                get_topics_of_namespace: Some(CommandGetTopicsOfNamespace { request_id, .. }),
                ..
            }
            | BaseCommand {
                get_topics_of_namespace_response:
                    Some(CommandGetTopicsOfNamespaceResponse { request_id, .. }),
                ..
            }
            | BaseCommand {
                get_schema: Some(CommandGetSchema { request_id, .. }),
                ..
            }
            | BaseCommand {
                get_schema_response: Some(CommandGetSchemaResponse { request_id, .. }),
                ..
            } => Some(RequestKey::RequestId(*request_id)),
            BaseCommand {
                send:
                    Some(CommandSend {
                        producer_id,
                        sequence_id,
                        ..
                    }),
                ..
            }
            | BaseCommand {
                send_error:
                    Some(CommandSendError {
                        producer_id,
                        sequence_id,
                        ..
                    }),
                ..
            }
            | BaseCommand {
                send_receipt:
                    Some(CommandSendReceipt {
                        producer_id,
                        sequence_id,
                        ..
                    }),
                ..
            } => Some(RequestKey::ProducerSend {
                producer_id: *producer_id,
                sequence_id: *sequence_id,
            }),
            BaseCommand {
                active_consumer_change: Some(CommandActiveConsumerChange { consumer_id, .. }),
                ..
            }
            | BaseCommand {
                message: Some(CommandMessage { consumer_id, .. }),
                ..
            }
            | BaseCommand {
                flow: Some(CommandFlow { consumer_id, .. }),
                ..
            }
            | BaseCommand {
                redeliver_unacknowledged_messages:
                    Some(CommandRedeliverUnacknowledgedMessages { consumer_id, .. }),
                ..
            }
            | BaseCommand {
                reached_end_of_topic: Some(CommandReachedEndOfTopic { consumer_id }),
                ..
            }
            | BaseCommand {
                ack: Some(CommandAck { consumer_id, .. }),
                ..
            } => Some(RequestKey::Consumer {
                consumer_id: *consumer_id,
            }),
            BaseCommand {
                close_consumer:
                    Some(CommandCloseConsumer {
                        consumer_id,
                        request_id,
                    }),
                ..
            } => Some(RequestKey::CloseConsumer {
                consumer_id: *consumer_id,
                request_id: *request_id,
            }),
            BaseCommand {
                auth_challenge: Some(CommandAuthChallenge { .. }),
                ..
            } => Some(RequestKey::AuthChallenge),
            BaseCommand {
                connect: Some(_), ..
            }
            | BaseCommand {
                connected: Some(_), ..
            }
            | BaseCommand { ping: Some(_), .. }
            | BaseCommand { pong: Some(_), .. } => None,
            _ => {
                match base_command::Type::try_from(self.command.r#type) {
                    Ok(type_) => {
                        warn!(
                            "Unexpected payload for command of type {:?}. This is likely a bug!",
                            type_
                        );
                    }
                    Err(()) => {
                        warn!(
                            "Received BaseCommand of unexpected type: {}",
                            self.command.r#type
                        );
                    }
                }
                None
            }
        }
    }
}

/// tokio and async-std codec for Pulsar messages
pub struct Codec;

#[cfg(feature = "tokio-runtime")]
impl tokio_util::codec::Encoder<Message> for Codec {
    type Error = ConnectionError;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), ConnectionError> {
        let command_size = item.command.encoded_len();
        let metadata_size = item
            .payload
            .as_ref()
            .map(|p| p.metadata.encoded_len())
            .unwrap_or(0);
        let payload_size = item.payload.as_ref().map(|p| p.data.len()).unwrap_or(0);
        let header_size = if item.payload.is_some() { 18 } else { 8 };
        // Total size does not include the size of the 'totalSize' field, so we subtract 4
        let total_size = command_size + metadata_size + payload_size + header_size - 4;
        let mut buf = Vec::with_capacity(total_size + 4);

        // Simple command frame
        buf.put_u32(total_size as u32);
        buf.put_u32(command_size as u32);
        item.command.encode(&mut buf)?;

        // Payload command frame
        if let Some(payload) = &item.payload {
            buf.put_u16(0x0e01);

            let crc_offset = buf.len();
            buf.put_u32(0); // NOTE: Checksum (CRC32c). Overrwritten later to avoid copying.

            let metdata_offset = buf.len();
            buf.put_u32(metadata_size as u32);
            payload.metadata.encode(&mut buf)?;
            buf.put(&payload.data[..]);

            let crc = CRC_CASTAGNOLI.checksum(&buf[metdata_offset..]);
            let mut crc_buf: &mut [u8] = &mut buf[crc_offset..metdata_offset];
            crc_buf.put_u32(crc);
        }
        if dst.remaining_mut() < buf.len() {
            dst.reserve(buf.len());
        }
        dst.put_slice(&buf);
        trace!("Encoder sending {} bytes", buf.len());
        //        println!("Wrote message {:?}", item);
        Ok(())
    }
}

#[cfg(feature = "tokio-runtime")]
impl tokio_util::codec::Decoder for Codec {
    type Item = Message;
    type Error = ConnectionError;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Message>, ConnectionError> {
        trace!("Decoder received {} bytes", src.len());
        if src.len() >= 4 {
            let mut buf = Cursor::new(src);
            // `messageSize` refers only to _remaining_ message size, so we add 4 to get total frame
            // size
            let message_size = buf.get_u32() as usize + 4;
            let src = buf.into_inner();
            if src.len() >= message_size {
                let msg = {
                    let (buf, command_frame) =
                        command_frame(&src[..message_size]).map_err(|err| {
                            ConnectionError::Decoding(format!(
                                "Error decoding command frame: {err:?}"
                            ))
                        })?;
                    let command = BaseCommand::decode(command_frame.command)?;

                    let payload = if !buf.is_empty() {
                        let (buf, payload_frame) = payload_frame(buf).map_err(|err| {
                            ConnectionError::Decoding(format!(
                                "Error decoding payload frame: {err:?}"
                            ))
                        })?;

                        // TODO: Check crc32 of payload data

                        let metadata = Metadata::decode(payload_frame.metadata)?;
                        Some(Payload {
                            metadata,
                            data: buf.to_vec(),
                        })
                    } else {
                        None
                    };

                    Message { command, payload }
                };

                //TODO advance as we read, rather than this weird post thing
                src.advance(message_size);
                //                println!("Read message {:?}", &msg);
                return Ok(Some(msg));
            }
        }
        Ok(None)
    }
}

#[cfg(feature = "async-std-runtime")]
impl asynchronous_codec::Encoder for Codec {
    type Item = Message;
    type Error = ConnectionError;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), ConnectionError> {
        let command_size = item.command.encoded_len();
        let metadata_size = item
            .payload
            .as_ref()
            .map(|p| p.metadata.encoded_len())
            .unwrap_or(0);
        let payload_size = item.payload.as_ref().map(|p| p.data.len()).unwrap_or(0);
        let header_size = if item.payload.is_some() { 18 } else { 8 };
        // Total size does not include the size of the 'totalSize' field, so we subtract 4
        let total_size = command_size + metadata_size + payload_size + header_size - 4;
        let mut buf = Vec::with_capacity(total_size + 4);

        // Simple command frame
        buf.put_u32(total_size as u32);
        buf.put_u32(command_size as u32);
        item.command.encode(&mut buf)?;

        // Payload command frame
        if let Some(payload) = &item.payload {
            buf.put_u16(0x0e01);

            let crc_offset = buf.len();
            buf.put_u32(0); // NOTE: Checksum (CRC32c). Overrwritten later to avoid copying.

            let metdata_offset = buf.len();
            buf.put_u32(metadata_size as u32);
            payload.metadata.encode(&mut buf)?;
            buf.put(&payload.data[..]);

            let crc = CRC_CASTAGNOLI.checksum(&buf[metdata_offset..]);
            let mut crc_buf: &mut [u8] = &mut buf[crc_offset..metdata_offset];
            crc_buf.put_u32(crc);
        }
        if dst.remaining_mut() < buf.len() {
            dst.reserve(buf.len());
        }
        dst.put_slice(&buf);
        trace!("Encoder sending {} bytes", buf.len());
        //        println!("Wrote message {:?}", item);
        Ok(())
    }
}

#[cfg(feature = "async-std-runtime")]
impl asynchronous_codec::Decoder for Codec {
    type Item = Message;
    type Error = ConnectionError;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Message>, ConnectionError> {
        trace!("Decoder received {} bytes", src.len());
        if src.len() >= 4 {
            let mut buf = Cursor::new(src);
            // `messageSize` refers only to _remaining_ message size, so we add 4 to get total frame
            // size
            let message_size = buf.get_u32() as usize + 4;
            let src = buf.into_inner();
            if src.len() >= message_size {
                let msg = {
                    let (buf, command_frame) =
                        command_frame(&src[..message_size]).map_err(|err| {
                            ConnectionError::Decoding(format!(
                                "Error decoding command frame: {err:?}"
                            ))
                        })?;
                    let command = BaseCommand::decode(command_frame.command)?;

                    let payload = if !buf.is_empty() {
                        let (buf, payload_frame) = payload_frame(buf).map_err(|err| {
                            ConnectionError::Decoding(format!(
                                "Error decoding payload frame: {err:?}"
                            ))
                        })?;

                        // TODO: Check crc32 of payload data

                        let metadata = Metadata::decode(payload_frame.metadata)?;
                        Some(Payload {
                            metadata,
                            data: buf.to_vec(),
                        })
                    } else {
                        None
                    };

                    Message { command, payload }
                };

                //TODO advance as we read, rather than this weird post thing
                src.advance(message_size);
                //                println!("Read message {:?}", &msg);
                return Ok(Some(msg));
            }
        }
        Ok(None)
    }
}

/// message payload
#[derive(Debug, Clone)]
pub struct Payload {
    /// message metadata added by Pulsar
    pub metadata: Metadata,
    /// raw message data
    pub data: Vec<u8>,
}

struct CommandFrame<'a> {
    #[allow(dead_code)]
    total_size: u32,
    #[allow(dead_code)]
    command_size: u32,
    command: &'a [u8],
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
fn command_frame(i: &[u8]) -> IResult<&[u8], CommandFrame> {
    let (i, total_size) = be_u32(i)?;
    let (i, command_size) = be_u32(i)?;
    let (i, command) = take(command_size)(i)?;

    Ok((
        i,
        CommandFrame {
            total_size,
            command_size,
            command,
        },
    ))
}

struct PayloadFrame<'a> {
    #[allow(dead_code)]
    magic_number: u16,
    #[allow(dead_code)]
    checksum: u32,
    #[allow(dead_code)]
    metadata_size: u32,
    metadata: &'a [u8],
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
fn payload_frame(i: &[u8]) -> IResult<&[u8], PayloadFrame> {
    let (i, magic_number) = be_u16(i)?;
    let (i, checksum) = be_u32(i)?;
    let (i, metadata_size) = be_u32(i)?;
    let (i, metadata) = take(metadata_size)(i)?;

    Ok((
        i,
        PayloadFrame {
            magic_number,
            checksum,
            metadata_size,
            metadata,
        },
    ))
}

pub(crate) struct BatchedMessage {
    pub metadata: SingleMessageMetadata,
    pub payload: Vec<u8>,
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
fn batched_message(i: &[u8]) -> IResult<&[u8], BatchedMessage> {
    let (i, metadata_size) = be_u32(i)?;
    let (i, metadata) = verify(
        map_res(take(metadata_size), SingleMessageMetadata::decode),
        // payload_size is defined as i32 in protobuf
        |metadata| metadata.payload_size >= 0,
    )(i)?;

    let (i, payload) = take(metadata.payload_size as u32)(i)?;

    Ok((
        i,
        BatchedMessage {
            metadata,
            payload: payload.to_vec(),
        },
    ))
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
pub(crate) fn parse_batched_message(
    count: u32,
    payload: &[u8],
) -> Result<Vec<BatchedMessage>, ConnectionError> {
    let (_, result) =
        nom::multi::count(batched_message, count as usize)(payload).map_err(|err| {
            ConnectionError::Decoding(format!("Error decoding batched messages: {err:?}"))
        })?;
    Ok(result)
}

impl BatchedMessage {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) fn serialize(&self, w: &mut Vec<u8>) {
        w.put_u32(self.metadata.encoded_len() as u32);
        let _ = self.metadata.encode(w);
        w.put_slice(&self.payload);
    }
}

pub mod proto {
    #![allow(clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/pulsar.proto.rs"));

    //trait implementations used in Consumer::unacked_messages
    impl Eq for MessageIdData {}

    impl std::hash::Hash for MessageIdData {
        #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.ledger_id.hash(state);
            self.entry_id.hash(state);
            self.partition.hash(state);
            self.batch_index.hash(state);
            self.ack_set.hash(state);
            self.batch_size.hash(state);
        }
    }

    pub fn client_version() -> String {
        format!("{}-v{}", "pulsar-rs", env!("CARGO_PKG_VERSION"))
    }
}

impl TryFrom<i32> for base_command::Type {
    type Error = ();

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn try_from(value: i32) -> Result<Self, ()> {
        match value {
            2 => Ok(base_command::Type::Connect),
            3 => Ok(base_command::Type::Connected),
            4 => Ok(base_command::Type::Subscribe),
            5 => Ok(base_command::Type::Producer),
            6 => Ok(base_command::Type::Send),
            7 => Ok(base_command::Type::SendReceipt),
            8 => Ok(base_command::Type::SendError),
            9 => Ok(base_command::Type::Message),
            10 => Ok(base_command::Type::Ack),
            11 => Ok(base_command::Type::Flow),
            12 => Ok(base_command::Type::Unsubscribe),
            13 => Ok(base_command::Type::Success),
            14 => Ok(base_command::Type::Error),
            15 => Ok(base_command::Type::CloseProducer),
            16 => Ok(base_command::Type::CloseConsumer),
            17 => Ok(base_command::Type::ProducerSuccess),
            18 => Ok(base_command::Type::Ping),
            19 => Ok(base_command::Type::Pong),
            20 => Ok(base_command::Type::RedeliverUnacknowledgedMessages),
            21 => Ok(base_command::Type::PartitionedMetadata),
            22 => Ok(base_command::Type::PartitionedMetadataResponse),
            23 => Ok(base_command::Type::Lookup),
            24 => Ok(base_command::Type::LookupResponse),
            25 => Ok(base_command::Type::ConsumerStats),
            26 => Ok(base_command::Type::ConsumerStatsResponse),
            27 => Ok(base_command::Type::ReachedEndOfTopic),
            28 => Ok(base_command::Type::Seek),
            29 => Ok(base_command::Type::GetLastMessageId),
            30 => Ok(base_command::Type::GetLastMessageIdResponse),
            31 => Ok(base_command::Type::ActiveConsumerChange),
            32 => Ok(base_command::Type::GetTopicsOfNamespace),
            33 => Ok(base_command::Type::GetTopicsOfNamespaceResponse),
            34 => Ok(base_command::Type::GetSchema),
            35 => Ok(base_command::Type::GetSchemaResponse),
            36 => Ok(base_command::Type::AuthChallenge),
            37 => Ok(base_command::Type::AuthResponse),
            _ => Err(()),
        }
    }
}

impl From<prost::EncodeError> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(e: prost::EncodeError) -> Self {
        ConnectionError::Encoding(e.to_string())
    }
}

impl From<prost::DecodeError> for ConnectionError {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn from(e: prost::DecodeError) -> Self {
        ConnectionError::Decoding(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    use crate::message::Codec;

    #[test]
    fn parse_simple_command() {
        let input: &[u8] = &[
            0, 0, 0, 34, 0, 0, 0, 30, 8, 2, 18, 26, 10, 16, 112, 117, 108, 115, 97, 114, 45, 114,
            115, 45, 118, 54, 46, 48, 46, 48, 32, 12, 42, 4, 110, 111, 110, 101,
        ];

        let message = Codec.decode(&mut input.into()).unwrap().unwrap();

        {
            let connect = message.command.connect.as_ref().unwrap();
            assert_eq!(connect.client_version, "pulsar-rs-v6.0.0");
            assert_eq!(connect.auth_method_name.as_ref().unwrap(), "none");
            assert_eq!(connect.protocol_version.as_ref().unwrap(), &12);
        }

        let mut output = BytesMut::with_capacity(38);
        Codec.encode(message, &mut output).unwrap();
        assert_eq!(&output, input);
    }

    #[test]
    fn parse_payload_command() {
        let input: &[u8] = &[
            0x00, 0x00, 0x00, 0x3D, 0x00, 0x00, 0x00, 0x08, 0x08, 0x06, 0x32, 0x04, 0x08, 0x00,
            0x10, 0x08, 0x0E, 0x01, 0x42, 0x83, 0x54, 0xB5, 0x00, 0x00, 0x00, 0x19, 0x0A, 0x0E,
            0x73, 0x74, 0x61, 0x6E, 0x64, 0x61, 0x6C, 0x6F, 0x6E, 0x65, 0x2D, 0x30, 0x2D, 0x33,
            0x10, 0x08, 0x18, 0xBE, 0xC0, 0xFC, 0x84, 0xD2, 0x2C, 0x68, 0x65, 0x6C, 0x6C, 0x6F,
            0x2D, 0x70, 0x75, 0x6C, 0x73, 0x61, 0x72, 0x2D, 0x38,
        ];

        let message = Codec.decode(&mut input.into()).unwrap().unwrap();
        {
            let send = message.command.send.as_ref().unwrap();
            assert_eq!(send.producer_id, 0);
            assert_eq!(send.sequence_id, 8);
        }

        {
            let payload = message.payload.as_ref().unwrap();
            assert_eq!(payload.metadata.producer_name, "standalone-0-3");
            assert_eq!(payload.metadata.sequence_id, 8);
            assert_eq!(payload.metadata.publish_time, 1533850624062);
        }

        let mut output = BytesMut::with_capacity(65);
        Codec.encode(message, &mut output).unwrap();
        assert_eq!(&output, input);
    }

    #[test]
    fn base_command_type_parsing() {
        use super::base_command::Type;
        let mut successes = 0;
        for i in 0..40 {
            if let Ok(type_) = Type::try_from(i) {
                successes += 1;
                assert_eq!(type_ as i32, i);
            }
        }
        assert_eq!(successes, 36);
    }
}
