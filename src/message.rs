use crate::connection::RequestKey;
use crate::error::ConnectionError;
use bytes::{Buf, BufMut, BytesMut};
use crc::crc32;
use nom::number::streaming::{be_u16, be_u32};
use prost::{self, Message as ImplProtobuf};
use std::io::Cursor;

pub use self::proto::BaseCommand;
pub use self::proto::MessageMetadata as Metadata;

#[derive(Debug)]
pub struct Message {
    pub command: BaseCommand,
    pub payload: Option<Payload>,
}

impl Message {
    pub fn request_key(&self) -> Option<RequestKey> {
        let command = &self.command;
        command
            .subscribe
            .as_ref()
            .map(|m| m.request_id)
            .or(command.partition_metadata.as_ref().map(|m| m.request_id))
            .or(command
                .partition_metadata_response
                .as_ref()
                .map(|m| m.request_id))
            .or(command.lookup_topic.as_ref().map(|m| m.request_id))
            .or(command.lookup_topic_response.as_ref().map(|m| m.request_id))
            .or(command.producer.as_ref().map(|m| m.request_id))
            .or(command.producer_success.as_ref().map(|m| m.request_id))
            .or(command.unsubscribe.as_ref().map(|m| m.request_id))
            .or(command.seek.as_ref().map(|m| m.request_id))
            .or(command.close_producer.as_ref().map(|m| m.request_id))
            .or(command.close_consumer.as_ref().map(|m| m.request_id))
            .or(command.success.as_ref().map(|m| m.request_id))
            .or(command.producer_success.as_ref().map(|m| m.request_id))
            .or(command.error.as_ref().map(|m| m.request_id))
            .or(command.consumer_stats.as_ref().map(|m| m.request_id))
            .or(command
                .consumer_stats_response
                .as_ref()
                .map(|m| m.request_id))
            .or(command.get_last_message_id.as_ref().map(|m| m.request_id))
            .or(command
                .get_last_message_id_response
                .as_ref()
                .map(|m| m.request_id))
            .or(command
                .get_topics_of_namespace
                .as_ref()
                .map(|m| m.request_id))
            .or(command
                .get_topics_of_namespace_response
                .as_ref()
                .map(|m| m.request_id))
            .or(command.get_schema.as_ref().map(|m| m.request_id))
            .or(command.get_schema_response.as_ref().map(|m| m.request_id))
            .map(|request_id| RequestKey::RequestId(request_id))
            .or(command
                .send_receipt
                .as_ref()
                .map(|r| RequestKey::ProducerSend {
                    producer_id: r.producer_id,
                    sequence_id: r.sequence_id,
                }))
            .or(command
                .send_error
                .as_ref()
                .map(|r| RequestKey::ProducerSend {
                    producer_id: r.producer_id,
                    sequence_id: r.sequence_id,
                }))
    }
}

pub struct Codec;

#[cfg(feature = "tokio-runtime")]
impl tokio_util::codec::Encoder<Message> for Codec {
    type Error = ConnectionError;

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

            let crc = crc32::checksum_castagnoli(&buf[metdata_offset..]);
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

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Message>, ConnectionError> {
        trace!("Decoder received {} bytes", src.len());
        if src.len() >= 4 {
            let mut buf = Cursor::new(src);
            // `messageSize` refers only to _remaining_ message size, so we add 4 to get total frame size
            let message_size = buf.get_u32() as usize + 4;
            let src = buf.into_inner();
            if src.len() >= message_size {
                let msg = {
                    let (buf, command_frame) =
                        command_frame(&src[..message_size]).map_err(|err| {
                            ConnectionError::Decoding(format!(
                                "Error decoding command frame: {:?}",
                                err
                            ))
                        })?;
                    let command = BaseCommand::decode(command_frame.command)?;

                    let payload = if buf.len() > 0 {
                        let (buf, payload_frame) = payload_frame(buf).map_err(|err| {
                            ConnectionError::Decoding(format!(
                                "Error decoding payload frame: {:?}",
                                err
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
impl futures_codec::Encoder for Codec {
    type Item = Message;
    type Error = ConnectionError;

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

            let crc = crc32::checksum_castagnoli(&buf[metdata_offset..]);
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
impl futures_codec::Decoder for Codec {
    type Item = Message;
    type Error = ConnectionError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Message>, ConnectionError> {
        trace!("Decoder received {} bytes", src.len());
        if src.len() >= 4 {
            let mut buf = Cursor::new(src);
            // `messageSize` refers only to _remaining_ message size, so we add 4 to get total frame size
            let message_size = buf.get_u32() as usize + 4;
            let src = buf.into_inner();
            if src.len() >= message_size {
                let msg = {
                    let (buf, command_frame) =
                        command_frame(&src[..message_size]).map_err(|err| {
                            ConnectionError::Decoding(format!(
                                "Error decoding command frame: {:?}",
                                err
                            ))
                        })?;
                    let command = BaseCommand::decode(command_frame.command)?;

                    let payload = if buf.len() > 0 {
                        let (buf, payload_frame) = payload_frame(buf).map_err(|err| {
                            ConnectionError::Decoding(format!(
                                "Error decoding payload frame: {:?}",
                                err
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

#[derive(Debug)]
pub struct Payload {
    pub metadata: Metadata,
    pub data: Vec<u8>,
}

struct CommandFrame<'a> {
    #[allow(dead_code)]
    total_size: u32,
    #[allow(dead_code)]
    command_size: u32,
    command: &'a [u8],
}

#[rustfmt::skip::macros(named)]
named!(command_frame<CommandFrame>,
    do_parse!(
        total_size: be_u32 >>
        command_size: be_u32 >>
        command: take!(command_size) >>

        (CommandFrame {
            total_size,
            command_size,
            command,
        })
    )
);

struct PayloadFrame<'a> {
    #[allow(dead_code)]
    magic_number: u16,
    #[allow(dead_code)]
    checksum: u32,
    #[allow(dead_code)]
    metadata_size: u32,
    metadata: &'a [u8],
}

#[rustfmt::skip::macros(named)]
named!(payload_frame<PayloadFrame>,
    do_parse!(
        magic_number: be_u16 >>
        checksum: be_u32 >>
        metadata_size: be_u32 >>
        metadata: take!(metadata_size) >>

        (PayloadFrame {
            magic_number,
            checksum,
            metadata_size,
            metadata,
        })
    )
);

pub(crate) struct BatchedMessage {
    pub metadata: proto::SingleMessageMetadata,
    pub payload: Vec<u8>,
}

#[rustfmt::skip::macros(named)]
named!(batched_message<BatchedMessage>,
    do_parse!(
        metadata_size: be_u32 >>
        metadata: map_res!(
            take!(metadata_size),
            proto::SingleMessageMetadata::decode
        ) >>
        payload: take!(metadata.payload_size) >>

        (BatchedMessage {
            metadata,
            payload: payload.to_vec(),
        })
    )
);

pub(crate) fn parse_batched_message(
    count: u32,
    payload: &[u8],
) -> Result<Vec<BatchedMessage>, ConnectionError> {
    let (_, result) =
        nom::multi::count(batched_message, count as usize)(payload).map_err(|err| {
            ConnectionError::Decoding(format!("Error decoding batched messages: {:?}", err))
        })?;
    Ok(result)
}

impl BatchedMessage {
    pub(crate) fn serialize(&self, w: &mut Vec<u8>) {
        w.put_u32(self.metadata.encoded_len() as u32);
        let _ = self.metadata.encode(w);
        w.put_slice(&self.payload);
    }
}

#[rustfmt::skip]
pub mod proto {
    #[derive(Clone, PartialEq, Message)]
    pub struct Schema {
        #[prost(string, required, tag="1")]
        pub name: String,
        #[prost(bytes, required, tag="3")]
        pub schema_data: Vec<u8>,
        #[prost(enumeration="schema::Type", required, tag="4")]
        pub type_: i32,
        #[prost(message, repeated, tag="5")]
        pub properties: ::std::vec::Vec<KeyValue>,
    }
    pub mod schema {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
        pub enum Type {
            None = 0,
            String = 1,
            Json = 2,
            Protobuf = 3,
            Avro = 4,
        }
    }
    #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Message)]
    pub struct MessageIdData {
        #[prost(uint64, required, tag="1")]
        pub ledger_id: u64,
        #[prost(uint64, required, tag="2")]
        pub entry_id: u64,
        #[prost(int32, optional, tag="3", default="-1")]
        pub partition: ::std::option::Option<i32>,
        #[prost(int32, optional, tag="4", default="-1")]
        pub batch_index: ::std::option::Option<i32>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct KeyValue {
        #[prost(string, required, tag="1")]
        pub key: String,
        #[prost(string, required, tag="2")]
        pub value: String,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct KeyLongValue {
        #[prost(string, required, tag="1")]
        pub key: String,
        #[prost(uint64, required, tag="2")]
        pub value: u64,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct EncryptionKeys {
        #[prost(string, required, tag="1")]
        pub key: String,
        #[prost(bytes, required, tag="2")]
        pub value: Vec<u8>,
        #[prost(message, repeated, tag="3")]
        pub metadata: ::std::vec::Vec<KeyValue>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct MessageMetadata {
        #[prost(string, required, tag="1")]
        pub producer_name: String,
        #[prost(uint64, required, tag="2")]
        pub sequence_id: u64,
        #[prost(uint64, required, tag="3")]
        pub publish_time: u64,
        #[prost(message, repeated, tag="4")]
        pub properties: ::std::vec::Vec<KeyValue>,
        /// Property set on replicated message,
        /// includes the source cluster name
        #[prost(string, optional, tag="5")]
        pub replicated_from: ::std::option::Option<String>,
        ///key to decide partition for the msg
        #[prost(string, optional, tag="6")]
        pub partition_key: ::std::option::Option<String>,
        /// Override namespace's replication
        #[prost(string, repeated, tag="7")]
        pub replicate_to: ::std::vec::Vec<String>,
        #[prost(enumeration="CompressionType", optional, tag="8", default="None")]
        pub compression: ::std::option::Option<i32>,
        #[prost(uint32, optional, tag="9", default="0")]
        pub uncompressed_size: ::std::option::Option<u32>,
        /// Removed below checksum field from Metadata as
        /// it should be part of send-command which keeps checksum of header + payload
        ///optional sfixed64 checksum = 10;
        /// differentiate single and batch message metadata
        #[prost(int32, optional, tag="11", default="1")]
        pub num_messages_in_batch: ::std::option::Option<i32>,
        /// the timestamp that this event occurs. it is typically set by applications.
        /// if this field is omitted, `publish_time` can be used for the purpose of `event_time`.
        #[prost(uint64, optional, tag="12", default="0")]
        pub event_time: ::std::option::Option<u64>,
        /// Contains encryption key name, encrypted key and metadata to describe the key
        #[prost(message, repeated, tag="13")]
        pub encryption_keys: ::std::vec::Vec<EncryptionKeys>,
        /// Algorithm used to encrypt data key
        #[prost(string, optional, tag="14")]
        pub encryption_algo: ::std::option::Option<String>,
        /// Additional parameters required by encryption
        #[prost(bytes, optional, tag="15")]
        pub encryption_param: ::std::option::Option<Vec<u8>>,
        #[prost(bytes, optional, tag="16")]
        pub schema_version: ::std::option::Option<Vec<u8>>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct SingleMessageMetadata {
        #[prost(message, repeated, tag="1")]
        pub properties: ::std::vec::Vec<KeyValue>,
        #[prost(string, optional, tag="2")]
        pub partition_key: ::std::option::Option<String>,
        #[prost(int32, required, tag="3")]
        pub payload_size: i32,
        #[prost(bool, optional, tag="4", default="false")]
        pub compacted_out: ::std::option::Option<bool>,
        /// the timestamp that this event occurs. it is typically set by applications.
        /// if this field is omitted, `publish_time` can be used for the purpose of `event_time`.
        #[prost(uint64, optional, tag="5", default="0")]
        pub event_time: ::std::option::Option<u64>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandConnect {
        #[prost(string, required, tag="1")]
        pub client_version: String,
        /// Deprecated. Use "auth_method_name" instead.
        #[prost(enumeration="AuthMethod", optional, tag="2")]
        pub auth_method: ::std::option::Option<i32>,
        #[prost(string, optional, tag="5")]
        pub auth_method_name: ::std::option::Option<String>,
        #[prost(bytes, optional, tag="3")]
        pub auth_data: ::std::option::Option<Vec<u8>>,
        #[prost(int32, optional, tag="4", default="0")]
        pub protocol_version: ::std::option::Option<i32>,
        /// Client can ask to be proxyied to a specific broker
        /// This is only honored by a Pulsar proxy
        #[prost(string, optional, tag="6")]
        pub proxy_to_broker_url: ::std::option::Option<String>,
        /// Original principal that was verified by
        /// a Pulsar proxy. In this case the auth info above
        /// will be the auth of the proxy itself
        #[prost(string, optional, tag="7")]
        pub original_principal: ::std::option::Option<String>,
        /// Original auth role and auth Method that was passed
        /// to the proxy. In this case the auth info above
        /// will be the auth of the proxy itself
        #[prost(string, optional, tag="8")]
        pub original_auth_data: ::std::option::Option<String>,
        #[prost(string, optional, tag="9")]
        pub original_auth_method: ::std::option::Option<String>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandConnected {
        #[prost(string, required, tag="1")]
        pub server_version: String,
        #[prost(int32, optional, tag="2", default="0")]
        pub protocol_version: ::std::option::Option<i32>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandSubscribe {
        #[prost(string, required, tag="1")]
        pub topic: String,
        #[prost(string, required, tag="2")]
        pub subscription: String,
        #[prost(enumeration="command_subscribe::SubType", required, tag="3")]
        pub sub_type: i32,
        #[prost(uint64, required, tag="4")]
        pub consumer_id: u64,
        #[prost(uint64, required, tag="5")]
        pub request_id: u64,
        #[prost(string, optional, tag="6")]
        pub consumer_name: ::std::option::Option<String>,
        #[prost(int32, optional, tag="7")]
        pub priority_level: ::std::option::Option<i32>,
        /// Signal wether the subscription should be backed by a
        /// durable cursor or not
        #[prost(bool, optional, tag="8", default="true")]
        pub durable: ::std::option::Option<bool>,
        /// If specified, the subscription will position the cursor
        /// markd-delete position  on the particular message id and
        /// will send messages from that point
        #[prost(message, optional, tag="9")]
        pub start_message_id: ::std::option::Option<MessageIdData>,
        //// Add optional metadata key=value to this consumer
        #[prost(message, repeated, tag="10")]
        pub metadata: ::std::vec::Vec<KeyValue>,
        #[prost(bool, optional, tag="11")]
        pub read_compacted: ::std::option::Option<bool>,
        #[prost(message, optional, tag="12")]
        pub schema: ::std::option::Option<Schema>,
        /// Signal wthether the subscription will initialize on latest
        /// or not -- earliest
        #[prost(enumeration="command_subscribe::InitialPosition", optional, tag="13", default="Latest")]
        pub initial_position: ::std::option::Option<i32>,
    }
    pub mod command_subscribe {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
        pub enum SubType {
            Exclusive = 0,
            Shared = 1,
            Failover = 2,
        }
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
        pub enum InitialPosition {
            Latest = 0,
            Earliest = 1,
        }
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandPartitionedTopicMetadata {
        #[prost(string, required, tag="1")]
        pub topic: String,
        #[prost(uint64, required, tag="2")]
        pub request_id: u64,
        /// TODO - Remove original_principal, original_auth_data, original_auth_method
        /// Original principal that was verified by
        /// a Pulsar proxy.
        #[prost(string, optional, tag="3")]
        pub original_principal: ::std::option::Option<String>,
        /// Original auth role and auth Method that was passed
        /// to the proxy.
        #[prost(string, optional, tag="4")]
        pub original_auth_data: ::std::option::Option<String>,
        #[prost(string, optional, tag="5")]
        pub original_auth_method: ::std::option::Option<String>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandPartitionedTopicMetadataResponse {
        /// Optional in case of error
        #[prost(uint32, optional, tag="1")]
        pub partitions: ::std::option::Option<u32>,
        #[prost(uint64, required, tag="2")]
        pub request_id: u64,
        #[prost(enumeration="command_partitioned_topic_metadata_response::LookupType", optional, tag="3")]
        pub response: ::std::option::Option<i32>,
        #[prost(enumeration="ServerError", optional, tag="4")]
        pub error: ::std::option::Option<i32>,
        #[prost(string, optional, tag="5")]
        pub message: ::std::option::Option<String>,
    }
    pub mod command_partitioned_topic_metadata_response {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
        pub enum LookupType {
            Success = 0,
            Failed = 1,
        }
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandLookupTopic {
        #[prost(string, required, tag="1")]
        pub topic: String,
        #[prost(uint64, required, tag="2")]
        pub request_id: u64,
        #[prost(bool, optional, tag="3", default="false")]
        pub authoritative: ::std::option::Option<bool>,
        /// TODO - Remove original_principal, original_auth_data, original_auth_method
        /// Original principal that was verified by
        /// a Pulsar proxy.
        #[prost(string, optional, tag="4")]
        pub original_principal: ::std::option::Option<String>,
        /// Original auth role and auth Method that was passed
        /// to the proxy.
        #[prost(string, optional, tag="5")]
        pub original_auth_data: ::std::option::Option<String>,
        #[prost(string, optional, tag="6")]
        pub original_auth_method: ::std::option::Option<String>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandLookupTopicResponse {
        /// Optional in case of error
        #[prost(string, optional, tag="1")]
        pub broker_service_url: ::std::option::Option<String>,
        #[prost(string, optional, tag="2")]
        pub broker_service_url_tls: ::std::option::Option<String>,
        #[prost(enumeration="command_lookup_topic_response::LookupType", optional, tag="3")]
        pub response: ::std::option::Option<i32>,
        #[prost(uint64, required, tag="4")]
        pub request_id: u64,
        #[prost(bool, optional, tag="5", default="false")]
        pub authoritative: ::std::option::Option<bool>,
        #[prost(enumeration="ServerError", optional, tag="6")]
        pub error: ::std::option::Option<i32>,
        #[prost(string, optional, tag="7")]
        pub message: ::std::option::Option<String>,
        /// If it's true, indicates to the client that it must
        /// always connect through the service url after the
        /// lookup has been completed.
        #[prost(bool, optional, tag="8", default="false")]
        pub proxy_through_service_url: ::std::option::Option<bool>,
    }
    pub mod command_lookup_topic_response {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
        pub enum LookupType {
            Redirect = 0,
            Connect = 1,
            Failed = 2,
        }
    }
    //// Create a new Producer on a topic, assigning the given producer_id,
    //// all messages sent with this producer_id will be persisted on the topic
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandProducer {
        #[prost(string, required, tag="1")]
        pub topic: String,
        #[prost(uint64, required, tag="2")]
        pub producer_id: u64,
        #[prost(uint64, required, tag="3")]
        pub request_id: u64,
        //// If a producer name is specified, the name will be used,
        //// otherwise the broker will generate a unique name
        #[prost(string, optional, tag="4")]
        pub producer_name: ::std::option::Option<String>,
        #[prost(bool, optional, tag="5", default="false")]
        pub encrypted: ::std::option::Option<bool>,
        //// Add optional metadata key=value to this producer
        #[prost(message, repeated, tag="6")]
        pub metadata: ::std::vec::Vec<KeyValue>,
        #[prost(message, optional, tag="7")]
        pub schema: ::std::option::Option<Schema>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandSend {
        #[prost(uint64, required, tag="1")]
        pub producer_id: u64,
        #[prost(uint64, required, tag="2")]
        pub sequence_id: u64,
        #[prost(int32, optional, tag="3", default="1")]
        pub num_messages: ::std::option::Option<i32>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandSendReceipt {
        #[prost(uint64, required, tag="1")]
        pub producer_id: u64,
        #[prost(uint64, required, tag="2")]
        pub sequence_id: u64,
        #[prost(message, optional, tag="3")]
        pub message_id: ::std::option::Option<MessageIdData>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandSendError {
        #[prost(uint64, required, tag="1")]
        pub producer_id: u64,
        #[prost(uint64, required, tag="2")]
        pub sequence_id: u64,
        #[prost(enumeration="ServerError", required, tag="3")]
        pub error: i32,
        #[prost(string, required, tag="4")]
        pub message: String,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandMessage {
        #[prost(uint64, required, tag="1")]
        pub consumer_id: u64,
        #[prost(message, required, tag="2")]
        pub message_id: MessageIdData,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandAck {
        #[prost(uint64, required, tag="1")]
        pub consumer_id: u64,
        #[prost(enumeration="command_ack::AckType", required, tag="2")]
        pub ack_type: i32,
        /// In case of individual acks, the client can pass a list of message ids
        #[prost(message, repeated, tag="3")]
        pub message_id: ::std::vec::Vec<MessageIdData>,
        #[prost(enumeration="command_ack::ValidationError", optional, tag="4")]
        pub validation_error: ::std::option::Option<i32>,
        #[prost(message, repeated, tag="5")]
        pub properties: ::std::vec::Vec<KeyLongValue>,
    }
    pub mod command_ack {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
        pub enum AckType {
            Individual = 0,
            Cumulative = 1,
        }
        /// Acks can contain a flag to indicate the consumer
        /// received an invalid message that got discarded
        /// before being passed on to the application.
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
        pub enum ValidationError {
            UncompressedSizeCorruption = 0,
            DecompressionError = 1,
            ChecksumMismatch = 2,
            BatchDeSerializeError = 3,
            DecryptionError = 4,
        }
    }
    /// changes on active consumer
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandActiveConsumerChange {
        #[prost(uint64, required, tag="1")]
        pub consumer_id: u64,
        #[prost(bool, optional, tag="2", default="false")]
        pub is_active: ::std::option::Option<bool>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandFlow {
        #[prost(uint64, required, tag="1")]
        pub consumer_id: u64,
        /// Max number of messages to prefetch, in addition
        /// of any number previously specified
        #[prost(uint32, required, tag="2")]
        pub message_permits: u32,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandUnsubscribe {
        #[prost(uint64, required, tag="1")]
        pub consumer_id: u64,
        #[prost(uint64, required, tag="2")]
        pub request_id: u64,
    }
    /// Reset an existing consumer to a particular message id
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandSeek {
        #[prost(uint64, required, tag="1")]
        pub consumer_id: u64,
        #[prost(uint64, required, tag="2")]
        pub request_id: u64,
        #[prost(message, optional, tag="3")]
        pub message_id: ::std::option::Option<MessageIdData>,
    }
    /// Message sent by broker to client when a topic
    /// has been forcefully terminated and there are no more
    /// messages left to consume
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandReachedEndOfTopic {
        #[prost(uint64, required, tag="1")]
        pub consumer_id: u64,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandCloseProducer {
        #[prost(uint64, required, tag="1")]
        pub producer_id: u64,
        #[prost(uint64, required, tag="2")]
        pub request_id: u64,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandCloseConsumer {
        #[prost(uint64, required, tag="1")]
        pub consumer_id: u64,
        #[prost(uint64, required, tag="2")]
        pub request_id: u64,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandRedeliverUnacknowledgedMessages {
        #[prost(uint64, required, tag="1")]
        pub consumer_id: u64,
        #[prost(message, repeated, tag="2")]
        pub message_ids: ::std::vec::Vec<MessageIdData>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandSuccess {
        #[prost(uint64, required, tag="1")]
        pub request_id: u64,
        #[prost(message, optional, tag="2")]
        pub schema: ::std::option::Option<Schema>,
    }
    //// Response from CommandProducer
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandProducerSuccess {
        #[prost(uint64, required, tag="1")]
        pub request_id: u64,
        #[prost(string, required, tag="2")]
        pub producer_name: String,
        /// The last sequence id that was stored by this producer in the previous session
        /// This will only be meaningful if deduplication has been enabled.
        #[prost(int64, optional, tag="3", default="-1")]
        pub last_sequence_id: ::std::option::Option<i64>,
        #[prost(bytes, optional, tag="4")]
        pub schema_version: ::std::option::Option<Vec<u8>>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandError {
        #[prost(uint64, required, tag="1")]
        pub request_id: u64,
        #[prost(enumeration="ServerError", required, tag="2")]
        pub error: i32,
        #[prost(string, required, tag="3")]
        pub message: String,
    }
    /// Commands to probe the state of connection.
    /// When either client or broker doesn't receive commands for certain
    /// amount of time, they will send a Ping probe.
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandPing {
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandPong {
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandConsumerStats {
        #[prost(uint64, required, tag="1")]
        pub request_id: u64,
        /// required string topic_name         = 2;
        /// required string subscription_name  = 3;
        #[prost(uint64, required, tag="4")]
        pub consumer_id: u64,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandConsumerStatsResponse {
        #[prost(uint64, required, tag="1")]
        pub request_id: u64,
        #[prost(enumeration="ServerError", optional, tag="2")]
        pub error_code: ::std::option::Option<i32>,
        #[prost(string, optional, tag="3")]
        pub error_message: ::std::option::Option<String>,
        //// Total rate of messages delivered to the consumer. msg/s
        #[prost(double, optional, tag="4")]
        pub msg_rate_out: ::std::option::Option<f64>,
        //// Total throughput delivered to the consumer. bytes/s
        #[prost(double, optional, tag="5")]
        pub msg_throughput_out: ::std::option::Option<f64>,
        //// Total rate of messages redelivered by this consumer. msg/s
        #[prost(double, optional, tag="6")]
        pub msg_rate_redeliver: ::std::option::Option<f64>,
        //// Name of the consumer
        #[prost(string, optional, tag="7")]
        pub consumer_name: ::std::option::Option<String>,
        //// Number of available message permits for the consumer
        #[prost(uint64, optional, tag="8")]
        pub available_permits: ::std::option::Option<u64>,
        //// Number of unacknowledged messages for the consumer
        #[prost(uint64, optional, tag="9")]
        pub unacked_messages: ::std::option::Option<u64>,
        //// Flag to verify if consumer is blocked due to reaching threshold of unacked messages
        #[prost(bool, optional, tag="10")]
        pub blocked_consumer_on_unacked_msgs: ::std::option::Option<bool>,
        //// Address of this consumer
        #[prost(string, optional, tag="11")]
        pub address: ::std::option::Option<String>,
        //// Timestamp of connection
        #[prost(string, optional, tag="12")]
        pub connected_since: ::std::option::Option<String>,
        //// Whether this subscription is Exclusive or Shared or Failover
        #[prost(string, optional, tag="13")]
        pub type_: ::std::option::Option<String>,
        //// Total rate of messages expired on this subscription. msg/s
        #[prost(double, optional, tag="14")]
        pub msg_rate_expired: ::std::option::Option<f64>,
        //// Number of messages in the subscription backlog
        #[prost(uint64, optional, tag="15")]
        pub msg_backlog: ::std::option::Option<u64>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandGetLastMessageId {
        #[prost(uint64, required, tag="1")]
        pub consumer_id: u64,
        #[prost(uint64, required, tag="2")]
        pub request_id: u64,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandGetLastMessageIdResponse {
        #[prost(message, required, tag="1")]
        pub last_message_id: MessageIdData,
        #[prost(uint64, required, tag="2")]
        pub request_id: u64,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandGetTopicsOfNamespace {
        #[prost(uint64, required, tag="1")]
        pub request_id: u64,
        #[prost(string, required, tag="2")]
        pub namespace: String,
        #[prost(enumeration="get_topics::Mode", optional, tag="3")]
        pub mode: ::std::option::Option<i32>,
    }

    pub mod get_topics {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
        pub enum Mode {
            Persistent = 0,
            NonPersistent = 1,
            All = 2,
        }
    }

    #[derive(Clone, PartialEq, Message)]
    pub struct CommandGetTopicsOfNamespaceResponse {
        #[prost(uint64, required, tag="1")]
        pub request_id: u64,
        #[prost(string, repeated, tag="2")]
        pub topics: ::std::vec::Vec<String>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandGetSchema {
        #[prost(uint64, required, tag="1")]
        pub request_id: u64,
        #[prost(string, required, tag="2")]
        pub topic: String,
        #[prost(bytes, optional, tag="3")]
        pub schema_version: ::std::option::Option<Vec<u8>>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CommandGetSchemaResponse {
        #[prost(uint64, required, tag="1")]
        pub request_id: u64,
        #[prost(enumeration="ServerError", optional, tag="2")]
        pub error_code: ::std::option::Option<i32>,
        #[prost(string, optional, tag="3")]
        pub error_message: ::std::option::Option<String>,
        #[prost(message, optional, tag="4")]
        pub schema: ::std::option::Option<Schema>,
        #[prost(bytes, optional, tag="5")]
        pub schema_version: ::std::option::Option<Vec<u8>>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct BaseCommand {
        #[prost(enumeration="base_command::Type", required, tag="1")]
        pub type_: i32,
        #[prost(message, optional, tag="2")]
        pub connect: ::std::option::Option<CommandConnect>,
        #[prost(message, optional, tag="3")]
        pub connected: ::std::option::Option<CommandConnected>,
        #[prost(message, optional, tag="4")]
        pub subscribe: ::std::option::Option<CommandSubscribe>,
        #[prost(message, optional, tag="5")]
        pub producer: ::std::option::Option<CommandProducer>,
        #[prost(message, optional, tag="6")]
        pub send: ::std::option::Option<CommandSend>,
        #[prost(message, optional, tag="7")]
        pub send_receipt: ::std::option::Option<CommandSendReceipt>,
        #[prost(message, optional, tag="8")]
        pub send_error: ::std::option::Option<CommandSendError>,
        #[prost(message, optional, tag="9")]
        pub message: ::std::option::Option<CommandMessage>,
        #[prost(message, optional, tag="10")]
        pub ack: ::std::option::Option<CommandAck>,
        #[prost(message, optional, tag="11")]
        pub flow: ::std::option::Option<CommandFlow>,
        #[prost(message, optional, tag="12")]
        pub unsubscribe: ::std::option::Option<CommandUnsubscribe>,
        #[prost(message, optional, tag="13")]
        pub success: ::std::option::Option<CommandSuccess>,
        #[prost(message, optional, tag="14")]
        pub error: ::std::option::Option<CommandError>,
        #[prost(message, optional, tag="15")]
        pub close_producer: ::std::option::Option<CommandCloseProducer>,
        #[prost(message, optional, tag="16")]
        pub close_consumer: ::std::option::Option<CommandCloseConsumer>,
        #[prost(message, optional, tag="17")]
        pub producer_success: ::std::option::Option<CommandProducerSuccess>,
        #[prost(message, optional, tag="18")]
        pub ping: ::std::option::Option<CommandPing>,
        #[prost(message, optional, tag="19")]
        pub pong: ::std::option::Option<CommandPong>,
        #[prost(message, optional, tag="20")]
        pub redeliver_unacknowledged_messages: ::std::option::Option<CommandRedeliverUnacknowledgedMessages>,
        #[prost(message, optional, tag="21")]
        pub partition_metadata: ::std::option::Option<CommandPartitionedTopicMetadata>,
        #[prost(message, optional, tag="22")]
        pub partition_metadata_response: ::std::option::Option<CommandPartitionedTopicMetadataResponse>,
        #[prost(message, optional, tag="23")]
        pub lookup_topic: ::std::option::Option<CommandLookupTopic>,
        #[prost(message, optional, tag="24")]
        pub lookup_topic_response: ::std::option::Option<CommandLookupTopicResponse>,
        #[prost(message, optional, tag="25")]
        pub consumer_stats: ::std::option::Option<CommandConsumerStats>,
        #[prost(message, optional, tag="26")]
        pub consumer_stats_response: ::std::option::Option<CommandConsumerStatsResponse>,
        #[prost(message, optional, tag="27")]
        pub reached_end_of_topic: ::std::option::Option<CommandReachedEndOfTopic>,
        #[prost(message, optional, tag="28")]
        pub seek: ::std::option::Option<CommandSeek>,
        #[prost(message, optional, tag="29")]
        pub get_last_message_id: ::std::option::Option<CommandGetLastMessageId>,
        #[prost(message, optional, tag="30")]
        pub get_last_message_id_response: ::std::option::Option<CommandGetLastMessageIdResponse>,
        #[prost(message, optional, tag="31")]
        pub active_consumer_change: ::std::option::Option<CommandActiveConsumerChange>,
        #[prost(message, optional, tag="32")]
        pub get_topics_of_namespace: ::std::option::Option<CommandGetTopicsOfNamespace>,
        #[prost(message, optional, tag="33")]
        pub get_topics_of_namespace_response: ::std::option::Option<CommandGetTopicsOfNamespaceResponse>,
        #[prost(message, optional, tag="34")]
        pub get_schema: ::std::option::Option<CommandGetSchema>,
        #[prost(message, optional, tag="35")]
        pub get_schema_response: ::std::option::Option<CommandGetSchemaResponse>,
    }
    pub mod base_command {
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
        pub enum Type {
            Connect = 2,
            Connected = 3,
            Subscribe = 4,
            Producer = 5,
            Send = 6,
            SendReceipt = 7,
            SendError = 8,
            Message = 9,
            Ack = 10,
            Flow = 11,
            Unsubscribe = 12,
            Success = 13,
            Error = 14,
            CloseProducer = 15,
            CloseConsumer = 16,
            ProducerSuccess = 17,
            Ping = 18,
            Pong = 19,
            RedeliverUnacknowledgedMessages = 20,
            PartitionedMetadata = 21,
            PartitionedMetadataResponse = 22,
            Lookup = 23,
            LookupResponse = 24,
            ConsumerStats = 25,
            ConsumerStatsResponse = 26,
            ReachedEndOfTopic = 27,
            Seek = 28,
            GetLastMessageId = 29,
            GetLastMessageIdResponse = 30,
            ActiveConsumerChange = 31,
            GetTopicsOfNamespace = 32,
            GetTopicsOfNamespaceResponse = 33,
            GetSchema = 34,
            GetSchemaResponse = 35,
        }
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
    pub enum CompressionType {
        None = 0,
        Lz4 = 1,
        Zlib = 2,
        Zstd = 3,
        Snappy = 4,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
    pub enum ServerError {
        UnknownError = 0,
        /// Error with ZK/metadata
        MetadataError = 1,
        /// Error writing reading from BK
        PersistenceError = 2,
        /// Non valid authentication
        AuthenticationError = 3,
        /// Not authorized to use resource
        AuthorizationError = 4,
        /// Unable to subscribe/unsubscribe because
        ConsumerBusy = 5,
        /// other consumers are connected
        ///
        /// Any error that requires client retry operation with a fresh lookup
        ServiceNotReady = 6,
        /// Unable to create producer because backlog quota exceeded
        ProducerBlockedQuotaExceededError = 7,
        /// Exception while creating producer because quota exceeded
        ProducerBlockedQuotaExceededException = 8,
        /// Error while verifying message checksum
        ChecksumError = 9,
        /// Error when an older client/version doesn't support a required feature
        UnsupportedVersionError = 10,
        /// Topic not found
        TopicNotFound = 11,
        /// Subscription not found
        SubscriptionNotFound = 12,
        /// Consumer not found
        ConsumerNotFound = 13,
        /// Error with too many simultaneously request
        TooManyRequests = 14,
        /// The topic has been terminated
        TopicTerminatedError = 15,
        /// Producer with same name is already connected
        ProducerBusy = 16,
        /// The topic name is not valid
        InvalidTopicName = 17,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
    pub enum AuthMethod {
        None = 0,
        YcaV1 = 1,
        Athens = 2,
    }
    /// Each protocol version identify new features that are
    /// incrementally added to the protocol
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
    pub enum ProtocolVersion {
        /// Initial versioning
        V0 = 0,
        /// Added application keep-alive
        V1 = 1,
        /// Added RedeliverUnacknowledgedMessages Command
        V2 = 2,
        /// Added compression with LZ4 and ZLib
        V3 = 3,
        /// Added batch message support
        V4 = 4,
        /// Added disconnect client w/o closing connection
        V5 = 5,
        /// Added checksum computation for metadata + payload
        V6 = 6,
        /// Added CommandLookupTopic - Binary Lookup
        V7 = 7,
        /// Added CommandConsumerStats - Client fetches broker side consumer stats
        V8 = 8,
        /// Added end of topic notification
        V9 = 9,
        /// Added proxy to broker
        V10 = 10,
        /// C++ consumers before this version are not correctly handling the checksum field
        V11 = 11,
        /// Added get topic's last messageId from broker
        V12 = 12,
        /// Added CommandActiveConsumerChange
        /// Added CommandGetTopicsOfNamespace
        ///
        /// Schema-registry : added avro schema format for json
        V13 = 13,
    }
}

impl From<prost::EncodeError> for ConnectionError {
    fn from(e: prost::EncodeError) -> Self {
        ConnectionError::Encoding(e.to_string())
    }
}

impl From<prost::DecodeError> for ConnectionError {
    fn from(e: prost::DecodeError) -> Self {
        ConnectionError::Decoding(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::message::Codec;
    use bytes::BytesMut;
    use tokio_util::codec::{Decoder, Encoder};

    #[test]
    fn parse_simple_command() {
        let input: &[u8] = &[
            0x00, 0x00, 0x00, 0x22, 0x00, 0x00, 0x00, 0x1E, 0x08, 0x02, 0x12, 0x1A, 0x0A, 0x10,
            0x32, 0x2E, 0x30, 0x2E, 0x31, 0x2D, 0x69, 0x6E, 0x63, 0x75, 0x62, 0x61, 0x74, 0x69,
            0x6E, 0x67, 0x20, 0x0C, 0x2A, 0x04, 0x6E, 0x6F, 0x6E, 0x65,
        ];

        let message = Codec.decode(&mut input.into()).unwrap().unwrap();

        {
            let connect = message.command.connect.as_ref().unwrap();
            assert_eq!(connect.client_version, "2.0.1-incubating");
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
}
