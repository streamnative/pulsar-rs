use chrono::Utc;

use crate::connection::{Authentication, CLIENT_VERSION, ConnectionOptions};
use crate::proto::{self, base_command::Type as CommandType, command_subscribe::SubType, Message, Payload, ServerError, MessageIdData};
use crate::producer::{self, ProducerOptions};
use crate::consumer::ConsumerOptions;
use crate::util::SerialId;
use crate::proto::command_ack::ValidationError;
use crate::error::Error;
use crate::proto::command_lookup_topic_response::LookupType as TopicLookupType;
use crate::proto::command_partitioned_topic_metadata_response::LookupType as PartitionLookupType;
use std::collections::BTreeMap;
use std::convert::TryInto;

pub(crate) trait IntoProto {
    fn into_proto(self, request_ids: &mut SerialId) -> proto::Message;
}

pub(crate) trait FromProto {
    fn from_proto(proto: proto::Message) -> Result<Self, Error>;
}

pub(crate) struct Connect {
    pub auth_method: Option<proto::AuthMethod>,
    pub auth_method_name: Option<String>,
    pub auth_data: Option<Vec<u8>>,
    pub proxy_to_broker_url: Option<String>,
    pub original_principal: Option<String>,
    pub original_auth_data: Option<String>,
    pub original_auth_method: Option<String>,
}

impl IntoProto for Connect {
    fn into_proto(self, request_ids: &mut SerialId) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Connect as i32,
                connect: Some(proto::CommandConnect {
                    client_version: String::from(CLIENT_VERSION),
                    protocol_version: Some(12),
                    auth_method_name: self.auth_method_name,
                    auth_data: self.auth_data,
                    proxy_to_broker_url: self.proxy_to_broker_url,
                    original_principal: self.original_principal,
                    original_auth_data: self.original_auth_data,
                    auth_method: self.auth_method.map(|a| a as i32),
                    original_auth_method: self.original_auth_method
                }),
                ..Default::default()
            },
            payload: None,
        }
    }
}

pub(crate) struct Ping;

impl IntoProto for Ping {
    fn into_proto(self, request_ids: &mut SerialId) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Ping as i32,
                ping: Some(proto::CommandPing {}),
                ..Default::default()
            },
            payload: None,
        }
    }
}

impl FromProto for Ping {
    fn from_proto(proto: Message) -> Result<Self, Error> {
        match check_error(proto)?.command {
            proto::BaseCommand { ping: Some(proto::CommandPing {}), .. } => {
                Ok(Ping)
            },
            other => {
                Err(Error::unexpected_response(format!("Expected `ping`, found {:?}", other)))
            }
        }
    }
}

pub(crate) struct Pong;

impl IntoProto for Pong {
    fn into_proto(self, request_ids: &mut SerialId) -> Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Pong as i32,
                pong: Some(proto::CommandPong {}),
                ..Default::default()
            },
            payload: None,
        }
    }
}

impl FromProto for Pong {
    fn from_proto(proto: Message) -> Result<Self, Error> {
        match check_error(proto)?.command {
            proto::BaseCommand { pong: Some(proto::CommandPong {}), .. } => {
                Ok(Pong)
            },
            other => {
                Err(Error::unexpected_response(format!("Expected `pong`, found {:?}", other)))
            }
        }
    }
}

pub(crate) struct CreateProducer {
    pub topic: String,
    pub producer_id: u64,
    pub options: Option<ProducerOptions>,
}

impl IntoProto for CreateProducer {
    fn into_proto(self, request_ids: &mut SerialId) -> proto::Message {
        let options = self.options.unwrap_or_default();
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Producer as i32,
                producer: Some(proto::CommandProducer {
                    topic: self.topic,
                    producer_id: self.producer_id,
                    request_id: request_ids.next(),
                    producer_name: options.producer_name,
                    encrypted: options.encrypted,
                    metadata: options.metadata.map(to_key_value).unwrap_or_default(),
                    schema: options.schema,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }
}

pub(crate) struct GetTopicsOfNamespace {
    namespace: String,
    mode: proto::get_topics::Mode,
}

impl IntoProto for GetTopicsOfNamespace {
    fn into_proto(self, request_ids: &mut SerialId) -> proto::Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::GetTopicsOfNamespace as i32,
                get_topics_of_namespace: Some(proto::CommandGetTopicsOfNamespace {
                    request_id: request_ids.next(),
                    namespace: self.namespace,
                    mode: Some(self.mode as i32),
                }),
                ..Default::default()
            },
            payload: None,
        }
    }
}

pub(crate) struct Send {
    producer_id: u64,
    producer_name: String,
    sequence_id: u64,
    num_messages: Option<i32>,
    message: producer::Message,
}

impl IntoProto for Send {
    fn into_proto(self, request_ids: &mut SerialId) -> proto::Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Send as i32,
                send: Some(proto::CommandSend {
                    producer_id: self.producer_id,
                    sequence_id: self.sequence_id,
                    num_messages: self.num_messages,
                }),
                ..Default::default()
            },
            payload: Some(Payload {
                metadata: proto::MessageMetadata {
                    producer_name: self.producer_name,
                    sequence_id: self.sequence_id,
                    properties: self.message.properties.map(to_key_value).unwrap_or_default(),
                    publish_time: Utc::now().timestamp_millis() as u64,
                    replicated_from: None,
                    partition_key: self.message.partition_key,
                    replicate_to: self.message.replicate_to,
                    compression: self.message.compression,
                    uncompressed_size: self.message.uncompressed_size,
                    num_messages_in_batch: self.message.num_messages_in_batch,
                    event_time: self.message.event_time,
                    encryption_keys: self.message.encryption_keys,
                    encryption_algo: self.message.encryption_algo,
                    encryption_param: self.message.encryption_param,
                    schema_version: self.message.schema_version,
                },
                data: self.message.payload,
            }),
        }
    }
}

pub(crate) struct LookupTopic {
    topic: String,
    authoritative: Option<bool>,
}

impl IntoProto for LookupTopic {
    fn into_proto(self, request_ids: &mut SerialId) -> proto::Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Lookup as i32,
                lookup_topic: Some(proto::CommandLookupTopic {
                    topic: self.topic,
                    request_id: request_ids.next(),
                    authoritative: self.authoritative,
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }
}

pub(crate) struct LookupPartitionedTopic {
    topic: String,
}

impl IntoProto for LookupPartitionedTopic {
    fn into_proto(self, request_ids: &mut SerialId) -> proto::Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::PartitionedMetadata as i32,
                partition_metadata: Some(proto::CommandPartitionedTopicMetadata {
                    topic: self.topic,
                    request_id: request_ids.next(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            payload: None,
        }
    }
}

pub(crate) struct CloseProducer {
    producer_id: u64,
}

impl IntoProto for CloseProducer {
    fn into_proto(self, request_ids: &mut SerialId) -> proto::Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::CloseProducer as i32,
                close_producer: Some(proto::CommandCloseProducer {
                    producer_id: self.producer_id,
                    request_id: request_ids.next(),
                }),
                ..Default::default()
            },
            payload: None,
        }
    }
}

impl FromProto for CloseProducer {
    fn from_proto(proto: Message) -> Result<Self, Error> {
        match check_error(proto)?.command {
            proto::BaseCommand { close_producer: Some(proto::CommandCloseProducer { producer_id, .. }), .. } => {
                Ok(CloseProducer {
                    producer_id
                })
            },
            other => {
                Err(Error::unexpected_response(format!("Expected `close_producer`, found {:?}", other)))
            }
        }
    }
}

pub(crate) struct Subscribe {
    topic: String,
    subscription: String,
    sub_type: SubType,
    consumer_id: u64,
    consumer_name: Option<String>,
    options: Option<ConsumerOptions>,
}

impl IntoProto for Subscribe {
    fn into_proto(self, request_ids: &mut SerialId) -> proto::Message {
        let options = self.options.unwrap_or_default();
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Subscribe as i32,
                subscribe: Some(proto::CommandSubscribe {
                    topic: self.topic,
                    subscription: self.subscription,
                    sub_type: self.sub_type as i32,
                    consumer_id: self.consumer_id,
                    request_id: request_ids.next(),
                    consumer_name: self.consumer_name,
                    priority_level: options.priority_level,
                    durable: options.durable,
                    metadata: options.metadata.map(to_key_value).unwrap_or_default(),
                    read_compacted: options.read_compacted,
                    initial_position: options.initial_position,
                    schema: options.schema,
                    start_message_id: options.start_message_id,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }
}

pub(crate) struct Flow {
    consumer_id: u64,
    message_permits: u32
}

impl IntoProto for Flow {
    fn into_proto(self, request_ids: &mut SerialId) -> proto::Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Flow as i32,
                flow: Some(proto::CommandFlow {
                    consumer_id: self.consumer_id,
                    message_permits: self.message_permits,
                }),
                ..Default::default()
            },
            payload: None,
        }
    }
}

pub(crate) struct Ack {
    consumer_id: u64,
    message_id: Vec<proto::MessageIdData>,
    cumulative: bool,
    validation_error: Option<ValidationError>,
    properties: Option<BTreeMap<String, u64>>,
}

impl IntoProto for Ack {
    fn into_proto(self, request_ids: &mut SerialId) -> proto::Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::Ack as i32,
                ack: Some(proto::CommandAck {
                    consumer_id: self.consumer_id,
                    ack_type: if self.cumulative {
                        proto::command_ack::AckType::Cumulative as i32
                    } else {
                        proto::command_ack::AckType::Individual as i32
                    },
                    message_id: self.message_id,
                    validation_error: self.validation_error.map(|e| e as i32),
                    properties: self.properties.map(to_key_long_value).unwrap_or_default(),
                }),
                ..Default::default()
            },
            payload: None,
        }

    }
}

pub(crate) struct RedeliverUnacknowlegedMessages {
    consumer_id: u64,
    message_ids: Vec<proto::MessageIdData>,
}

impl IntoProto for RedeliverUnacknowlegedMessages {
    fn into_proto(self, request_ids: &mut SerialId) -> proto::Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::RedeliverUnacknowledgedMessages as i32,
                redeliver_unacknowledged_messages: Some(
                    proto::CommandRedeliverUnacknowledgedMessages {
                        consumer_id: self.consumer_id,
                        message_ids: self.message_ids,
                    },
                ),
                ..Default::default()
            },
            payload: None,
        }
    }
}

pub(crate) struct CloseConsumer {
    consumer_id: u64,
}

impl IntoProto for CloseConsumer {
    fn into_proto(self, request_ids: &mut SerialId) -> proto::Message {
        Message {
            command: proto::BaseCommand {
                type_: CommandType::CloseConsumer as i32,
                close_consumer: Some(proto::CommandCloseConsumer {
                    consumer_id: self.consumer_id,
                    request_id: request_ids.next(),
                }),
                ..Default::default()
            },
            payload: None,
        }
    }
}

impl FromProto for CloseConsumer {
    fn from_proto(proto: Message) -> Result<Self, Error> {
        match check_error(proto)?.command {
            proto::BaseCommand { close_consumer: Some(proto::CommandCloseConsumer { consumer_id, .. }), .. } => {
                Ok(CloseConsumer {
                    consumer_id
                })
            },
            other => {
                Err(Error::unexpected_response(format!("Expected `CloseConsumer`, found {:?}", other)))
            }
        }
    }
}

pub(crate) struct LookupTopicResponse {
    pub broker_service_url: String,
    pub broker_service_url_tls: Option<String>,
    pub response: TopicLookupType,
    pub authoritative: bool,
    pub proxy_through_service_url: bool,
}

impl FromProto for LookupTopicResponse {
    fn from_proto(proto: Message) -> Result<Self, Error> {
        match check_error(proto)?.command {
            proto::BaseCommand { lookup_topic_response: Some(response), .. } => {
                if let Some(error) = response.error {
                    return Err(Error::pulsar(error.try_into()?, response.message.unwrap_or_else(|| "<no message>".to_owned())));
                }
                match (response.broker_service_url, response.response, response.authoritative) {
                    (Some(broker_service_url), Some(lookup_type), Some(authoritative)) => {
                        Ok(LookupTopicResponse {
                            broker_service_url,
                            broker_service_url_tls: response.broker_service_url_tls,
                            response: lookup_type.try_into()?,
                            authoritative,
                            proxy_through_service_url: response.proxy_through_service_url.unwrap_or(false)
                        })
                    },
                    _ => Err(Error::decoding(format!("Expected LookupTopicResponse. Found: {:?}", resposne)))
                }
            },
            other => {
                Err(Error::unexpected_response(format!("Expected `LookupTopicResponse`, found {:?}", other)))
            }
        }
    }
}

pub(crate) struct PartitionedTopicMetadata {
    pub partitions: u32,
}

impl FromProto for PartitionedTopicMetadata {
    fn from_proto(proto: Message) -> Result<Self, Error> {
        match check_error(proto)?.command {
            proto::BaseCommand { partition_metadata_response: Some(response), .. } => {
                if let Some(error) = response.error {
                    return Err(Error::pulsar(error.try_into()?, response.message.unwrap_or_else(|| "<no message>".to_owned())));
                }
                match response.partitions {
                    Some(partitions) => Ok(PartitionedTopicMetadata { partitions }),
                    None => Err(Error::decoding(format!("Expected PartitionedTopicMetadata. found: {:?}", resposne)))
                }
            },
            other => {
                Err(Error::unexpected_response(format!("Expected `PartitionedTopicMetadata`, found {:?}", other)))
            }
        }
    }
}

pub(crate) struct CreateProducerSuccess {
    pub producer_name: String,
    pub last_sequence_id: Option<i64>,
    pub schema_version: Option<Vec<u8>>,
}

impl FromProto for CreateProducerSuccess {
    fn from_proto(proto: Message) -> Result<Self, Error> {
        match check_error(proto)?.command {
            proto::BaseCommand { producer_success: Some(response), .. } => {
                Ok(CreateProducerSuccess {
                    producer_name: response.producer_name,
                    last_sequence_id: response.last_sequence_id,
                    schema_version: response.schema_version,
                })
            },
            other => {
                Err(Error::unexpected_response(format!("Expected `CreateProducerSuccess`, found {:?}", other)))
            }
        }
    }
}

pub(crate) struct NamespaceTopics {
    pub topics: Vec<String>,
}

impl FromProto for NamespaceTopics {
    fn from_proto(proto: Message) -> Result<Self, Error> {
        fn from_proto(proto: Message) -> Result<Self, Error> {
            match check_error(proto)?.command {
                proto::BaseCommand { get_topics_of_namespace_response: Some(response), .. } => {
                    Ok(NamespaceTopics {
                        topics: response.topics
                    })
                },
                other => {
                    Err(Error::unexpected_response(format!("Expected `NamespaceTopics`, found {:?}", other)))
                }
            }
        }
    }
}

pub(crate) struct CommandSuccess;

impl FromProto for CommandSuccess {
    fn from_proto(proto: Message) -> Result<Self, Error> {
        match check_error(proto)?.command {
            proto::BaseCommand { success: Some(_), .. } => {
                Ok(CommandSuccess)
            },
            other => {
                Err(Error::unexpected_response(format!("Expected `NamespaceTopics`, found {:?}", other)))
            }
        }
    }
}

pub struct SendReceipt {
    pub producer_id: u64,
    pub sequence_id: u64,
    pub message_id: Option<MessageIdData>,
}

impl FromProto for SendReceipt {
    fn from_proto(proto: Message) -> Result<Self, Error> {
        match check_error(proto)?.command {
            proto::BaseCommand { send_receipt: Some(response), .. } => {
                Ok(SendReceipt {
                    producer_id: response.producer_id,
                    sequence_id: response.sequence_id,
                    message_id: response.message_id,
                })
            },
            other => {
                Err(Error::unexpected_response(format!("Expected `SendReceipt`, found {:?}", other)))
            }
        }
    }
}

fn check_error(msg: proto::Message) -> Result<proto::Message, Error> {
    if let Some(error) = msg.command.error {
        Err(error.into())
    } else {
        Ok(msg)
    }
}

fn to_key_value(props: BTreeMap<String, String>) -> Vec<proto::KeyValue> {
    props.into_iter()
        .map(|(key, value)| proto::KeyValue {
            key, value
        })
        .collect()
}

fn to_key_long_value(props: BTreeMap<String, u64>) -> Vec<proto::KeyLongValue> {
    props.into_iter()
        .map(|(key, value)| proto::KeyLongValue {
            key, value
        })
        .collect()
}

