#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Schema {
    #[prost(string, required, tag="1")]
    pub name: std::string::String,
    #[prost(bytes, required, tag="3")]
    pub schema_data: std::vec::Vec<u8>,
    #[prost(enumeration="schema::Type", required, tag="4")]
    pub r#type: i32,
    #[prost(message, repeated, tag="5")]
    pub properties: ::std::vec::Vec<KeyValue>,
}
pub mod schema {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Type {
        None = 0,
        String = 1,
        Json = 2,
        Protobuf = 3,
        Avro = 4,
        Bool = 5,
        Int8 = 6,
        Int16 = 7,
        Int32 = 8,
        Int64 = 9,
        Float = 10,
        Double = 11,
        Date = 12,
        Time = 13,
        Timestamp = 14,
        KeyValue = 15,
        Instant = 16,
        LocalDate = 17,
        LocalTime = 18,
        LocalDateTime = 19,
        ProtobufNative = 20,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
#[derive(Eq, PartialOrd, Ord, Hash)]
pub struct MessageIdData {
    #[prost(uint64, required, tag="1")]
    pub ledger_id: u64,
    #[prost(uint64, required, tag="2")]
    pub entry_id: u64,
    #[prost(int32, optional, tag="3", default="-1")]
    pub partition: ::std::option::Option<i32>,
    #[prost(int32, optional, tag="4", default="-1")]
    pub batch_index: ::std::option::Option<i32>,
    #[prost(int64, repeated, packed="false", tag="5")]
    pub ack_set: ::std::vec::Vec<i64>,
    #[prost(int32, optional, tag="6")]
    pub batch_size: ::std::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyValue {
    #[prost(string, required, tag="1")]
    pub key: std::string::String,
    #[prost(string, required, tag="2")]
    pub value: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyLongValue {
    #[prost(string, required, tag="1")]
    pub key: std::string::String,
    #[prost(uint64, required, tag="2")]
    pub value: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IntRange {
    #[prost(int32, required, tag="1")]
    pub start: i32,
    #[prost(int32, required, tag="2")]
    pub end: i32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EncryptionKeys {
    #[prost(string, required, tag="1")]
    pub key: std::string::String,
    #[prost(bytes, required, tag="2")]
    pub value: std::vec::Vec<u8>,
    #[prost(message, repeated, tag="3")]
    pub metadata: ::std::vec::Vec<KeyValue>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MessageMetadata {
    #[prost(string, required, tag="1")]
    pub producer_name: std::string::String,
    #[prost(uint64, required, tag="2")]
    pub sequence_id: u64,
    #[prost(uint64, required, tag="3")]
    pub publish_time: u64,
    #[prost(message, repeated, tag="4")]
    pub properties: ::std::vec::Vec<KeyValue>,
    /// Property set on replicated message,
    /// includes the source cluster name
    #[prost(string, optional, tag="5")]
    pub replicated_from: ::std::option::Option<std::string::String>,
    ///key to decide partition for the msg
    #[prost(string, optional, tag="6")]
    pub partition_key: ::std::option::Option<std::string::String>,
    /// Override namespace's replication
    #[prost(string, repeated, tag="7")]
    pub replicate_to: ::std::vec::Vec<std::string::String>,
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
    pub encryption_algo: ::std::option::Option<std::string::String>,
    /// Additional parameters required by encryption
    #[prost(bytes, optional, tag="15")]
    pub encryption_param: ::std::option::Option<std::vec::Vec<u8>>,
    #[prost(bytes, optional, tag="16")]
    pub schema_version: ::std::option::Option<std::vec::Vec<u8>>,
    #[prost(bool, optional, tag="17", default="false")]
    pub partition_key_b64_encoded: ::std::option::Option<bool>,
    /// Specific a key to overwrite the message key which used for ordering dispatch in Key_Shared mode.
    #[prost(bytes, optional, tag="18")]
    pub ordering_key: ::std::option::Option<std::vec::Vec<u8>>,
    /// Mark the message to be delivered at or after the specified timestamp
    #[prost(int64, optional, tag="19")]
    pub deliver_at_time: ::std::option::Option<i64>,
    /// Identify whether a message is a "marker" message used for
    /// internal metadata instead of application published data.
    /// Markers will generally not be propagated back to clients
    #[prost(int32, optional, tag="20")]
    pub marker_type: ::std::option::Option<i32>,
    /// transaction related message info
    #[prost(uint64, optional, tag="22")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="23")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    //// Add highest sequence id to support batch message with external sequence id
    #[prost(uint64, optional, tag="24", default="0")]
    pub highest_sequence_id: ::std::option::Option<u64>,
    /// Indicate if the message payload value is set
    #[prost(bool, optional, tag="25", default="false")]
    pub null_value: ::std::option::Option<bool>,
    #[prost(string, optional, tag="26")]
    pub uuid: ::std::option::Option<std::string::String>,
    #[prost(int32, optional, tag="27")]
    pub num_chunks_from_msg: ::std::option::Option<i32>,
    #[prost(int32, optional, tag="28")]
    pub total_chunk_msg_size: ::std::option::Option<i32>,
    #[prost(int32, optional, tag="29")]
    pub chunk_id: ::std::option::Option<i32>,
    /// Indicate if the message partition key is set
    #[prost(bool, optional, tag="30", default="false")]
    pub null_partition_key: ::std::option::Option<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SingleMessageMetadata {
    #[prost(message, repeated, tag="1")]
    pub properties: ::std::vec::Vec<KeyValue>,
    #[prost(string, optional, tag="2")]
    pub partition_key: ::std::option::Option<std::string::String>,
    #[prost(int32, required, tag="3")]
    pub payload_size: i32,
    #[prost(bool, optional, tag="4", default="false")]
    pub compacted_out: ::std::option::Option<bool>,
    /// the timestamp that this event occurs. it is typically set by applications.
    /// if this field is omitted, `publish_time` can be used for the purpose of `event_time`.
    #[prost(uint64, optional, tag="5", default="0")]
    pub event_time: ::std::option::Option<u64>,
    #[prost(bool, optional, tag="6", default="false")]
    pub partition_key_b64_encoded: ::std::option::Option<bool>,
    /// Specific a key to overwrite the message key which used for ordering dispatch in Key_Shared mode.
    #[prost(bytes, optional, tag="7")]
    pub ordering_key: ::std::option::Option<std::vec::Vec<u8>>,
    /// Allows consumer retrieve the sequence id that the producer set.
    #[prost(uint64, optional, tag="8")]
    pub sequence_id: ::std::option::Option<u64>,
    /// Indicate if the message payload value is set
    #[prost(bool, optional, tag="9", default="false")]
    pub null_value: ::std::option::Option<bool>,
    /// Indicate if the message partition key is set
    #[prost(bool, optional, tag="10", default="false")]
    pub null_partition_key: ::std::option::Option<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandConnect {
    #[prost(string, required, tag="1")]
    pub client_version: std::string::String,
    /// Deprecated. Use "auth_method_name" instead.
    #[prost(enumeration="AuthMethod", optional, tag="2")]
    pub auth_method: ::std::option::Option<i32>,
    #[prost(string, optional, tag="5")]
    pub auth_method_name: ::std::option::Option<std::string::String>,
    #[prost(bytes, optional, tag="3")]
    pub auth_data: ::std::option::Option<std::vec::Vec<u8>>,
    #[prost(int32, optional, tag="4", default="0")]
    pub protocol_version: ::std::option::Option<i32>,
    /// Client can ask to be proxyied to a specific broker
    /// This is only honored by a Pulsar proxy
    #[prost(string, optional, tag="6")]
    pub proxy_to_broker_url: ::std::option::Option<std::string::String>,
    /// Original principal that was verified by
    /// a Pulsar proxy. In this case the auth info above
    /// will be the auth of the proxy itself
    #[prost(string, optional, tag="7")]
    pub original_principal: ::std::option::Option<std::string::String>,
    /// Original auth role and auth Method that was passed
    /// to the proxy. In this case the auth info above
    /// will be the auth of the proxy itself
    #[prost(string, optional, tag="8")]
    pub original_auth_data: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag="9")]
    pub original_auth_method: ::std::option::Option<std::string::String>,
    /// Feature flags
    #[prost(message, optional, tag="10")]
    pub feature_flags: ::std::option::Option<FeatureFlags>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FeatureFlags {
    #[prost(bool, optional, tag="1", default="false")]
    pub supports_auth_refresh: ::std::option::Option<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandConnected {
    #[prost(string, required, tag="1")]
    pub server_version: std::string::String,
    #[prost(int32, optional, tag="2", default="0")]
    pub protocol_version: ::std::option::Option<i32>,
    #[prost(int32, optional, tag="3")]
    pub max_message_size: ::std::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandAuthResponse {
    #[prost(string, optional, tag="1")]
    pub client_version: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag="2")]
    pub response: ::std::option::Option<AuthData>,
    #[prost(int32, optional, tag="3", default="0")]
    pub protocol_version: ::std::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandAuthChallenge {
    #[prost(string, optional, tag="1")]
    pub server_version: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag="2")]
    pub challenge: ::std::option::Option<AuthData>,
    #[prost(int32, optional, tag="3", default="0")]
    pub protocol_version: ::std::option::Option<i32>,
}
/// To support mutual authentication type, such as Sasl, reuse this command to mutual auth.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AuthData {
    #[prost(string, optional, tag="1")]
    pub auth_method_name: ::std::option::Option<std::string::String>,
    #[prost(bytes, optional, tag="2")]
    pub auth_data: ::std::option::Option<std::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeySharedMeta {
    #[prost(enumeration="KeySharedMode", required, tag="1")]
    pub key_shared_mode: i32,
    #[prost(message, repeated, tag="3")]
    pub hash_ranges: ::std::vec::Vec<IntRange>,
    #[prost(bool, optional, tag="4", default="false")]
    pub allow_out_of_order_delivery: ::std::option::Option<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandSubscribe {
    #[prost(string, required, tag="1")]
    pub topic: std::string::String,
    #[prost(string, required, tag="2")]
    pub subscription: std::string::String,
    #[prost(enumeration="command_subscribe::SubType", required, tag="3")]
    pub sub_type: i32,
    #[prost(uint64, required, tag="4")]
    pub consumer_id: u64,
    #[prost(uint64, required, tag="5")]
    pub request_id: u64,
    #[prost(string, optional, tag="6")]
    pub consumer_name: ::std::option::Option<std::string::String>,
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
    /// Signal whether the subscription will initialize on latest
    /// or not -- earliest
    #[prost(enumeration="command_subscribe::InitialPosition", optional, tag="13", default="Latest")]
    pub initial_position: ::std::option::Option<i32>,
    /// Mark the subscription as "replicated". Pulsar will make sure
    /// to periodically sync the state of replicated subscriptions
    /// across different clusters (when using geo-replication).
    #[prost(bool, optional, tag="14")]
    pub replicate_subscription_state: ::std::option::Option<bool>,
    /// If true, the subscribe operation will cause a topic to be
    /// created if it does not exist already (and if topic auto-creation
    /// is allowed by broker.
    /// If false, the subscribe operation will fail if the topic
    /// does not exist.
    #[prost(bool, optional, tag="15", default="true")]
    pub force_topic_creation: ::std::option::Option<bool>,
    /// If specified, the subscription will reset cursor's position back
    /// to specified seconds and  will send messages from that point
    #[prost(uint64, optional, tag="16", default="0")]
    pub start_message_rollback_duration_sec: ::std::option::Option<u64>,
    #[prost(message, optional, tag="17")]
    pub key_shared_meta: ::std::option::Option<KeySharedMeta>,
}
pub mod command_subscribe {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum SubType {
        Exclusive = 0,
        Shared = 1,
        Failover = 2,
        KeyShared = 3,
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum InitialPosition {
        Latest = 0,
        Earliest = 1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandPartitionedTopicMetadata {
    #[prost(string, required, tag="1")]
    pub topic: std::string::String,
    #[prost(uint64, required, tag="2")]
    pub request_id: u64,
    /// TODO - Remove original_principal, original_auth_data, original_auth_method
    /// Original principal that was verified by
    /// a Pulsar proxy.
    #[prost(string, optional, tag="3")]
    pub original_principal: ::std::option::Option<std::string::String>,
    /// Original auth role and auth Method that was passed
    /// to the proxy.
    #[prost(string, optional, tag="4")]
    pub original_auth_data: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag="5")]
    pub original_auth_method: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
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
    pub message: ::std::option::Option<std::string::String>,
}
pub mod command_partitioned_topic_metadata_response {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum LookupType {
        Success = 0,
        Failed = 1,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandLookupTopic {
    #[prost(string, required, tag="1")]
    pub topic: std::string::String,
    #[prost(uint64, required, tag="2")]
    pub request_id: u64,
    #[prost(bool, optional, tag="3", default="false")]
    pub authoritative: ::std::option::Option<bool>,
    /// TODO - Remove original_principal, original_auth_data, original_auth_method
    /// Original principal that was verified by
    /// a Pulsar proxy.
    #[prost(string, optional, tag="4")]
    pub original_principal: ::std::option::Option<std::string::String>,
    /// Original auth role and auth Method that was passed
    /// to the proxy.
    #[prost(string, optional, tag="5")]
    pub original_auth_data: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag="6")]
    pub original_auth_method: ::std::option::Option<std::string::String>,
    ///
    #[prost(string, optional, tag="7")]
    pub advertised_listener_name: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandLookupTopicResponse {
    /// Optional in case of error
    #[prost(string, optional, tag="1")]
    pub broker_service_url: ::std::option::Option<std::string::String>,
    #[prost(string, optional, tag="2")]
    pub broker_service_url_tls: ::std::option::Option<std::string::String>,
    #[prost(enumeration="command_lookup_topic_response::LookupType", optional, tag="3")]
    pub response: ::std::option::Option<i32>,
    #[prost(uint64, required, tag="4")]
    pub request_id: u64,
    #[prost(bool, optional, tag="5", default="false")]
    pub authoritative: ::std::option::Option<bool>,
    #[prost(enumeration="ServerError", optional, tag="6")]
    pub error: ::std::option::Option<i32>,
    #[prost(string, optional, tag="7")]
    pub message: ::std::option::Option<std::string::String>,
    /// If it's true, indicates to the client that it must
    /// always connect through the service url after the
    /// lookup has been completed.
    #[prost(bool, optional, tag="8", default="false")]
    pub proxy_through_service_url: ::std::option::Option<bool>,
}
pub mod command_lookup_topic_response {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum LookupType {
        Redirect = 0,
        Connect = 1,
        Failed = 2,
    }
}
//// Create a new Producer on a topic, assigning the given producer_id,
//// all messages sent with this producer_id will be persisted on the topic
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandProducer {
    #[prost(string, required, tag="1")]
    pub topic: std::string::String,
    #[prost(uint64, required, tag="2")]
    pub producer_id: u64,
    #[prost(uint64, required, tag="3")]
    pub request_id: u64,
    //// If a producer name is specified, the name will be used,
    //// otherwise the broker will generate a unique name
    #[prost(string, optional, tag="4")]
    pub producer_name: ::std::option::Option<std::string::String>,
    #[prost(bool, optional, tag="5", default="false")]
    pub encrypted: ::std::option::Option<bool>,
    //// Add optional metadata key=value to this producer
    #[prost(message, repeated, tag="6")]
    pub metadata: ::std::vec::Vec<KeyValue>,
    #[prost(message, optional, tag="7")]
    pub schema: ::std::option::Option<Schema>,
    /// If producer reconnect to broker, the epoch of this producer will +1
    #[prost(uint64, optional, tag="8", default="0")]
    pub epoch: ::std::option::Option<u64>,
    /// Indicate the name of the producer is generated or user provided
    /// Use default true here is in order to be forward compatible with the client
    #[prost(bool, optional, tag="9", default="true")]
    pub user_provided_producer_name: ::std::option::Option<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandSend {
    #[prost(uint64, required, tag="1")]
    pub producer_id: u64,
    #[prost(uint64, required, tag="2")]
    pub sequence_id: u64,
    #[prost(int32, optional, tag="3", default="1")]
    pub num_messages: ::std::option::Option<i32>,
    #[prost(uint64, optional, tag="4", default="0")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="5", default="0")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    //// Add highest sequence id to support batch message with external sequence id
    #[prost(uint64, optional, tag="6", default="0")]
    pub highest_sequence_id: ::std::option::Option<u64>,
    #[prost(bool, optional, tag="7", default="false")]
    pub is_chunk: ::std::option::Option<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandSendReceipt {
    #[prost(uint64, required, tag="1")]
    pub producer_id: u64,
    #[prost(uint64, required, tag="2")]
    pub sequence_id: u64,
    #[prost(message, optional, tag="3")]
    pub message_id: ::std::option::Option<MessageIdData>,
    #[prost(uint64, optional, tag="4", default="0")]
    pub highest_sequence_id: ::std::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandSendError {
    #[prost(uint64, required, tag="1")]
    pub producer_id: u64,
    #[prost(uint64, required, tag="2")]
    pub sequence_id: u64,
    #[prost(enumeration="ServerError", required, tag="3")]
    pub error: i32,
    #[prost(string, required, tag="4")]
    pub message: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandMessage {
    #[prost(uint64, required, tag="1")]
    pub consumer_id: u64,
    #[prost(message, required, tag="2")]
    pub message_id: MessageIdData,
    #[prost(uint32, optional, tag="3", default="0")]
    pub redelivery_count: ::std::option::Option<u32>,
    #[prost(int64, repeated, packed="false", tag="4")]
    pub ack_set: ::std::vec::Vec<i64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
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
    #[prost(uint64, optional, tag="6", default="0")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="7", default="0")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="8")]
    pub request_id: ::std::option::Option<u64>,
}
pub mod command_ack {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum AckType {
        Individual = 0,
        Cumulative = 1,
    }
    /// Acks can contain a flag to indicate the consumer
    /// received an invalid message that got discarded
    /// before being passed on to the application.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum ValidationError {
        UncompressedSizeCorruption = 0,
        DecompressionError = 1,
        ChecksumMismatch = 2,
        BatchDeSerializeError = 3,
        DecryptionError = 4,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandAckResponse {
    #[prost(uint64, required, tag="1")]
    pub consumer_id: u64,
    #[prost(uint64, optional, tag="2", default="0")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="3", default="0")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    #[prost(enumeration="ServerError", optional, tag="4")]
    pub error: ::std::option::Option<i32>,
    #[prost(string, optional, tag="5")]
    pub message: ::std::option::Option<std::string::String>,
    #[prost(uint64, optional, tag="6")]
    pub request_id: ::std::option::Option<u64>,
}
/// changes on active consumer
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandActiveConsumerChange {
    #[prost(uint64, required, tag="1")]
    pub consumer_id: u64,
    #[prost(bool, optional, tag="2", default="false")]
    pub is_active: ::std::option::Option<bool>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandFlow {
    #[prost(uint64, required, tag="1")]
    pub consumer_id: u64,
    /// Max number of messages to prefetch, in addition
    /// of any number previously specified
    #[prost(uint32, required, tag="2")]
    pub message_permits: u32,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandUnsubscribe {
    #[prost(uint64, required, tag="1")]
    pub consumer_id: u64,
    #[prost(uint64, required, tag="2")]
    pub request_id: u64,
}
/// Reset an existing consumer to a particular message id
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandSeek {
    #[prost(uint64, required, tag="1")]
    pub consumer_id: u64,
    #[prost(uint64, required, tag="2")]
    pub request_id: u64,
    #[prost(message, optional, tag="3")]
    pub message_id: ::std::option::Option<MessageIdData>,
    #[prost(uint64, optional, tag="4")]
    pub message_publish_time: ::std::option::Option<u64>,
}
/// Message sent by broker to client when a topic
/// has been forcefully terminated and there are no more
/// messages left to consume
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandReachedEndOfTopic {
    #[prost(uint64, required, tag="1")]
    pub consumer_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandCloseProducer {
    #[prost(uint64, required, tag="1")]
    pub producer_id: u64,
    #[prost(uint64, required, tag="2")]
    pub request_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandCloseConsumer {
    #[prost(uint64, required, tag="1")]
    pub consumer_id: u64,
    #[prost(uint64, required, tag="2")]
    pub request_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandRedeliverUnacknowledgedMessages {
    #[prost(uint64, required, tag="1")]
    pub consumer_id: u64,
    #[prost(message, repeated, tag="2")]
    pub message_ids: ::std::vec::Vec<MessageIdData>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandSuccess {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(message, optional, tag="2")]
    pub schema: ::std::option::Option<Schema>,
}
//// Response from CommandProducer
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandProducerSuccess {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(string, required, tag="2")]
    pub producer_name: std::string::String,
    /// The last sequence id that was stored by this producer in the previous session
    /// This will only be meaningful if deduplication has been enabled.
    #[prost(int64, optional, tag="3", default="-1")]
    pub last_sequence_id: ::std::option::Option<i64>,
    #[prost(bytes, optional, tag="4")]
    pub schema_version: ::std::option::Option<std::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandError {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(enumeration="ServerError", required, tag="2")]
    pub error: i32,
    #[prost(string, required, tag="3")]
    pub message: std::string::String,
}
/// Commands to probe the state of connection.
/// When either client or broker doesn't receive commands for certain
/// amount of time, they will send a Ping probe.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandPing {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandPong {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandConsumerStats {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    /// required string topic_name         = 2;
    /// required string subscription_name  = 3;
    #[prost(uint64, required, tag="4")]
    pub consumer_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandConsumerStatsResponse {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(enumeration="ServerError", optional, tag="2")]
    pub error_code: ::std::option::Option<i32>,
    #[prost(string, optional, tag="3")]
    pub error_message: ::std::option::Option<std::string::String>,
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
    pub consumer_name: ::std::option::Option<std::string::String>,
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
    pub address: ::std::option::Option<std::string::String>,
    //// Timestamp of connection
    #[prost(string, optional, tag="12")]
    pub connected_since: ::std::option::Option<std::string::String>,
    //// Whether this subscription is Exclusive or Shared or Failover
    #[prost(string, optional, tag="13")]
    pub r#type: ::std::option::Option<std::string::String>,
    //// Total rate of messages expired on this subscription. msg/s
    #[prost(double, optional, tag="14")]
    pub msg_rate_expired: ::std::option::Option<f64>,
    //// Number of messages in the subscription backlog
    #[prost(uint64, optional, tag="15")]
    pub msg_backlog: ::std::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetLastMessageId {
    #[prost(uint64, required, tag="1")]
    pub consumer_id: u64,
    #[prost(uint64, required, tag="2")]
    pub request_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetLastMessageIdResponse {
    #[prost(message, required, tag="1")]
    pub last_message_id: MessageIdData,
    #[prost(uint64, required, tag="2")]
    pub request_id: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetTopicsOfNamespace {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(string, required, tag="2")]
    pub namespace: std::string::String,
    #[prost(enumeration="command_get_topics_of_namespace::Mode", optional, tag="3", default="Persistent")]
    pub mode: ::std::option::Option<i32>,
}
pub mod command_get_topics_of_namespace {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Mode {
        Persistent = 0,
        NonPersistent = 1,
        All = 2,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetTopicsOfNamespaceResponse {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(string, repeated, tag="2")]
    pub topics: ::std::vec::Vec<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetSchema {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(string, required, tag="2")]
    pub topic: std::string::String,
    #[prost(bytes, optional, tag="3")]
    pub schema_version: ::std::option::Option<std::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetSchemaResponse {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(enumeration="ServerError", optional, tag="2")]
    pub error_code: ::std::option::Option<i32>,
    #[prost(string, optional, tag="3")]
    pub error_message: ::std::option::Option<std::string::String>,
    #[prost(message, optional, tag="4")]
    pub schema: ::std::option::Option<Schema>,
    #[prost(bytes, optional, tag="5")]
    pub schema_version: ::std::option::Option<std::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetOrCreateSchema {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(string, required, tag="2")]
    pub topic: std::string::String,
    #[prost(message, required, tag="3")]
    pub schema: Schema,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetOrCreateSchemaResponse {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(enumeration="ServerError", optional, tag="2")]
    pub error_code: ::std::option::Option<i32>,
    #[prost(string, optional, tag="3")]
    pub error_message: ::std::option::Option<std::string::String>,
    #[prost(bytes, optional, tag="4")]
    pub schema_version: ::std::option::Option<std::vec::Vec<u8>>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandNewTxn {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(uint64, optional, tag="2", default="0")]
    pub txn_ttl_seconds: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="3", default="0")]
    pub tc_id: ::std::option::Option<u64>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandNewTxnResponse {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(uint64, optional, tag="2", default="0")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="3", default="0")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    #[prost(enumeration="ServerError", optional, tag="4")]
    pub error: ::std::option::Option<i32>,
    #[prost(string, optional, tag="5")]
    pub message: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandAddPartitionToTxn {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(uint64, optional, tag="2", default="0")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="3", default="0")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    #[prost(string, repeated, tag="4")]
    pub partitions: ::std::vec::Vec<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandAddPartitionToTxnResponse {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(uint64, optional, tag="2", default="0")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="3", default="0")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    #[prost(enumeration="ServerError", optional, tag="4")]
    pub error: ::std::option::Option<i32>,
    #[prost(string, optional, tag="5")]
    pub message: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Subscription {
    #[prost(string, required, tag="1")]
    pub topic: std::string::String,
    #[prost(string, required, tag="2")]
    pub subscription: std::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandAddSubscriptionToTxn {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(uint64, optional, tag="2", default="0")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="3", default="0")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    #[prost(message, repeated, tag="4")]
    pub subscription: ::std::vec::Vec<Subscription>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandAddSubscriptionToTxnResponse {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(uint64, optional, tag="2", default="0")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="3", default="0")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    #[prost(enumeration="ServerError", optional, tag="4")]
    pub error: ::std::option::Option<i32>,
    #[prost(string, optional, tag="5")]
    pub message: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandEndTxn {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(uint64, optional, tag="2", default="0")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="3", default="0")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    #[prost(enumeration="TxnAction", optional, tag="4")]
    pub txn_action: ::std::option::Option<i32>,
    #[prost(message, repeated, tag="5")]
    pub message_id: ::std::vec::Vec<MessageIdData>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandEndTxnResponse {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(uint64, optional, tag="2", default="0")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="3", default="0")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    #[prost(enumeration="ServerError", optional, tag="4")]
    pub error: ::std::option::Option<i32>,
    #[prost(string, optional, tag="5")]
    pub message: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandEndTxnOnPartition {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(uint64, optional, tag="2", default="0")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="3", default="0")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    #[prost(string, optional, tag="4")]
    pub topic: ::std::option::Option<std::string::String>,
    #[prost(enumeration="TxnAction", optional, tag="5")]
    pub txn_action: ::std::option::Option<i32>,
    #[prost(message, repeated, tag="6")]
    pub message_id: ::std::vec::Vec<MessageIdData>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandEndTxnOnPartitionResponse {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(uint64, optional, tag="2", default="0")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="3", default="0")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    #[prost(enumeration="ServerError", optional, tag="4")]
    pub error: ::std::option::Option<i32>,
    #[prost(string, optional, tag="5")]
    pub message: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandEndTxnOnSubscription {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(uint64, optional, tag="2", default="0")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="3", default="0")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    #[prost(message, optional, tag="4")]
    pub subscription: ::std::option::Option<Subscription>,
    #[prost(enumeration="TxnAction", optional, tag="5")]
    pub txn_action: ::std::option::Option<i32>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandEndTxnOnSubscriptionResponse {
    #[prost(uint64, required, tag="1")]
    pub request_id: u64,
    #[prost(uint64, optional, tag="2", default="0")]
    pub txnid_least_bits: ::std::option::Option<u64>,
    #[prost(uint64, optional, tag="3", default="0")]
    pub txnid_most_bits: ::std::option::Option<u64>,
    #[prost(enumeration="ServerError", optional, tag="4")]
    pub error: ::std::option::Option<i32>,
    #[prost(string, optional, tag="5")]
    pub message: ::std::option::Option<std::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BaseCommand {
    #[prost(enumeration="base_command::Type", required, tag="1")]
    pub r#type: i32,
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
    #[prost(message, optional, tag="36")]
    pub auth_challenge: ::std::option::Option<CommandAuthChallenge>,
    #[prost(message, optional, tag="37")]
    pub auth_response: ::std::option::Option<CommandAuthResponse>,
    #[prost(message, optional, tag="38")]
    pub ack_response: ::std::option::Option<CommandAckResponse>,
    #[prost(message, optional, tag="39")]
    pub get_or_create_schema: ::std::option::Option<CommandGetOrCreateSchema>,
    #[prost(message, optional, tag="40")]
    pub get_or_create_schema_response: ::std::option::Option<CommandGetOrCreateSchemaResponse>,
    /// transaction related
    #[prost(message, optional, tag="50")]
    pub new_txn: ::std::option::Option<CommandNewTxn>,
    #[prost(message, optional, tag="51")]
    pub new_txn_response: ::std::option::Option<CommandNewTxnResponse>,
    #[prost(message, optional, tag="52")]
    pub add_partition_to_txn: ::std::option::Option<CommandAddPartitionToTxn>,
    #[prost(message, optional, tag="53")]
    pub add_partition_to_txn_response: ::std::option::Option<CommandAddPartitionToTxnResponse>,
    #[prost(message, optional, tag="54")]
    pub add_subscription_to_txn: ::std::option::Option<CommandAddSubscriptionToTxn>,
    #[prost(message, optional, tag="55")]
    pub add_subscription_to_txn_response: ::std::option::Option<CommandAddSubscriptionToTxnResponse>,
    #[prost(message, optional, tag="56")]
    pub end_txn: ::std::option::Option<CommandEndTxn>,
    #[prost(message, optional, tag="57")]
    pub end_txn_response: ::std::option::Option<CommandEndTxnResponse>,
    #[prost(message, optional, tag="58")]
    pub end_txn_on_partition: ::std::option::Option<CommandEndTxnOnPartition>,
    #[prost(message, optional, tag="59")]
    pub end_txn_on_partition_response: ::std::option::Option<CommandEndTxnOnPartitionResponse>,
    #[prost(message, optional, tag="60")]
    pub end_txn_on_subscription: ::std::option::Option<CommandEndTxnOnSubscription>,
    #[prost(message, optional, tag="61")]
    pub end_txn_on_subscription_response: ::std::option::Option<CommandEndTxnOnSubscriptionResponse>,
}
pub mod base_command {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
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
        AuthChallenge = 36,
        AuthResponse = 37,
        AckResponse = 38,
        GetOrCreateSchema = 39,
        GetOrCreateSchemaResponse = 40,
        /// transaction related
        NewTxn = 50,
        NewTxnResponse = 51,
        AddPartitionToTxn = 52,
        AddPartitionToTxnResponse = 53,
        AddSubscriptionToTxn = 54,
        AddSubscriptionToTxnResponse = 55,
        EndTxn = 56,
        EndTxnResponse = 57,
        EndTxnOnPartition = 58,
        EndTxnOnPartitionResponse = 59,
        EndTxnOnSubscription = 60,
        EndTxnOnSubscriptionResponse = 61,
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CompressionType {
    None = 0,
    Lz4 = 1,
    Zlib = 2,
    Zstd = 3,
    Snappy = 4,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
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
    /// Specified schema was incompatible with topic schema
    IncompatibleSchema = 18,
    /// Dispatcher assign consumer error
    ConsumerAssignError = 19,
    /// Transaction coordinator not found error
    TransactionCoordinatorNotFound = 20,
    /// Invalid txn status error
    InvalidTxnStatus = 21,
    /// Not allowed error
    NotAllowedError = 22,
    /// Ack with transaction conflict
    TransactionConflict = 23,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum AuthMethod {
    None = 0,
    YcaV1 = 1,
    Athens = 2,
}
/// Each protocol version identify new features that are
/// incrementally added to the protocol
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
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
    /// Add CommandAuthChallenge and CommandAuthResponse for mutual auth
    V14 = 14,
    /// Added Key_Shared subscription
    ///
    /// Add CommandGetOrCreateSchema and CommandGetOrCreateSchemaResponse
    V15 = 15,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum KeySharedMode {
    AutoSplit = 0,
    Sticky = 1,
}
/// --- transaction related ---

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TxnAction {
    Commit = 0,
    Abort = 1,
}
