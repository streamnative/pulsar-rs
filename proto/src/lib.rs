mod pulsar;
pub use pulsar::*;

use std::convert::TryFrom;

impl TryFrom<i32> for proto::base_command::Type {
    type Error = ();

    fn try_from(value: i32) -> Result<Self, ()> {
        match value {
            2 => Ok(proto::base_command::Type::Connect),
            3 => Ok(proto::base_command::Type::Connected),
            4 => Ok(proto::base_command::Type::Subscribe),
            5 => Ok(proto::base_command::Type::Producer),
            6 => Ok(proto::base_command::Type::Send),
            7 => Ok(proto::base_command::Type::SendReceipt),
            8 => Ok(proto::base_command::Type::SendError),
            9 => Ok(proto::base_command::Type::Message),
            10 => Ok(proto::base_command::Type::Ack),
            11 => Ok(proto::base_command::Type::Flow),
            12 => Ok(proto::base_command::Type::Unsubscribe),
            13 => Ok(proto::base_command::Type::Success),
            14 => Ok(proto::base_command::Type::Error),
            15 => Ok(proto::base_command::Type::CloseProducer),
            16 => Ok(proto::base_command::Type::CloseConsumer),
            17 => Ok(proto::base_command::Type::ProducerSuccess),
            18 => Ok(proto::base_command::Type::Ping),
            19 => Ok(proto::base_command::Type::Pong),
            20 => Ok(proto::base_command::Type::RedeliverUnacknowledgedMessages),
            21 => Ok(proto::base_command::Type::PartitionedMetadata),
            22 => Ok(proto::base_command::Type::PartitionedMetadataResponse),
            23 => Ok(proto::base_command::Type::Lookup),
            24 => Ok(proto::base_command::Type::LookupResponse),
            25 => Ok(proto::base_command::Type::ConsumerStats),
            26 => Ok(proto::base_command::Type::ConsumerStatsResponse),
            27 => Ok(proto::base_command::Type::ReachedEndOfTopic),
            28 => Ok(proto::base_command::Type::Seek),
            29 => Ok(proto::base_command::Type::GetLastMessageId),
            30 => Ok(proto::base_command::Type::GetLastMessageIdResponse),
            31 => Ok(proto::base_command::Type::ActiveConsumerChange),
            32 => Ok(proto::base_command::Type::GetTopicsOfNamespace),
            33 => Ok(proto::base_command::Type::GetTopicsOfNamespaceResponse),
            34 => Ok(proto::base_command::Type::GetSchema),
            35 => Ok(proto::base_command::Type::GetSchemaResponse),
            _ => Err(()),
        }
    }
}
