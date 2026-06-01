use std::sync::Arc;

use futures::channel::{mpsc, oneshot};

use crate::{
    connection::Connection,
    message::{proto::MessageIdData, Message as RawMessage},
    Error, Executor, Payload,
};

pub type MessageIdDataReceiver = mpsc::Receiver<Result<(MessageIdData, Payload, u32), Error>>;

pub enum EngineEvent<Exe: Executor> {
    Message(Option<RawMessage>),
    EngineMessage(Option<EngineMessage<Exe>>),
}

pub enum EngineMessage<Exe: Executor> {
    Ack(MessageIdData, bool),
    Nack(MessageIdData),
    UnackedRedelivery,
    GetConnection(oneshot::Sender<Arc<Connection<Exe>>>),
}

pub(crate) enum InternalEngineEvent<Exe: Executor> {
    Message(Option<RawMessage>),
    EngineMessage(Option<InternalEngineMessage<Exe>>),
}

pub(crate) enum InternalEngineMessage<Exe: Executor> {
    Ack(MessageIdData, bool),
    Nack(MessageIdData, Option<u32>),
    NegativeAckRedelivery,
    UnackedRedelivery,
    GetConnection(oneshot::Sender<Arc<Connection<Exe>>>),
    Close(oneshot::Sender<Result<(), crate::error::ConnectionError>>),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MessageData {
    pub id: MessageIdData,
    pub batch_size: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct DeadLetterPolicy {
    /// Maximum number of times that a message will be redelivered before being sent to the dead
    /// letter queue.
    pub max_redeliver_count: usize,
    /// Name of the dead topic where the failing messages will be sent.
    pub dead_letter_topic: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::proto;

    fn message_id() -> proto::MessageIdData {
        proto::MessageIdData {
            ledger_id: 1,
            entry_id: 1,
            partition: None,
            batch_index: None,
            ack_set: vec![],
            batch_size: None,
            first_chunk_message_id: None,
        }
    }

    #[test]
    fn test_nack_engine_message_with_count() {
        let message = InternalEngineMessage::<crate::executor::TokioExecutor>::Nack(
            message_id(),
            Some(42u32),
        );

        match message {
            InternalEngineMessage::Nack(_, count) => assert_eq!(count, Some(42u32)),
            _ => panic!("expected nack engine message"),
        }
    }

    #[test]
    fn test_nack_engine_message_id_only_carries_none() {
        let message =
            InternalEngineMessage::<crate::executor::TokioExecutor>::Nack(message_id(), None);

        match message {
            InternalEngineMessage::Nack(_, count) => assert_eq!(count, None),
            _ => panic!("expected nack engine message"),
        }
    }

    #[test]
    fn public_nack_engine_message_shape_stays_id_only() {
        let message = EngineMessage::<crate::executor::TokioExecutor>::Nack(message_id());

        match message {
            EngineMessage::Nack(_) => {}
            _ => panic!("expected public nack engine message"),
        }
    }

    #[test]
    fn public_nack_engine_message_accepts_default_message_id_with_one_field() {
        let message =
            EngineMessage::<crate::executor::TokioExecutor>::Nack(MessageIdData::default());

        match message {
            EngineMessage::Nack(_) => {}
            _ => panic!("expected public nack engine message"),
        }
    }

    #[test]
    fn internal_engine_message_supports_private_negative_ack_due_event() {
        let message =
            InternalEngineMessage::<crate::executor::TokioExecutor>::NegativeAckRedelivery;

        match message {
            InternalEngineMessage::NegativeAckRedelivery => {}
            _ => panic!("expected private negative ack redelivery event"),
        }
    }

    #[test]
    fn topic_consumer_passes_nack_config_to_consumer_engine_new() {
        let source = include_str!("topic.rs");

        assert!(!source.contains("_nack_redelivery_delay"));
        assert!(!source.contains("_negative_ack_backoff"));
        assert!(source.contains("nack_redelivery_delay,"));
        assert!(source.contains("negative_ack_backoff,"));
        assert!(source.contains("ConsumerEngine::new("));
    }

    #[test]
    fn reader_and_multi_topic_consumers_keep_topic_consumer_construction_path() {
        let client_source = include_str!("../client.rs");
        let builder_source = include_str!("builder.rs");
        let multi_source = include_str!("multi.rs");

        assert!(client_source.contains("pub fn reader(&self) -> ConsumerBuilder<Exe>"));
        assert!(client_source.contains("ConsumerBuilder::new(self)"));
        assert!(builder_source.contains("TopicConsumer::new"));
        assert!(multi_source.contains("TopicConsumer::new"));
    }
}
