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
    UnackedRedelivery,
    GetConnection(oneshot::Sender<Arc<Connection<Exe>>>),
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
}
