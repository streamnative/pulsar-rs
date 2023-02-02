use std::sync::Arc;

use futures::channel::{mpsc, oneshot};

use crate::{
    connection::Connection,
    message::{proto::MessageIdData, Message as RawMessage},
    Error, Executor, Payload,
};

pub type MessageIdDataReceiver = mpsc::Receiver<Result<(MessageIdData, Payload), Error>>;

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
