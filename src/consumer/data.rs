use crate::connection::Connection;
use crate::message::proto::MessageIdData;
use crate::message::Message as RawMessage;
use crate::{Error, Executor, Payload};
use futures::channel::{mpsc, oneshot};
use std::sync::Arc;

pub type MessageIdDataReceiver = mpsc::Receiver<Result<(MessageIdData, Payload), Error>>;

pub enum EngineEvent<Exe: Executor> {
    Message(Option<RawMessage>),
    EngineMessage(Option<EngineMessage<Exe>>),
}

pub enum EngineMessage<Exe: Executor> {
    Ack(MessageData, bool),
    Nack(MessageData),
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
