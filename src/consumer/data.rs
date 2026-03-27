use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{
    channel::{mpsc, oneshot},
    Stream,
};

use crate::{
    connection::Connection,
    message::{proto::MessageIdData, Message as RawMessage},
    Error, Executor, Payload,
};

pub type MessageIdDataReceiver = mpsc::Receiver<Result<(MessageIdData, Payload), Error>>;

/// (message_id, payload, decoded_value, decode_error_description)
pub type DecodedMessageReceiver<T> =
    mpsc::Receiver<Result<(MessageIdData, Payload, Option<T>, Option<String>), Error>>;

/// Unified receiver that avoids spawning a pass-through task when no schema is
/// attached. When polling, `Raw` maps `(id, payload)` →
/// `(id, payload, None, None)` inline without an extra async task or channel
/// hop.
pub enum MessageReceiver<T> {
    /// No schema: poll the raw engine receiver directly.
    Raw(MessageIdDataReceiver),
    /// Schema attached: poll the decoded receiver (async decode task fills it).
    Decoded(DecodedMessageReceiver<T>),
}

impl<T> Stream for MessageReceiver<T> {
    type Item = Result<(MessageIdData, Payload, Option<T>, Option<String>), Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY: we only access the inner receivers which are Unpin (mpsc::Receiver).
        let this = self.get_mut();
        match this {
            MessageReceiver::Raw(rx) => match Pin::new(rx).poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(None),
                Poll::Ready(Some(result)) => {
                    Poll::Ready(Some(result.map(|(id, payload)| (id, payload, None, None))))
                }
            },
            MessageReceiver::Decoded(rx) => Pin::new(rx).poll_next(cx),
        }
    }
}

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
