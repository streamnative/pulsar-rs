use crate::{
    error::ConnectionError,
    message::{parse_batched_message, proto::MessageIdData, BatchedMessage, Metadata},
    Payload,
};

pub struct BatchedMessageIterator {
    messages: std::vec::IntoIter<BatchedMessage>,
    message_id: MessageIdData,
    metadata: Metadata,
    total_messages: u32,
    current_index: u32,
}

impl BatchedMessageIterator {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(message_id: MessageIdData, payload: Payload) -> Result<Self, ConnectionError> {
        let total_messages = payload
            .metadata
            .num_messages_in_batch
            .expect("expected batched message") as u32;
        let messages = parse_batched_message(total_messages, &payload.data)?;

        Ok(Self {
            messages: messages.into_iter(),
            message_id,
            total_messages,
            metadata: payload.metadata,
            current_index: 0,
        })
    }
}

impl Iterator for BatchedMessageIterator {
    type Item = (MessageIdData, Payload);

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn next(&mut self) -> Option<Self::Item> {
        let remaining = self.total_messages - self.current_index;
        if remaining == 0 {
            return None;
        }
        let index = self.current_index;
        self.current_index += 1;
        if let Some(batched_message) = self.messages.next() {
            let id = MessageIdData {
                batch_index: Some(index as i32),
                ..self.message_id.clone()
            };

            let metadata = Metadata {
                properties: batched_message.metadata.properties,
                partition_key: batched_message.metadata.partition_key,
                event_time: batched_message.metadata.event_time,
                ..self.metadata.clone()
            };

            let payload = Payload {
                metadata,
                data: batched_message.payload,
            };

            Some((id, payload))
        } else {
            None
        }
    }
}
