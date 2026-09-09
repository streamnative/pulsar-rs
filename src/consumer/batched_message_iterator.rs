use crate::{
    consumer::batch_acknowledgment::AckSet,
    error::ConnectionError,
    message::{parse_batched_message, proto::MessageIdData, BatchedMessage, Metadata},
    Payload,
};

/// Splits a batched entry into its messages, each with its own id.
pub struct BatchedMessageIterator {
    messages: std::vec::IntoIter<BatchedMessage>,
    message_id: MessageIdData,
    metadata: Metadata,
    total_messages: u32,
    current_index: u32,
    unacknowledged: AckSet,
    skipped: u32,
}

impl BatchedMessageIterator {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(
        message_id: MessageIdData,
        payload: Payload,
        broker_ack_set: &[i64],
    ) -> Result<Self, ConnectionError> {
        let total_messages = payload
            .metadata
            .num_messages_in_batch
            .expect("expected batched message") as u32;
        let messages = parse_batched_message(total_messages, &payload.data)?;
        let mut unacknowledged = AckSet::full(total_messages);
        if !broker_ack_set.is_empty() {
            unacknowledged.and(&AckSet::from(broker_ack_set));
        }

        Ok(Self {
            messages: messages.into_iter(),
            message_id,
            total_messages,
            metadata: payload.metadata,
            current_index: 0,
            unacknowledged,
            skipped: 0,
        })
    }

    /// Messages not yielded because the broker reported them as acked.
    pub fn skipped(&self) -> u32 {
        self.skipped
    }

    /// The entry's unacked messages, the broker's bitset applied.
    pub fn into_ack_set(self) -> AckSet {
        self.unacknowledged
    }
}

impl Iterator for BatchedMessageIterator {
    type Item = (MessageIdData, Payload);

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_index == self.total_messages {
                return None;
            }
            let index = self.current_index;
            self.current_index += 1;
            let batched_message = self.messages.next()?;

            if !self.unacknowledged.get(index) {
                self.skipped += 1;
                continue;
            }

            let id = MessageIdData {
                batch_index: Some(index as i32),
                batch_size: Some(self.total_messages as i32),
                ..self.message_id.clone()
            };

            let metadata = Metadata {
                properties: batched_message.metadata.properties,
                partition_key: batched_message.metadata.partition_key,
                partition_key_b64_encoded: batched_message.metadata.partition_key_b64_encoded,
                ordering_key: batched_message.metadata.ordering_key,
                event_time: batched_message.metadata.event_time,
                ..self.metadata.clone()
            };

            let payload = Payload {
                metadata,
                data: batched_message.payload,
            };

            return Some((id, payload));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::proto::SingleMessageMetadata;

    fn batched_payload_with(
        messages: &[(SingleMessageMetadata, &[u8])],
        envelope: Metadata,
    ) -> Payload {
        let mut data = Vec::new();
        for (metadata, payload) in messages {
            BatchedMessage {
                metadata: SingleMessageMetadata {
                    payload_size: payload.len() as i32,
                    ..metadata.clone()
                },
                payload: payload.to_vec(),
            }
            .serialize(&mut data);
        }
        Payload {
            metadata: Metadata {
                num_messages_in_batch: Some(messages.len() as i32),
                ..envelope
            },
            data,
        }
    }

    fn batched_payload(payloads: &[&[u8]]) -> Payload {
        let messages: Vec<_> = payloads
            .iter()
            .map(|payload| (SingleMessageMetadata::default(), *payload))
            .collect();
        batched_payload_with(&messages, Metadata::default())
    }

    fn entry() -> MessageIdData {
        MessageIdData {
            ledger_id: 1,
            entry_id: 2,
            partition: Some(-1),
            ..Default::default()
        }
    }

    #[test]
    fn yields_every_message_with_batch_index_and_size() {
        let mut iterator =
            BatchedMessageIterator::new(entry(), batched_payload(&[b"a", b"b", b"c"]), &[])
                .unwrap();
        let messages: Vec<_> = iterator.by_ref().collect();
        assert_eq!(iterator.skipped(), 0);
        assert_eq!(iterator.into_ack_set().to_words(), vec![0b111]);
        assert_eq!(
            messages
                .iter()
                .map(|(id, payload)| (id.batch_index, id.batch_size, payload.data.clone()))
                .collect::<Vec<_>>(),
            vec![
                (Some(0), Some(3), b"a".to_vec()),
                (Some(1), Some(3), b"b".to_vec()),
                (Some(2), Some(3), b"c".to_vec()),
            ]
        );
        assert!(messages.iter().all(|(id, _)| id.entry() == entry()));
    }

    #[test]
    fn skips_messages_the_broker_reports_as_acked() {
        let mut iterator =
            BatchedMessageIterator::new(entry(), batched_payload(&[b"a", b"b", b"c"]), &[0b101])
                .unwrap();
        let messages: Vec<_> = iterator.by_ref().collect();
        assert_eq!(iterator.skipped(), 1);
        assert_eq!(
            messages
                .iter()
                .map(|(id, payload)| (id.batch_index, payload.data.clone()))
                .collect::<Vec<_>>(),
            vec![(Some(0), b"a".to_vec()), (Some(2), b"c".to_vec())]
        );
        assert_eq!(iterator.into_ack_set().to_words(), vec![0b101]);
    }

    #[test]
    fn the_broker_set_is_masked_to_the_batch_size() {
        let iterator =
            BatchedMessageIterator::new(entry(), batched_payload(&[b"a", b"b"]), &[0b1111])
                .unwrap();
        assert_eq!(iterator.into_ack_set(), AckSet::full(2));
    }

    #[test]
    fn an_entry_the_broker_reports_fully_acked_yields_nothing() {
        let mut iterator =
            BatchedMessageIterator::new(entry(), batched_payload(&[b"a", b"b"]), &[0]).unwrap();
        assert_eq!(iterator.by_ref().count(), 0);
        assert_eq!(iterator.skipped(), 2);
        assert!(iterator.into_ack_set().is_empty());
    }

    #[test]
    fn per_message_b64_flag_not_taken_from_batch_envelope() {
        let messages = [
            (
                SingleMessageMetadata {
                    partition_key: Some("plain-key".into()),
                    partition_key_b64_encoded: Some(false),
                    ..Default::default()
                },
                &b"a"[..],
            ),
            (
                SingleMessageMetadata {
                    partition_key: Some("YmluYXJ5".into()),
                    partition_key_b64_encoded: Some(true),
                    ..Default::default()
                },
                &b"b"[..],
            ),
        ];
        // The envelope flag deliberately disagrees with the first message.
        let envelope = Metadata {
            partition_key_b64_encoded: Some(true),
            ..Default::default()
        };

        let expanded: Vec<_> =
            BatchedMessageIterator::new(entry(), batched_payload_with(&messages, envelope), &[])
                .unwrap()
                .collect();

        assert_eq!(expanded.len(), 2);
        assert_eq!(
            expanded[0].1.metadata.partition_key.as_deref(),
            Some("plain-key")
        );
        assert_eq!(
            expanded[0].1.metadata.partition_key_b64_encoded,
            Some(false),
            "per-message flag must win over the batch envelope"
        );
        assert_eq!(expanded[1].1.metadata.partition_key_b64_encoded, Some(true));
    }
}
