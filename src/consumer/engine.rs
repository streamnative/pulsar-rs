use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{
    channel::{mpsc, mpsc::UnboundedSender},
    SinkExt, StreamExt,
};

use crate::{
    connection::Connection,
    consumer::{
        batched_message_iterator::BatchedMessageIterator,
        data::{DeadLetterPolicy, EngineEvent, EngineMessage},
        options::ConsumerOptions,
    },
    error::{ConnectionError, ConsumerError},
    message::{
        proto::{command_subscribe::SubType, MessageIdData},
        Message as RawMessage,
    },
    producer::Message,
    proto,
    proto::{BaseCommand, CommandCloseConsumer, CommandMessage},
    retry_op::retry_subscribe_consumer,
    Error, Executor, Payload, Pulsar,
};

const SYSTEM_PROPERTY_REAL_TOPIC: &str = "REAL_TOPIC";
const SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID: &str = "ORIGIN_MESSAGE_ID";

pub struct ConsumerEngine<Exe: Executor> {
    client: Pulsar<Exe>,
    connection: Arc<Connection<Exe>>,
    topic: String,
    subscription: String,
    sub_type: SubType,
    id: u64,
    name: Option<String>,
    tx: mpsc::Sender<Result<(MessageIdData, Payload), Error>>,
    messages_rx: Option<mpsc::UnboundedReceiver<RawMessage>>,
    engine_rx: Option<mpsc::UnboundedReceiver<EngineMessage<Exe>>>,
    event_rx: mpsc::UnboundedReceiver<EngineEvent<Exe>>,
    event_tx: UnboundedSender<EngineEvent<Exe>>,
    batch_size: u32,
    remaining_messages: i64,
    unacked_message_redelivery_delay: Option<Duration>,
    unacked_messages: HashMap<MessageIdData, Instant>,
    dead_letter_policy: Option<DeadLetterPolicy>,
    options: ConsumerOptions,
    last_dequeued_message_id: Option<MessageIdData>,
    /// Whether reconnects resubscribe from the last dequeued message id.
    /// Captured once at construction: only consumers CREATED with a
    /// start-message rollback opt into this — their cursor position is
    /// client-driven, so a resubscribe at the default position would skip
    /// the undrained window. For everything else — durable subscriptions in
    /// particular — the broker-side cursor governs, and sending a resume id
    /// would move it past unacked messages. Not reset when a later seek
    /// clears the rollback from `options`: the cursor stays client-driven
    /// for the lifetime of this engine.
    track_resume_position: bool,
}

impl<Exe: Executor> ConsumerEngine<Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn new(
        client: Pulsar<Exe>,
        connection: Arc<Connection<Exe>>,
        topic: String,
        subscription: String,
        sub_type: SubType,
        id: u64,
        name: Option<String>,
        tx: mpsc::Sender<Result<(MessageIdData, Payload), Error>>,
        messages_rx: mpsc::UnboundedReceiver<RawMessage>,
        engine_rx: mpsc::UnboundedReceiver<EngineMessage<Exe>>,
        batch_size: u32,
        unacked_message_redelivery_delay: Option<Duration>,
        dead_letter_policy: Option<DeadLetterPolicy>,
        options: ConsumerOptions,
    ) -> ConsumerEngine<Exe> {
        let (event_tx, event_rx) = mpsc::unbounded();
        let track_resume_position = rollback_enabled(&options);
        ConsumerEngine {
            client,
            connection,
            topic,
            subscription,
            sub_type,
            id,
            name,
            tx,
            messages_rx: Some(messages_rx),
            engine_rx: Some(engine_rx),
            event_rx,
            event_tx,
            batch_size,
            remaining_messages: batch_size as i64,
            unacked_message_redelivery_delay,
            unacked_messages: HashMap::new(),
            dead_letter_policy,
            options,
            last_dequeued_message_id: None,
            track_resume_position,
        }
    }

    fn register_source<E, M>(&self, mut rx: mpsc::UnboundedReceiver<E>, mapper: M) -> Result<(), ()>
    where
        E: Send + 'static,
        M: Fn(Option<E>) -> EngineEvent<Exe> + Send + Sync + 'static,
    {
        let mut event_tx = self.event_tx.clone();

        self.client.executor.spawn(Box::pin(async move {
            while let Some(msg) = rx.next().await {
                let r = event_tx.send(mapper(Some(msg))).await;
                if let Err(err) = r {
                    log::error!("Error sending event to channel - {err}");
                    if err.is_disconnected() {
                        break;
                    }
                }
            }

            let send_end_res = event_tx.send(mapper(None)).await;
            if let Err(err) = send_end_res {
                log::debug!("Error sending close event to channel - {err}");
            }

            log::warn!("rx terminated");
        }))
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn engine(&mut self) -> Result<(), Error> {
        debug!("starting the consumer engine for topic {}", self.topic);

        loop {
            if !self.connection.is_valid() {
                if let Some(err) = self.connection.error() {
                    error!(
                        "Consumer: connection {} is not valid: {:?}",
                        self.connection.id(),
                        err
                    );
                    self.reconnect().await?;
                }
            }

            if let Some(messages_rx) = self.messages_rx.take() {
                self.register_source(messages_rx, |msg| EngineEvent::Message(msg))
                    .map_err(|_| {
                        Error::Custom(String::from("Error registering messages_rx source"))
                    })?;
            }

            if let Some(engine_rx) = self.engine_rx.take() {
                self.register_source(engine_rx, |msg| EngineEvent::EngineMessage(msg))
                    .map_err(|_| {
                        Error::Custom(String::from("Error registering engine_rx source"))
                    })?;
            }

            // In the casual workflow, we use the `batch_size` as a maximum number that we could
            // send using a send flow command message to ask the broker to send us
            // messages, which is why we have a subtraction below (`batch_size` -
            // `remaining_messages`).
            //
            // In the special case of batch messages (which is defined by clients), the number of
            // messages could be greater than the given batch size and the remaining
            // messages goes negative, which is why we use an `i64` as we want to keep
            // track of the number of negative messages as the next send flow will be
            // the batch_size - minus remaining messages which allow us to retrieve the casual
            // workflow of flow command messages.
            //
            // Here is the example of it works for a batch_size at 1000 and the error case:
            //
            // ```
            // Message (1) -> (batch_size = 1000, remaing_messages = 999, no flow message trigger)
            // ... 499 messages later ...
            // Message (1) -> (batch_size = 1000, remaing_messages = 499, flow message trigger, ask broker to send => batch_size - remaining_messages = 501)
            // ... 200 messages later ...
            // BatchMessage (1024) -> (batch_size = 1000, remaining_messages = 4294967096, no flow message trigger) [underflow on remaining messages - without the patch]
            // BatchMessage (1024) -> (batch_size = 1000, remaining_messages = -1124, flow message trigger, ask broker to send =>  batch_size - remaining_messages = 2124) [no underflow on remaining messages - with the patch]
            // ```
            if self.remaining_messages < (self.batch_size.div_ceil(2) as i64) {
                match self
                    .connection
                    .sender()
                    .send_flow(
                        self.id,
                        (self.batch_size as i64 - self.remaining_messages) as u32,
                    )
                    .await
                {
                    Ok(()) => {}
                    Err(ConnectionError::Disconnected) => {
                        debug!("consumer engine: consumer connection disconnected, trying to reconnect");
                        continue;
                    }
                    // we don't need to handle the SlowDown error, since send_flow waits on the
                    // channel to be not full
                    Err(e) => {
                        error!("consumer engine: we got a unrecoverable connection error, {e}");
                        return Err(e.into());
                    }
                }

                self.remaining_messages = self.batch_size as i64;
            }

            match Self::timeout(self.event_rx.next(), Duration::from_secs(1)).await {
                Err(_) => {
                    // If you are reading this comment, you may have an issue where you have
                    // received a batched message that is greater that the batch
                    // size and then break the way that we send flow command message.
                    //
                    // In that case, you could increase your batch size or patch this driver by
                    // adding the following line, if you are sure that you have
                    // at least 1 incoming message per second.
                    //
                    // ```rust
                    // self.remaining_messages = 0;
                    // ```
                    debug!("consumer engine: timeout (1s)");
                }
                Ok(Some(EngineEvent::Message(msg))) => {
                    debug!("consumer engine: received message, {:?}", msg);
                    let out = self.handle_message_opt(msg).await;
                    if let Some(res) = out {
                        return res;
                    }
                }
                Ok(Some(EngineEvent::EngineMessage(msg))) => {
                    debug!("consumer engine: received engine message");
                    let continue_loop = self.handle_ack_opt(msg).await;
                    if !continue_loop {
                        return Ok(());
                    }
                }
                Ok(None) => {
                    log::warn!("Event stream is terminated");
                    return Ok(());
                }
            }
        }
    }

    async fn handle_message_opt(
        &mut self,
        message_opt: Option<crate::message::Message>,
    ) -> Option<Result<(), Error>> {
        match message_opt {
            None => {
                debug!("Consumer: old message source terminated");
                None
            }
            Some(message) => {
                self.remaining_messages -= message
                    .payload
                    .as_ref()
                    .and_then(|payload| {
                        debug!(
                            "Consumer: received message payload, num_messages_in_batch = {:?}",
                            payload.metadata.num_messages_in_batch
                        );
                        payload.metadata.num_messages_in_batch
                    })
                    .unwrap_or(1) as i64;

                match self.process_message(message).await {
                    // Continue
                    Ok(true) => None,
                    // End of Topic
                    Ok(false) => Some(Ok(())),
                    Err(e) => {
                        if let Err(e) = self.tx.send(Err(e)).await {
                            error!("cannot send a message from the consumer engine to the consumer({}), stopping the engine", self.id);
                            Some(Err(Error::Consumer(e.into())))
                        } else {
                            None
                        }
                    }
                }
            }
        }
    }

    async fn handle_ack_opt(&mut self, ack_opt: Option<EngineMessage<Exe>>) -> bool {
        match ack_opt {
            None => {
                trace!("ack channel was closed");
                false
            }
            Some(EngineMessage::Ack(message_id, cumulative)) => {
                self.ack(message_id, cumulative).await;
                true
            }
            Some(EngineMessage::Nack(message_id)) => {
                if let Err(e) = self
                    .connection
                    .sender()
                    .send_redeliver_unacknowleged_messages(self.id, vec![message_id.clone()])
                    .await
                {
                    error!(
                        "could not ask for redelivery for message {:?}: {:?}",
                        message_id, e
                    );
                }
                true
            }
            Some(EngineMessage::UnackedRedelivery) => {
                let mut h = HashSet::new();
                let now = Instant::now();
                // info!("unacked messages length: {}", self.unacked_messages.len());
                for (id, t) in self.unacked_messages.iter() {
                    if *t < now {
                        h.insert(id.clone());
                    }
                }

                let ids: Vec<_> = h.iter().cloned().collect();
                if !ids.is_empty() {
                    // info!("will unack ids: {:?}", ids);
                    if let Err(e) = self
                        .connection
                        .sender()
                        .send_redeliver_unacknowleged_messages(self.id, ids)
                        .await
                    {
                        error!("could not ask for redelivery: {:?}", e);
                    } else {
                        for i in h.iter() {
                            self.unacked_messages.remove(i);
                        }
                    }
                }
                true
            }
            Some(EngineMessage::GetConnection(sender)) => {
                let _ = sender.send(self.connection.clone()).map_err(|_| {
                    error!(
                        "consumer requested the engine's connection but dropped the \
                                     channel before receiving"
                    );
                });
                true
            }
            Some(EngineMessage::SeekPosition(message_id)) => {
                // Everything recorded before the seek is stale: rebase the
                // subscribe options on the seek target so a reconnect resumes
                // there, and drop the rollback so it cannot rewind past it.
                self.options.start_message_id = message_id;
                self.options.start_message_rollback_duration_secs = None;
                self.last_dequeued_message_id = None;
                true
            }
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn ack(&mut self, message_id: MessageIdData, cumulative: bool) {
        // FIXME: this does not handle cumulative acks
        self.unacked_messages.remove(&message_id);
        let res = self
            .connection
            .sender()
            .send_ack(self.id, vec![message_id], cumulative)
            .await;
        if res.is_err() {
            error!("ack error: {:?}", res);
        }
    }

    /// Process the message. Returns `true` if there are more messages to process
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn process_message(&mut self, message: RawMessage) -> Result<bool, Error> {
        match message {
            RawMessage {
                command:
                    BaseCommand {
                        reached_end_of_topic: Some(_),
                        ..
                    },
                ..
            } => {
                return Ok(false);
            }
            RawMessage {
                command:
                    BaseCommand {
                        active_consumer_change: Some(active_consumer_change),
                        ..
                    },
                ..
            } => {
                // TODO: Communicate this status to the Consumer and expose it
                debug!(
                    "Active consumer change for {} - Active: {:?}",
                    self.debug_format(),
                    active_consumer_change.is_active
                );
            }
            RawMessage {
                command:
                    BaseCommand {
                        message: Some(message),
                        ..
                    },
                payload: Some(payload),
            } => {
                self.process_payload(message, payload).await?;
            }
            RawMessage {
                command: BaseCommand {
                    message: Some(_), ..
                },
                payload: None,
            } => {
                error!(
                    "Consumer {} received message without payload",
                    self.debug_format()
                );
            }
            RawMessage {
                command:
                    BaseCommand {
                        close_consumer: Some(CommandCloseConsumer { consumer_id, .. }),
                        ..
                    },
                ..
            } => {
                error!(
                    "Broker notification of closed consumer {}: {}",
                    consumer_id,
                    self.debug_format()
                );

                self.reconnect().await?;
            }
            unexpected => {
                let r#type = proto::base_command::Type::try_from(unexpected.command.r#type)
                    .map(|t| format!("{t:?}"))
                    .unwrap_or_else(|_| unexpected.command.r#type.to_string());
                warn!(
                    "Unexpected message type sent to consumer: {}. This is probably a bug!",
                    r#type
                );
            }
        }
        Ok(true)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn process_payload(
        &mut self,
        message: CommandMessage,
        mut payload: Payload,
    ) -> Result<(), Error> {
        let compression = match payload.metadata.compression {
            None => proto::CompressionType::None,
            Some(compression) => proto::CompressionType::try_from(compression).map_err(|err| {
                error!("unknown compression type: {}", compression);
                Error::Consumer(ConsumerError::Io(std::io::Error::other(format!(
                    "unknown compression type {compression}: {err}"
                ))))
            })?,
        };

        let payload = match compression {
            proto::CompressionType::None => payload,
            proto::CompressionType::Lz4 => {
                #[cfg(not(feature = "lz4"))]
                {
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::other(
                        "got a LZ4 compressed message but 'lz4' cargo feature is deactivated",
                    ))));
                }

                #[cfg(feature = "lz4")]
                {
                    let decompressed_payload = lz4::block::decompress(
                        &payload.data[..],
                        payload.metadata.uncompressed_size.map(|i| i as i32),
                    )
                    .map_err(ConsumerError::Io)?;

                    payload.data = decompressed_payload;
                    payload
                }
            }
            proto::CompressionType::Zlib => {
                #[cfg(not(feature = "flate2"))]
                {
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::other(
                        "got a zlib compressed message but 'flate2' cargo feature is deactivated",
                    ))));
                }

                #[cfg(feature = "flate2")]
                {
                    use std::io::Read;

                    use flate2::read::ZlibDecoder;

                    let mut d = ZlibDecoder::new(&payload.data[..]);
                    let mut decompressed_payload = Vec::new();
                    d.read_to_end(&mut decompressed_payload)
                        .map_err(ConsumerError::Io)?;

                    payload.data = decompressed_payload;
                    payload
                }
            }
            proto::CompressionType::Zstd => {
                #[cfg(not(feature = "zstd"))]
                {
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::other(
                        "got a zstd compressed message but 'zstd' cargo feature is deactivated",
                    ))));
                }

                #[cfg(feature = "zstd")]
                {
                    let decompressed_payload =
                        zstd::decode_all(&payload.data[..]).map_err(ConsumerError::Io)?;

                    payload.data = decompressed_payload;
                    payload
                }
            }
            proto::CompressionType::Snappy => {
                #[cfg(not(feature = "snap"))]
                {
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::other(
                        "got a Snappy compressed message but 'snap' cargo feature is deactivated",
                    ))));
                }

                #[cfg(feature = "snap")]
                {
                    use std::io::Read;

                    let mut decompressed_payload = Vec::new();
                    let mut decoder = snap::read::FrameDecoder::new(&payload.data[..]);
                    decoder
                        .read_to_end(&mut decompressed_payload)
                        .map_err(ConsumerError::Io)?;

                    payload.data = decompressed_payload;
                    payload
                }
            }
        };

        let payloads = if payload.metadata.num_messages_in_batch.is_some() {
            BatchedMessageIterator::new(message.message_id, payload)?.collect()
        } else {
            vec![(message.message_id, payload)]
        };
        for (message_id, payload) in payloads {
            match (message.redelivery_count, &self.dead_letter_policy) {
                (Some(redelivery_count), Some(dead_letter_policy))
                    if redelivery_count as usize >= dead_letter_policy.max_redeliver_count =>
                {
                    // Send message to Dead Letter Topic and ack message in original topic
                    let Payload { data, metadata } = payload;
                    let mut properties = metadata
                        .properties
                        .into_iter()
                        .map(|p| (p.key, p.value))
                        .collect::<HashMap<_, _>>();
                    properties
                        .entry(SYSTEM_PROPERTY_REAL_TOPIC.to_string())
                        .or_insert_with(|| self.topic.clone());
                    properties
                        .entry(SYSTEM_PROPERTY_ORIGIN_MESSAGE_ID.to_string())
                        .or_insert_with(|| {
                            if let Some(batch_index) = message_id.batch_index {
                                format!(
                                    "{}:{}:{}:{}",
                                    message_id.ledger_id,
                                    message_id.entry_id,
                                    message_id.partition.unwrap_or(-1),
                                    batch_index
                                )
                            } else {
                                format!(
                                    "{}:{}:{}",
                                    message_id.ledger_id,
                                    message_id.entry_id,
                                    message_id.partition.unwrap_or(-1)
                                )
                            }
                        });
                    let message = Message {
                        payload: data,
                        properties,
                        partition_key: metadata.partition_key,
                        ordering_key: metadata.ordering_key,
                        event_time: metadata.event_time,
                        ..Default::default()
                    };
                    self.client
                        .send(&dead_letter_policy.dead_letter_topic, message)
                        .await?
                        .await
                        .map_err(|e| {
                            error!("One shot cancelled {:?}", e);
                            Error::Custom("DLQ send error".to_string())
                        })?;

                    self.ack(message_id, false).await;
                }
                _ => self.send_to_consumer(message_id, payload).await?,
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send_to_consumer(
        &mut self,
        message_id: MessageIdData,
        payload: Payload,
    ) -> Result<(), Error> {
        let now = Instant::now();
        self.tx
            .send(Ok((message_id.clone(), payload)))
            .await
            .map_err(|e| {
                error!("tx returned {:?}", e);
                Error::Custom("tx closed".to_string())
            })?;
        self.last_dequeued_message_id = Some(message_id.clone());
        if let Some(duration) = self.unacked_message_redelivery_delay {
            self.unacked_messages.insert(message_id, now + duration);
        }
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn reconnect(&mut self) -> Result<(), Error> {
        debug!("reconnecting consumer for topic: {}", self.topic);

        // send CloseConsumer to server
        // remove our resolver from the Connection consumers BTreeMap,
        // stopping the current Message source
        if let Err(e) = self.connection.sender().close_consumer(self.id).await {
            error!(
                "could not close consumer {:?}({}) for topic {}: {:?}",
                self.name, self.id, self.topic, e
            );
        }
        // should have logged "rx terminated" by now

        let broker_address = self.client.lookup_topic(&self.topic).await?;

        // Re-applying the start-message rollback on every resubscribe would
        // reset the cursor back by the window again after each broker/network
        // blip, replaying messages this consumer already processed. Mirror the
        // Java client (ConsumerImpl.clearReceiverQueue): once the consumer has
        // made progress, resubscribe from the last dequeued message id instead
        // of rolling back — clearing the rollback alone would resubscribe at
        // the default position (latest) and skip whatever was left of the
        // window. Gated to rollback consumers: see track_resume_position.
        let options = resubscribe_options(
            &self.options,
            self.track_resume_position,
            self.last_dequeued_message_id.as_ref(),
        );

        let messages = retry_subscribe_consumer(
            &self.client,
            &mut self.connection,
            broker_address,
            &self.topic,
            &self.subscription,
            self.sub_type,
            self.id,
            &self.name,
            &options,
            self.batch_size,
        )
        .await?;

        self.remaining_messages = self.batch_size as i64;
        self.messages_rx = Some(messages);

        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn debug_format(&self) -> String {
        format!(
            "[{id} - {subscription}{name}: {topic}]",
            id = self.id,
            subscription = &self.subscription,
            name = self
                .name
                .as_ref()
                .map(|s| format!("({s})"))
                .unwrap_or_default(),
            topic = &self.topic
        )
    }

    #[cfg(feature = "async-std")]
    async fn timeout<F: Future<Output = O>, O>(
        fut: F,
        dur: Duration,
    ) -> Result<O, async_std::future::TimeoutError> {
        use async_std::prelude::FutureExt;
        fut.timeout(dur).await
    }

    #[cfg(all(not(feature = "async-std"), feature = "tokio"))]
    async fn timeout<F: Future<Output = O>, O>(
        fut: F,
        dur: Duration,
    ) -> Result<O, tokio::time::error::Elapsed> {
        tokio::time::timeout_at(tokio::time::Instant::now() + dur, fut).await
    }
}

impl<Exe: Executor> std::ops::Drop for ConsumerEngine<Exe> {
    fn drop(&mut self) {
        let conn = self.connection.clone();
        let id = self.id;
        let name = self.name.clone();
        let topic = self.topic.clone();
        let _ = self.client.executor.spawn(Box::pin(async move {
            if let Err(e) = conn.sender().close_consumer(id).await {
                error!(
                    "could not close consumer {:?}({}) for topic {}: {:?}",
                    name, id, topic, e
                );
            }
        }));
    }
}

/// Options to use when resubscribing after a connection loss.
///
/// Mirrors the Java client (`ConsumerImpl.clearReceiverQueue`): while the
/// consumer has made no progress the original options — including the
/// start-message rollback — are re-sent unchanged; once a message has been
/// dequeued, the resubscribe starts from that message id (the broker resumes
/// delivery after it) and the rollback is dropped. Sending the rollback again
/// would rewind the cursor by the whole window on every reconnect, while
/// dropping it without a resume position would jump to the default position
/// (latest) and skip the undrained remainder of the window.
///
/// For batched messages the resume id carries the batch index of the last
/// dequeued message; the broker redelivers the containing entry, so a few
/// messages of that batch may be seen again (at-least-once, same as the
/// Java client's coarser entry-level resume).
///
/// `track_resume_position` gates the whole rewrite to consumers created
/// with the rollback option, whose cursor position is client-driven. For
/// every other consumer — durable subscriptions in particular — the options
/// are returned unchanged: there the broker-side cursor governs, and
/// sending a resume id would move it past in-flight unacked messages,
/// losing them instead of redelivering.
/// A zero rollback means "disabled", matching the wire encoding (the
/// `CommandSubscribe` builder filters zero out as "no rollback") — it must
/// not opt the consumer into client-side resume tracking either.
fn rollback_enabled(options: &ConsumerOptions) -> bool {
    options
        .start_message_rollback_duration_secs
        .is_some_and(|secs| secs > 0)
}

#[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
fn resubscribe_options(
    options: &ConsumerOptions,
    track_resume_position: bool,
    last_dequeued_message_id: Option<&MessageIdData>,
) -> ConsumerOptions {
    let mut options = options.clone();
    if !track_resume_position {
        return options;
    }
    if let Some(resume_from) = last_dequeued_message_id {
        options.start_message_id = Some(resume_from.clone());
        options.start_message_rollback_duration_secs = None;
    }
    options
}

#[cfg(test)]
mod tests {
    use super::{resubscribe_options, rollback_enabled};
    use crate::{consumer::ConsumerOptions, message::proto::MessageIdData};

    #[test]
    fn zero_rollback_is_disabled() {
        // Matches the wire encoding: CommandSubscribe omits a zero rollback,
        // so it must not opt the consumer into resume tracking either.
        assert!(!rollback_enabled(&ConsumerOptions::default()));
        assert!(!rollback_enabled(
            &ConsumerOptions::default().with_start_message_rollback_duration_secs(0)
        ));
        assert!(rollback_enabled(
            &ConsumerOptions::default().with_start_message_rollback_duration_secs(600)
        ));
    }

    fn message_id(ledger_id: u64, entry_id: u64) -> MessageIdData {
        MessageIdData {
            ledger_id,
            entry_id,
            ..Default::default()
        }
    }

    #[test]
    fn resubscribe_without_progress_keeps_rollback_and_start_position() {
        let options = ConsumerOptions::default()
            .with_start_message_rollback_duration_secs(600)
            .starting_on_message(message_id(1, 1));
        let resubscribe = resubscribe_options(&options, true, None);
        assert_eq!(resubscribe.start_message_rollback_duration_secs, Some(600));
        assert_eq!(resubscribe.start_message_id, Some(message_id(1, 1)));
    }

    #[test]
    fn resubscribe_after_progress_resumes_from_last_dequeued() {
        let options = ConsumerOptions::default()
            .with_start_message_rollback_duration_secs(600)
            .starting_on_message(message_id(1, 1));
        let last = message_id(7, 42);
        let resubscribe = resubscribe_options(&options, true, Some(&last));
        // the rollback must not rewind the cursor again...
        assert_eq!(resubscribe.start_message_rollback_duration_secs, None);
        // ...and the resume position replaces the original start id, so the
        // broker continues after the last delivered message instead of
        // jumping to the default position (latest).
        assert_eq!(resubscribe.start_message_id, Some(last));
        // everything else must be preserved
        assert_eq!(resubscribe.durable, options.durable);
    }

    #[test]
    fn resubscribe_without_rollback_never_rewrites_the_start_position() {
        // A consumer NOT created with the rollback option must reconnect
        // with its original options even after progress: for a durable
        // subscription the broker cursor governs the resume position, and
        // sending the last dequeued id would move it past in-flight
        // unacked messages, losing them instead of redelivering.
        let options = ConsumerOptions::default().starting_on_message(message_id(1, 1));
        let last = message_id(7, 42);
        let resubscribe = resubscribe_options(&options, false, Some(&last));
        assert_eq!(resubscribe.start_message_id, Some(message_id(1, 1)));
        assert_eq!(resubscribe.start_message_rollback_duration_secs, None);
    }
}
