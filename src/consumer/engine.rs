use std::{
    collections::{HashMap, HashSet},
    future::Future,
    io::ErrorKind,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::{
    channel::{mpsc, mpsc::UnboundedSender, oneshot},
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
    proto,
    proto::{BaseCommand, CommandCloseConsumer, CommandMessage},
    BrokerAddress, Error, Executor, Payload, Pulsar,
};

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
    remaining_messages: u32,
    unacked_message_redelivery_delay: Option<Duration>,
    unacked_messages: HashMap<MessageIdData, Instant>,
    dead_letter_policy: Option<DeadLetterPolicy>,
    options: ConsumerOptions,
    drop_signal: Option<oneshot::Sender<()>>,
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
        drop_signal: oneshot::Sender<()>,
    ) -> ConsumerEngine<Exe> {
        let (event_tx, event_rx) = mpsc::unbounded();
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
            remaining_messages: batch_size,
            unacked_message_redelivery_delay,
            unacked_messages: HashMap::new(),
            dead_letter_policy,
            options,
            drop_signal: Some(drop_signal),
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
                }
            }
            let send_end_res = event_tx.send(mapper(None)).await;
            if let Err(err) = send_end_res {
                log::error!("Error sending end event to channel - {err}");
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

            if self.remaining_messages < self.batch_size / 2 {
                match self
                    .connection
                    .sender()
                    .send_flow(self.id, self.batch_size - self.remaining_messages)
                {
                    Ok(()) => {}
                    Err(ConnectionError::Disconnected) => {
                        self.reconnect().await?;
                        self.connection
                            .sender()
                            .send_flow(self.id, self.batch_size - self.remaining_messages)?;
                    }
                    Err(e) => return Err(e.into()),
                }
                self.remaining_messages = self.batch_size;
            }

            match Self::timeout(self.event_rx.next(), Duration::from_secs(1)).await {
                Err(_timeout) => {}
                Ok(Some(EngineEvent::Message(msg))) => {
                    let out = self.handle_message_opt(msg).await;
                    if let Some(res) = out {
                        return res;
                    }
                }
                Ok(Some(EngineEvent::EngineMessage(msg))) => {
                    let continue_loop = self.handle_ack_opt(msg);
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
                error!("Consumer: messages::next: returning Disconnected");
                if let Err(err) = self.reconnect().await {
                    Some(Err(err))
                } else {
                    None
                }
                //return Err(Error::Consumer(ConsumerError::Connection(ConnectionError::Disconnected)).into());
            }
            Some(message) => {
                self.remaining_messages -= message
                    .payload
                    .as_ref()
                    .and_then(|payload| payload.metadata.num_messages_in_batch)
                    .unwrap_or(1i32) as u32;

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

    fn handle_ack_opt(&mut self, ack_opt: Option<EngineMessage<Exe>>) -> bool {
        match ack_opt {
            None => {
                trace!("ack channel was closed");
                false
            }
            Some(EngineMessage::Ack(message_id, cumulative)) => {
                self.ack(message_id, cumulative);
                true
            }
            Some(EngineMessage::Nack(message_id)) => {
                if let Err(e) = self
                    .connection
                    .sender()
                    .send_redeliver_unacknowleged_messages(self.id, vec![message_id.clone()])
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
        }
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn ack(&mut self, message_id: MessageIdData, cumulative: bool) {
        // FIXME: this does not handle cumulative acks
        self.unacked_messages.remove(&message_id);
        let res = self
            .connection
            .sender()
            .send_ack(self.id, vec![message_id], cumulative);
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
            Some(compression) => {
                proto::CompressionType::from_i32(compression).ok_or_else(|| {
                    error!("unknown compression type: {}", compression);
                    Error::Consumer(ConsumerError::Io(std::io::Error::new(
                        ErrorKind::Other,
                        format!("unknown compression type: {compression}"),
                    )))
                })?
            }
        };

        let payload = match compression {
            proto::CompressionType::None => payload,
            proto::CompressionType::Lz4 => {
                #[cfg(not(feature = "lz4"))]
                {
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "got a LZ4 compressed message but 'lz4' cargo feature is deactivated",
                    )))
                    .into());
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
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "got a zlib compressed message but 'flate2' cargo feature is deactivated",
                    )))
                    .into());
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
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "got a zstd compressed message but 'zstd' cargo feature is deactivated",
                    )))
                    .into());
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
                    return Err(Error::Consumer(ConsumerError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "got a Snappy compressed message but 'snap' cargo feature is deactivated",
                    )))
                    .into());
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

        match payload.metadata.num_messages_in_batch {
            Some(_) => {
                let it = BatchedMessageIterator::new(message.message_id, payload)?;
                for (id, payload) in it {
                    // TODO: Dead letter policy for batched messages
                    self.send_to_consumer(id, payload).await?;
                }
            }
            None => match (message.redelivery_count, self.dead_letter_policy.as_ref()) {
                (Some(redelivery_count), Some(dead_letter_policy)) => {
                    // Send message to Dead Letter Topic and ack message in original topic
                    if redelivery_count as usize >= dead_letter_policy.max_redeliver_count {
                        self.client
                            .send(&dead_letter_policy.dead_letter_topic, payload.data)
                            .await?
                            .await
                            .map_err(|e| {
                                error!("One shot cancelled {:?}", e);
                                Error::Custom("DLQ send error".to_string())
                            })?;

                        self.ack(message.message_id, false);
                    } else {
                        self.send_to_consumer(message.message_id, payload).await?
                    }
                }
                _ => self.send_to_consumer(message.message_id, payload).await?,
            },
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
        if let Some(duration) = self.unacked_message_redelivery_delay {
            self.unacked_messages.insert(message_id, now + duration);
        }
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn retry_with_delay(
        &mut self,
        current_retries: &mut u32,
        error: &str,
        topic: &String,
        text: &Option<String>,
        broker_address: &BrokerAddress,
    ) -> Result<Arc<Connection<Exe>>, Error> {
        warn!(
            "subscribing({}) answered {}, retrying request after {}ms (max_retries = {:?}): {}",
            topic,
            error,
            self.client.operation_retry_options.retry_delay.as_millis(),
            self.client.operation_retry_options.max_retries,
            text.as_deref().unwrap_or_default()
        );

        *current_retries += 1;
        self.client
            .executor
            .delay(self.client.operation_retry_options.retry_delay)
            .await;

        let addr = self.client.lookup_topic(topic).await?;
        let connection = self.client.manager.get_connection(&addr).await?;
        self.connection = connection.clone();

        warn!(
            "Retry #{} -> reconnecting consumer {:#} using connection {:#} to broker {:#} to topic {:#}",
            current_retries,
            self.id,
            self.connection.id(),
            broker_address.url,
            self.topic
        );

        Ok(connection)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn subscribe_topic(
        &mut self,
        resolver: UnboundedSender<crate::message::Message>,
        topic: &String,
        current_retries: &mut u32,
        broker_address: &BrokerAddress,
    ) -> Result<(), Result<(), Error>> {
        let start = Instant::now();
        let operation_retry_options = self.client.operation_retry_options.clone();

        match self
            .connection
            .sender()
            .subscribe(
                resolver.clone(),
                topic.clone(),
                self.subscription.clone(),
                self.sub_type,
                self.id,
                self.name.clone(),
                self.options.clone(),
            )
            .await
        {
            Ok(_success) => {
                if *current_retries > 0 {
                    let dur = (Instant::now() - start).as_secs();
                    log::info!(
                        "subscribing({}) success after {} retries over {} seconds",
                        topic,
                        *current_retries + 1,
                        dur
                    );
                }
            }
            Err(ConnectionError::PulsarError(Some(err), text)) => {
                return match err {
                    proto::ServerError::ServiceNotReady | proto::ServerError::ConsumerBusy => {
                        match operation_retry_options.max_retries {
                            Some(max_retries) if *current_retries < max_retries => {
                                self.retry_with_delay(
                                    current_retries,
                                    err.as_str_name(),
                                    topic,
                                    &text,
                                    broker_address,
                                )
                                    .await
                                    .map_err(Err)?;

                                Err(Ok(()))
                            }
                            _ => {
                                error!("subscribe topic({}) reached max retries", topic);

                                Err(Err(ConnectionError::PulsarError(Some(err), text).into()))
                            }
                        }
                    }
                    _ => Err(Err(Error::Connection(ConnectionError::PulsarError(
                        Some(err),
                        text,
                    )))),
                }
            }
            Err(ConnectionError::Io(e))
            // Retryable IO Error
            if matches!(
                    e.kind(),
                    ErrorKind::BrokenPipe
                        | ErrorKind::ConnectionAborted
                        | ErrorKind::ConnectionReset
                        | ErrorKind::Interrupted
                        | ErrorKind::NotConnected
                        | ErrorKind::TimedOut
                        | ErrorKind::UnexpectedEof
                ) =>
                {
                    return match operation_retry_options.max_retries {
                        Some(max_retries) if *current_retries < max_retries => {
                            self.retry_with_delay(
                                current_retries,
                                e.kind().to_string().as_str(),
                                topic,
                                &None,
                                broker_address,
                            )
                                .await
                                .map_err(Err)?;

                            Err(Ok(()))
                        }
                        _ => {
                            error!("subscribing({}) reached max retries", topic);
                            Err(Err(Error::Connection(ConnectionError::Io(e))))
                        }
                    }
                }
            Err(e) => {
                error!("reconnect error [{:?}]: {:?}", line!(), e);
                return Err(Err(Error::Connection(e)));
            }
        }

        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn reconnect(&mut self) -> Result<(), Error> {
        debug!("reconnecting consumer for topic: {}", self.topic);
        tokio::time::sleep(Duration::from_secs(2)).await;
        if let Some(prev_single) = std::mem::replace(&mut self.drop_signal, None) {
            // kill the previous errored consumer
            drop(prev_single);
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        let broker_address = self.client.lookup_topic(&self.topic).await?;
        let conn = self.client.manager.get_connection(&broker_address).await?;

        self.connection = conn;

        warn!(
            "Retry -> reconnecting consumer {:#} using connection {:#} to broker {:#} to topic {:#}",
            self.id,
            self.connection.id(),
            broker_address.url,
            self.topic
        );

        let topic = self.topic.clone();

        let mut current_retries = 0u32;
        let (resolver, messages) = mpsc::unbounded();

        // Reconnection loop
        // If the error is recoverable
        // Try to reconnect in the bounds of retry limits
        loop {
            match self
                .subscribe_topic(
                    resolver.clone(),
                    &topic,
                    &mut current_retries,
                    &broker_address,
                )
                .await
            {
                // Reconnection went well
                Ok(()) => break,
                // An retryable error occurs
                Err(Ok(())) => continue,
                // An non-retryable error happens, the connection must die !
                Err(Err(e)) => return Err(e),
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        self.messages_rx = Some(messages);

        // drop_signal will be dropped when Consumer is dropped, then
        // drop_receiver will return, and we can close the consumer
        let (drop_signal, drop_receiver) = oneshot::channel::<()>();
        let conn = Arc::downgrade(&self.connection);
        let name = self.name.clone();
        let id = self.id;
        let topic = self.topic.clone();
        let _ = self.client.executor.spawn(Box::pin(async move {
            let _res = drop_receiver.await;
            // if we receive a message, it indicates we want to stop this task

            error!("Disconnection !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

            match conn.upgrade() {
                None => {
                    debug!("Connection already dropped, no weak reference remaining")
                }
                Some(connection) => {
                    debug!("Closing producers of connection {}", connection.id());
                    let res = connection.sender().close_consumer(id).await;

                    if let Err(e) = res {
                        error!(
                            "could not close consumer {:?}({}) for topic {}: {:?}",
                            name, id, topic, e
                        );
                    }
                }
            }
        }));

        if let Some(prev_single) = std::mem::replace(&mut self.drop_signal, Some(drop_signal)) {
            drop(prev_single);
        }

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
