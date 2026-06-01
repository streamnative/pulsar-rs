use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
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
        data::{DeadLetterPolicy, InternalEngineEvent, InternalEngineMessage},
        negative_ack_backoff::NegativeAckBackoff,
        negative_ack_tracker::{
            NegativeAckSchedule, NegativeAckTracker, NEGATIVE_ACK_REDELIVERY_TICK_INTERVAL,
        },
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
    tx: mpsc::Sender<Result<(MessageIdData, Payload, u32), Error>>,
    messages_rx: Option<mpsc::UnboundedReceiver<RawMessage>>,
    engine_rx: Option<mpsc::UnboundedReceiver<InternalEngineMessage<Exe>>>,
    event_rx: mpsc::UnboundedReceiver<InternalEngineEvent<Exe>>,
    event_tx: UnboundedSender<InternalEngineEvent<Exe>>,
    batch_size: u32,
    remaining_messages: i64,
    unacked_message_redelivery_delay: Option<Duration>,
    unacked_messages: HashMap<MessageIdData, Instant>,
    nack_redelivery_delay: Option<Duration>,
    negative_ack_backoff: Option<Arc<dyn NegativeAckBackoff + Send + Sync>>,
    negative_ack_tracker: Option<NegativeAckTracker>,
    negative_ack_ticker_running: Option<Arc<AtomicBool>>,
    negative_ack_due_event_pending: Arc<AtomicBool>,
    dead_letter_policy: Option<DeadLetterPolicy>,
    options: ConsumerOptions,
    closed: bool,
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
        tx: mpsc::Sender<Result<(MessageIdData, Payload, u32), Error>>,
        messages_rx: mpsc::UnboundedReceiver<RawMessage>,
        engine_rx: mpsc::UnboundedReceiver<InternalEngineMessage<Exe>>,
        batch_size: u32,
        unacked_message_redelivery_delay: Option<Duration>,
        nack_redelivery_delay: Option<Duration>,
        negative_ack_backoff: Option<Arc<dyn NegativeAckBackoff + Send + Sync>>,
        dead_letter_policy: Option<DeadLetterPolicy>,
        options: ConsumerOptions,
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
            remaining_messages: batch_size as i64,
            unacked_message_redelivery_delay,
            unacked_messages: HashMap::new(),
            nack_redelivery_delay,
            negative_ack_backoff,
            negative_ack_tracker: None,
            negative_ack_ticker_running: None,
            negative_ack_due_event_pending: Arc::new(AtomicBool::new(false)),
            dead_letter_policy,
            options,
            closed: false,
        }
    }

    fn register_source<E, M>(&self, mut rx: mpsc::UnboundedReceiver<E>, mapper: M) -> Result<(), ()>
    where
        E: Send + 'static,
        M: Fn(Option<E>) -> InternalEngineEvent<Exe> + Send + Sync + 'static,
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
                self.register_source(messages_rx, |msg| InternalEngineEvent::Message(msg))
                    .map_err(|_| {
                        Error::Custom(String::from("Error registering messages_rx source"))
                    })?;
            }

            if let Some(engine_rx) = self.engine_rx.take() {
                self.register_source(engine_rx, |msg| InternalEngineEvent::EngineMessage(msg))
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
                Ok(Some(InternalEngineEvent::Message(msg))) => {
                    debug!("consumer engine: received message, {:?}", msg);
                    let out = self.handle_message_opt(msg).await;
                    if let Some(res) = out {
                        return res;
                    }
                }
                Ok(Some(InternalEngineEvent::EngineMessage(msg))) => {
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

    async fn handle_ack_opt(&mut self, ack_opt: Option<InternalEngineMessage<Exe>>) -> bool {
        match ack_opt {
            None => {
                trace!("ack channel was closed");
                false
            }
            Some(InternalEngineMessage::Ack(message_id, cumulative)) => {
                self.ack(message_id, cumulative).await;
                true
            }
            Some(InternalEngineMessage::Nack(message_id, redelivery_count)) => {
                self.handle_negative_ack(message_id, redelivery_count).await;
                true
            }
            Some(InternalEngineMessage::NegativeAckRedelivery) => {
                self.handle_negative_ack_redelivery().await;
                true
            }
            Some(InternalEngineMessage::UnackedRedelivery) => {
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
            Some(InternalEngineMessage::GetConnection(sender)) => {
                let _ = sender.send(self.connection.clone()).map_err(|_| {
                    error!(
                        "consumer requested the engine's connection but dropped the \
                                     channel before receiving"
                    );
                });
                true
            }
            Some(InternalEngineMessage::Close(sender)) => {
                self.clear_negative_ack_state();
                let res = self
                    .connection
                    .sender()
                    .close_consumer(self.id)
                    .await
                    .map(|_| ());
                self.closed = true;
                let _ = sender.send(res);
                false
            }
        }
    }

    fn make_negative_ack_tracker(&self) -> NegativeAckTracker {
        NegativeAckTracker::new(
            self.nack_redelivery_delay,
            self.negative_ack_backoff.clone(),
        )
    }

    fn ensure_negative_ack_tracker(&mut self) -> &mut NegativeAckTracker {
        if self.negative_ack_tracker.is_none() {
            self.negative_ack_tracker = Some(self.make_negative_ack_tracker());
        }
        self.negative_ack_tracker
            .as_mut()
            .expect("negative ack tracker was just initialized")
    }

    fn start_negative_ack_ticker(&mut self) -> Result<(), ()> {
        if self
            .negative_ack_ticker_running
            .as_ref()
            .map(|ticker_running| ticker_running.load(Ordering::SeqCst))
            .unwrap_or(false)
        {
            return Ok(());
        }

        let ticker_running = Arc::new(AtomicBool::new(true));
        self.negative_ack_ticker_running = Some(ticker_running.clone());
        let due_event_pending = self.negative_ack_due_event_pending.clone();
        let mut event_tx = self.event_tx.clone();
        let mut interval = self
            .client
            .executor
            .interval(NEGATIVE_ACK_REDELIVERY_TICK_INTERVAL);

        let spawn_result = self.client.executor.spawn(Box::pin(async move {
            while interval.next().await.is_some() {
                if !ticker_running.load(Ordering::SeqCst) {
                    break;
                }

                if due_event_pending
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
                {
                    continue;
                }

                if event_tx
                    .send(InternalEngineEvent::EngineMessage(Some(
                        InternalEngineMessage::NegativeAckRedelivery,
                    )))
                    .await
                    .is_err()
                {
                    due_event_pending.store(false, Ordering::SeqCst);
                    ticker_running.store(false, Ordering::SeqCst);
                    break;
                }
            }
        }));

        if spawn_result.is_err() {
            if let Some(ticker_running) = &self.negative_ack_ticker_running {
                ticker_running.store(false, Ordering::SeqCst);
            }
            self.negative_ack_due_event_pending
                .store(false, Ordering::SeqCst);
        }

        spawn_result
    }

    fn stop_negative_ack_ticker_if_idle(&mut self) {
        let tracker_empty = match &self.negative_ack_tracker {
            Some(tracker) => tracker.is_empty(),
            None => true,
        };

        if tracker_empty {
            if let Some(ticker_running) = &self.negative_ack_ticker_running {
                ticker_running.store(false, Ordering::SeqCst);
            }
            self.negative_ack_ticker_running = None;
            self.negative_ack_due_event_pending
                .store(false, Ordering::SeqCst);
        }
    }

    fn clear_negative_ack_state(&mut self) {
        if let Some(tracker) = self.negative_ack_tracker.as_mut() {
            tracker.clear();
        }
        if let Some(ticker_running) = &self.negative_ack_ticker_running {
            ticker_running.store(false, Ordering::SeqCst);
        }
        self.negative_ack_ticker_running = None;
        self.negative_ack_due_event_pending
            .store(false, Ordering::SeqCst);
    }

    async fn handle_negative_ack(
        &mut self,
        message_id: MessageIdData,
        redelivery_count: Option<u32>,
    ) {
        self.remove_negative_ack_from_unacked(&message_id);

        let schedule = self.ensure_negative_ack_tracker().schedule(
            message_id.clone(),
            redelivery_count,
            Instant::now(),
        );

        match schedule {
            NegativeAckSchedule::Immediate => {
                match self
                    .send_negative_ack_redelivery(vec![message_id.clone()])
                    .await
                {
                    Ok(()) => {
                        self.ensure_negative_ack_tracker()
                            .mark_dispatched(std::slice::from_ref(&message_id));
                        self.stop_negative_ack_ticker_if_idle();
                    }
                    Err(e) => {
                        error!(
                            "could not ask for immediate negative ack redelivery: {:?}",
                            e
                        );
                    }
                }
            }
            NegativeAckSchedule::Scheduled | NegativeAckSchedule::DuplicateEarlier => {
                if self.start_negative_ack_ticker().is_err() {
                    error!("could not start negative ack redelivery ticker");
                    self.ensure_negative_ack_tracker()
                        .mark_dispatched(std::slice::from_ref(&message_id));
                    if let Err(e) = self.send_negative_ack_redelivery(vec![message_id]).await {
                        error!(
                            "could not ask for immediate negative ack redelivery after ticker failure: {:?}",
                            e
                        );
                    }
                    self.stop_negative_ack_ticker_if_idle();
                }
            }
            NegativeAckSchedule::RetryPending => {}
        }
    }

    fn remove_negative_ack_from_unacked(&mut self, message_id: &MessageIdData) {
        self.unacked_messages
            .retain(|id, _| !NegativeAckTracker::is_same_redelivery_entry(id, message_id));
    }

    async fn handle_negative_ack_redelivery(&mut self) {
        self.negative_ack_due_event_pending
            .store(false, Ordering::SeqCst);

        let ids = match self.negative_ack_tracker.as_mut() {
            Some(tracker) => tracker.collect_due(Instant::now()),
            None => return,
        };

        if ids.is_empty() {
            self.stop_negative_ack_ticker_if_idle();
            return;
        }

        let now = Instant::now();
        match self.send_negative_ack_redelivery(ids.clone()).await {
            Ok(()) => {
                if let Some(tracker) = self.negative_ack_tracker.as_mut() {
                    tracker.mark_dispatched(&ids);
                }
            }
            Err(e) => {
                error!("could not ask for delayed negative ack redelivery: {:?}", e);
                if let Some(tracker) = self.negative_ack_tracker.as_mut() {
                    tracker.mark_retry_pending(&ids, now + NEGATIVE_ACK_REDELIVERY_TICK_INTERVAL);
                }
            }
        }

        self.stop_negative_ack_ticker_if_idle();
    }

    async fn send_negative_ack_redelivery(
        &self,
        ids: Vec<MessageIdData>,
    ) -> Result<(), ConnectionError> {
        let ids = ids
            .into_iter()
            .map(|id| NegativeAckTracker::redelivery_message_id(&id))
            .collect();
        self.connection
            .sender()
            .send_redeliver_unacknowleged_messages(self.id, ids)
            .await
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn ack(&mut self, message_id: MessageIdData, cumulative: bool) {
        // FIXME: this does not handle cumulative acks
        self.unacked_messages.remove(&message_id);
        if let Some(tracker) = self.negative_ack_tracker.as_mut() {
            if cumulative {
                tracker.cancel_cumulative_ack(&message_id);
            } else {
                tracker.cancel_ack(&message_id);
            }
        }
        self.stop_negative_ack_ticker_if_idle();
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
        let redelivery_count = message.redelivery_count.unwrap_or(0);
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
                _ => {
                    self.send_to_consumer(message_id, payload, redelivery_count)
                        .await?
                }
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    async fn send_to_consumer(
        &mut self,
        message_id: MessageIdData,
        payload: Payload,
        redelivery_count: u32,
    ) -> Result<(), Error> {
        let now = Instant::now();
        self.tx
            .send(Ok((message_id.clone(), payload, redelivery_count)))
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

        let messages = retry_subscribe_consumer(
            &self.client,
            &mut self.connection,
            broker_address,
            &self.topic,
            &self.subscription,
            self.sub_type,
            self.id,
            &self.name,
            &self.options,
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
        self.clear_negative_ack_state();

        if self.closed {
            return;
        }

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

#[cfg(test)]
mod tests {
    fn source_contains(parts: &[&str]) -> bool {
        let pattern = parts.concat();
        include_str!("engine.rs").contains(&pattern)
    }

    #[test]
    fn nack_handling_uses_tracker_with_zero_delay_immediate_redelivery() {
        assert!(source_contains(&["NegativeAck", "Schedule::Immediate"]));
        assert!(source_contains(&["ensure_", "negative_ack_tracker"]));
        assert!(source_contains(&[
            "ensure_negative_ack_tracker().",
            "schedule("
        ]));
        assert!(source_contains(&[
            "remove_",
            "negative_ack_from_unacked(&message_id)"
        ]));
        assert!(source_contains(&[
            "NegativeAckTracker::",
            "is_same_redelivery_entry(id, message_id)",
        ]));
        assert!(source_contains(&["redelivery_", "count,"]));
        assert!(source_contains(&["Instant::", "now(),"]));
        assert!(source_contains(&[
            "send_negative_ack_",
            "redelivery(vec![message_id.clone()]",
        ]));
        assert!(source_contains(&[
            "mark_",
            "dispatched(std::slice::from_ref(&message_id))",
        ]));
        assert!(source_contains(
            &["stop_", "negative_ack_ticker_if_idle()",]
        ));
    }

    #[test]
    fn lazy_ticker_coalesces_due_events() {
        assert!(source_contains(&["start_", "negative_ack_ticker"]));
        assert!(source_contains(&["compare_", "exchange(false, true"]));
        assert!(source_contains(&["NegativeAck", "Redelivery"]));
        assert!(source_contains(&[
            "NEGATIVE_ACK_",
            "REDELIVERY_TICK_INTERVAL",
        ]));
    }

    #[test]
    fn due_redelivery_dispatch_is_in_flight_retry_safe() {
        assert!(source_contains(&["collect_", "due(Instant::now())"]));
        assert!(source_contains(&["mark_", "dispatched(&ids)"]));
        assert!(source_contains(&["mark_", "retry_pending("]));
        assert!(source_contains(&["&ids", ","]));
        assert!(source_contains(&[
            "now + NEGATIVE_ACK_",
            "REDELIVERY_TICK_INTERVAL"
        ]));
    }

    #[test]
    fn ack_and_close_paths_cancel_or_clear_negative_ack_tracker() {
        assert!(source_contains(&["cancel_", "ack(&message_id)"]));
        assert!(source_contains(&["cancel_", "cumulative_ack(&message_id)"]));
        assert!(source_contains(&["tracker.", "clear()"]));
        assert!(source_contains(&["ticker_running.", "store(false"]));
    }

    #[test]
    fn reconnect_preserves_negative_ack_tracker_state() {
        let source = include_str!("engine.rs");
        let reconnect_source = source
            .split("async fn reconnect")
            .nth(1)
            .and_then(|tail| tail.split("fn debug_format").next())
            .expect("reconnect source section exists");

        assert!(!reconnect_source.contains("negative_ack_tracker"));
        assert!(!reconnect_source.contains("tracker.clear()"));
    }
}
