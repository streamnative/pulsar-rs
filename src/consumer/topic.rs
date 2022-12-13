use std::{
    io::ErrorKind,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use async_std::prelude::Stream;
use chrono::{DateTime, Utc};
use futures::{
    channel::{mpsc, oneshot},
    FutureExt, SinkExt, StreamExt,
};

use crate::{
    connection::Connection,
    consumer::{
        config::ConsumerConfig,
        data::{DeadLetterPolicy, EngineMessage, MessageData, MessageIdDataReceiver},
        engine::ConsumerEngine,
        message::Message,
    },
    error::{ConnectionError, ConsumerError},
    message::proto::MessageIdData,
    proto,
    proto::CommandConsumerStatsResponse,
    BrokerAddress, DeserializeMessage, Error, Executor, Payload, Pulsar,
};

// this is entirely public for use in reader.rs
pub struct TopicConsumer<T: DeserializeMessage, Exe: Executor> {
    pub(crate) consumer_id: u64,
    pub(crate) config: ConsumerConfig,
    topic: String,
    messages: Pin<Box<MessageIdDataReceiver>>,
    engine_tx: mpsc::UnboundedSender<EngineMessage<Exe>>,
    #[allow(unused)]
    data_type: PhantomData<fn(Payload) -> T::Output>,
    pub(crate) dead_letter_policy: Option<DeadLetterPolicy>,
    pub(super) last_message_received: Option<DateTime<Utc>>,
    pub(super) messages_received: u64,
}

impl<T: DeserializeMessage, Exe: Executor> TopicConsumer<T, Exe> {
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(super) async fn new(
        client: Pulsar<Exe>,
        topic: String,
        mut addr: BrokerAddress,
        config: ConsumerConfig,
    ) -> Result<TopicConsumer<T, Exe>, Error> {
        let ConsumerConfig {
            subscription,
            sub_type,
            batch_size,
            consumer_name,
            consumer_id,
            unacked_message_redelivery_delay,
            options,
            dead_letter_policy,
        } = config.clone();
        let consumer_id = consumer_id.unwrap_or_else(rand::random);
        let (resolver, messages) = mpsc::unbounded();
        let batch_size = batch_size.unwrap_or(1000);

        let mut connection = client.manager.get_connection(&addr).await?;
        let mut current_retries = 0u32;
        let start = Instant::now();
        let operation_retry_options = client.operation_retry_options.clone();

        loop {
            match connection
                .sender()
                .subscribe(
                    resolver.clone(),
                    topic.clone(),
                    subscription.clone(),
                    sub_type,
                    consumer_id,
                    consumer_name.clone(),
                    options.clone(),
                )
                .await
            {
                Ok(_) => {
                    if current_retries > 0 {
                        let dur = (Instant::now() - start).as_secs();
                        log::info!(
                            "subscribe({}) success after {} retries over {} seconds",
                            topic,
                            current_retries + 1,
                            dur
                        );
                    }
                    break;
                }
                Err(ConnectionError::PulsarError(Some(err), text)) => {
                    if matches!(
                        err,
                        proto::ServerError::ServiceNotReady | proto::ServerError::ConsumerBusy
                    ) {
                        // Pulsar retryable error
                        match operation_retry_options.max_retries {
                            Some(max_retries) if current_retries < max_retries => {
                                error!("subscribe({}) answered {}, retrying request after {}ms (max_retries = {:?}): {}",
                                topic, err.as_str_name(), operation_retry_options.retry_delay.as_millis(),
                                operation_retry_options.max_retries, text.unwrap_or_default());

                                current_retries += 1;
                                client
                                    .executor
                                    .delay(operation_retry_options.retry_delay)
                                    .await;

                                // we need to look up again the topic's address
                                let prev = addr;
                                addr = client.lookup_topic(&topic).await?;
                                if prev != addr {
                                    info!(
                                        "topic {} moved: previous = {:?}, new = {:?}",
                                        topic, prev, addr
                                    );
                                }

                                connection = client.manager.get_connection(&addr).await?;
                                continue;
                            }
                            _ => {
                                error!("subscribe({}) reached max retries", topic);

                                return Err(ConnectionError::PulsarError(
                                    Some(proto::ServerError::ServiceNotReady),
                                    text,
                                )
                                .into());
                            }
                        }
                    }
                }
                Err(ConnectionError::Io(e))
                    if matches!(
                        e.kind(),
                        ErrorKind::ConnectionReset
                            | ErrorKind::ConnectionAborted
                            | ErrorKind::NotConnected
                            | ErrorKind::BrokenPipe
                            | ErrorKind::TimedOut
                            | ErrorKind::Interrupted
                            | ErrorKind::UnexpectedEof
                    ) =>
                {
                    match operation_retry_options.max_retries {
                        Some(max_retries) if current_retries < max_retries => {
                            error!(
                                    "create consumer( {} {}, retrying request after {}ms (max_retries = {:?})",
                                    topic, e.kind().to_string(), operation_retry_options.retry_delay.as_millis(),
                                    operation_retry_options.max_retries
                                );

                            current_retries += 1;
                            client
                                .executor
                                .delay(operation_retry_options.retry_delay)
                                .await;

                            let addr = client.lookup_topic(&topic).await?;
                            connection = client.manager.get_connection(&addr).await?;

                            continue;
                        }
                        _ => {
                            // The error was retryable but the number of overall retries
                            // is exhausted
                            return Err(ConsumerError::Io(e).into());
                        }
                    }
                }
                Err(ConnectionError::Io(e)) => {
                    // The error is not retryable
                    return Err(ConsumerError::Io(e).into());
                }
                Err(e) => return Err(Error::Connection(e)),
            }
        }

        connection
            .sender()
            .send_flow(consumer_id, batch_size)
            .map_err(|e| {
                error!("TopicConsumer::new error[{}]: {:?}", line!(), e);
                e
            })
            .map_err(|e| Error::Consumer(ConsumerError::Connection(e)))?;

        let (engine_tx, engine_rx) = mpsc::unbounded();
        // drop_signal will be dropped when Consumer is dropped, then
        // drop_receiver will return, and we can close the consumer
        let (_drop_signal, drop_receiver) = oneshot::channel::<()>();
        let conn = connection.clone();
        let name = consumer_name.clone();
        let topic_name = topic.clone();
        let _ = client.executor.spawn(Box::pin(async move {
            let _res = drop_receiver.await;
            // if we receive a message, it indicates we want to stop this task
            if _res.is_err() {
                if let Err(e) = conn.sender().close_consumer(consumer_id).await {
                    error!(
                        "could not close consumer {:?}({}) for topic {}: {:?}",
                        consumer_name, consumer_id, topic_name, e
                    );
                }
            }
        }));

        if unacked_message_redelivery_delay.is_some() {
            let mut redelivery_tx = engine_tx.clone();
            let mut interval = client.executor.interval(Duration::from_millis(500));
            let res = client.executor.spawn(Box::pin(async move {
                while interval.next().await.is_some() {
                    if redelivery_tx
                        .send(EngineMessage::UnackedRedelivery)
                        .await
                        .is_err()
                    {
                        // Consumer shut down - stop ticker
                        break;
                    }
                }
            }));
            if res.is_err() {
                return Err(Error::Executor);
            }
        }
        let (tx, rx) = mpsc::channel(1000);
        let mut c = ConsumerEngine::new(
            client.clone(),
            connection.clone(),
            topic.clone(),
            subscription.clone(),
            sub_type,
            consumer_id,
            name,
            tx,
            messages,
            engine_rx,
            batch_size,
            unacked_message_redelivery_delay,
            dead_letter_policy.clone(),
            options.clone(),
            _drop_signal,
        );
        let f = async move {
            c.engine()
                .map(|res| {
                    debug!("consumer engine stopped: {:?}", res);
                })
                .await;
        };
        if client.executor.spawn(Box::pin(f)).is_err() {
            return Err(Error::Executor);
        }

        Ok(TopicConsumer {
            consumer_id,
            config,
            topic,
            messages: Box::pin(rx),
            engine_tx,
            data_type: PhantomData,
            dead_letter_policy,
            last_message_received: None,
            messages_received: 0,
        })
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn topic(&self) -> String {
        self.topic.clone()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn connection(&mut self) -> Result<Arc<Connection<Exe>>, Error> {
        let (resolver, response) = oneshot::channel();
        self.engine_tx
            .send(EngineMessage::GetConnection(resolver))
            .await
            .map_err(|_| ConsumerError::Connection(ConnectionError::Disconnected))?;

        response.await.map_err(|oneshot::Canceled| {
            error!("the consumer engine dropped the request");
            ConnectionError::Disconnected.into()
        })
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_stats(&mut self) -> Result<CommandConsumerStatsResponse, Error> {
        let consumer_id = self.consumer_id;
        let conn = self.connection().await?;
        let consumer_stats_response = conn.sender().get_consumer_stats(consumer_id).await?;
        Ok(consumer_stats_response)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn check_connection(&mut self) -> Result<(), Error> {
        let conn = self.connection().await?;
        info!("check connection for id {}", conn.id());
        conn.sender().send_ping().await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(EngineMessage::Ack(msg.message_id.clone(), false))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) fn acker(&self) -> mpsc::UnboundedSender<EngineMessage<Exe>> {
        self.engine_tx.clone()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn cumulative_ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(EngineMessage::Ack(msg.message_id.clone(), true))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn nack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(EngineMessage::Nack(msg.message_id.clone()))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn seek(
        &mut self,
        message_id: Option<MessageIdData>,
        timestamp: Option<u64>,
    ) -> Result<(), Error> {
        let consumer_id = self.consumer_id;
        self.connection()
            .await?
            .sender()
            .seek(consumer_id, message_id, timestamp)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn unsubscribe(&mut self) -> Result<(), Error> {
        let consumer_id = self.consumer_id;
        self.connection()
            .await?
            .sender()
            .unsubscribe(consumer_id)
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn get_last_message_id(&mut self) -> Result<MessageIdData, Error> {
        let consumer_id = self.consumer_id;
        let conn = self.connection().await?;
        let get_last_message_id_response = conn.sender().get_last_message_id(consumer_id).await?;
        Ok(get_last_message_id_response.last_message_id)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn last_message_received(&self) -> Option<DateTime<Utc>> {
        self.last_message_received
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn messages_received(&self) -> u64 {
        self.messages_received
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn config(&self) -> &ConsumerConfig {
        &self.config
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn create_message(&self, message_id: MessageIdData, payload: Payload) -> Message<T> {
        Message {
            topic: self.topic.clone(),
            message_id: MessageData {
                id: message_id,
                batch_size: payload.metadata.num_messages_in_batch,
            },
            payload,
            _phantom: PhantomData,
        }
    }
}

impl<T: DeserializeMessage, Exe: Executor> Stream for TopicConsumer<T, Exe> {
    type Item = Result<Message<T>, Error>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.messages.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok((id, payload)))) => {
                self.last_message_received = Some(Utc::now());
                self.messages_received += 1;
                Poll::Ready(Some(Ok(self.create_message(id, payload))))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
        }
    }
}
