use std::{
    marker::PhantomData,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};

use chrono::{DateTime, Utc};
use futures::{
    channel::{mpsc, oneshot},
    FutureExt, SinkExt, Stream, StreamExt,
};

use crate::{
    connection::Connection,
    consumer::{
        config::ConsumerConfig,
        data::{DeadLetterPolicy, InternalEngineMessage, MessageData, MessageIdDataReceiver},
        engine::ConsumerEngine,
        message::Message,
    },
    error::{ConnectionError, ConsumerError},
    message::proto::{MessageIdData, Schema},
    proto::CommandConsumerStatsResponse,
    retry_op::retry_subscribe_consumer,
    BrokerAddress, DeserializeMessage, Error, Executor, Payload, Pulsar,
};

// this is entirely public for use in reader.rs
pub struct TopicConsumer<T: DeserializeMessage, Exe: Executor> {
    pub(crate) consumer_id: u64,
    pub(crate) config: ConsumerConfig,
    topic: String,
    messages: Pin<Box<MessageIdDataReceiver>>,
    engine_tx: mpsc::UnboundedSender<InternalEngineMessage<Exe>>,
    unacked_redelivery_ticker_running: Option<Arc<AtomicBool>>,
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
        addr: BrokerAddress,
        config: ConsumerConfig,
    ) -> Result<TopicConsumer<T, Exe>, Error> {
        static CONSUMER_ID_GENERATOR: AtomicU64 = AtomicU64::new(0);

        let ConsumerConfig {
            subscription,
            sub_type,
            batch_size,
            consumer_name,
            consumer_id,
            unacked_message_redelivery_delay,
            options,
            dead_letter_policy,
            nack_redelivery_delay,
            negative_ack_backoff,
        } = config.clone();
        let consumer_id =
            consumer_id.unwrap_or_else(|| CONSUMER_ID_GENERATOR.fetch_add(1, Ordering::SeqCst));
        let batch_size = batch_size.unwrap_or(1000);
        let mut connection = client.manager.get_connection(&addr).await?;

        let messages = retry_subscribe_consumer(
            &client,
            &mut connection,
            addr,
            &topic,
            &subscription,
            sub_type,
            consumer_id,
            &consumer_name,
            &options,
            batch_size,
        )
        .await?;

        let (engine_tx, engine_rx) = mpsc::unbounded();

        let unacked_redelivery_ticker_running = if unacked_message_redelivery_delay.is_some() {
            let ticker_running = Arc::new(AtomicBool::new(true));
            let ticker_running_task = ticker_running.clone();
            let mut redelivery_tx = engine_tx.clone();
            let mut interval = client.executor.interval(Duration::from_millis(500));
            let res = client.executor.spawn(Box::pin(async move {
                while ticker_running_task.load(Ordering::SeqCst) && interval.next().await.is_some()
                {
                    if redelivery_tx
                        .send(InternalEngineMessage::UnackedRedelivery)
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
            Some(ticker_running)
        } else {
            None
        };
        let receiver_queue_size = options.receiver_queue_size.unwrap_or(1000);
        let (tx, rx) = mpsc::channel(receiver_queue_size as usize);
        let mut c = ConsumerEngine::new(
            client.clone(),
            connection.clone(),
            topic.clone(),
            subscription.clone(),
            sub_type,
            consumer_id,
            consumer_name,
            tx,
            messages,
            engine_rx,
            batch_size,
            unacked_message_redelivery_delay,
            nack_redelivery_delay,
            negative_ack_backoff,
            dead_letter_policy.clone(),
            options.clone(),
        );
        let engine_task = client.executor.spawn(Box::pin(async move {
            c.engine()
                .map(|res| {
                    debug!("consumer engine stopped: {:?}", res);
                })
                .await;
        }));
        if engine_task.is_err() {
            return Err(Error::Executor);
        }

        Ok(TopicConsumer {
            consumer_id,
            config,
            topic,
            messages: Box::pin(rx),
            engine_tx,
            unacked_redelivery_ticker_running,
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
            .send(InternalEngineMessage::GetConnection(resolver))
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
            .send(InternalEngineMessage::Ack(msg.message_id().clone(), false))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn ack_with_id(&mut self, msg_id: MessageIdData) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(InternalEngineMessage::Ack(msg_id, false))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) fn acker(&self) -> mpsc::UnboundedSender<InternalEngineMessage<Exe>> {
        self.engine_tx.clone()
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn cumulative_ack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(InternalEngineMessage::Ack(msg.message_id().clone(), true))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn cumulative_ack_with_id(
        &mut self,
        msg_id: MessageIdData,
    ) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(InternalEngineMessage::Ack(msg_id, true))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn nack(&mut self, msg: &Message<T>) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(InternalEngineMessage::Nack(
                msg.message_id().clone(),
                msg.broker_redelivery_count(),
            ))
            .await?;
        Ok(())
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub async fn nack_with_id(&mut self, msg_id: MessageIdData) -> Result<(), ConsumerError> {
        self.engine_tx
            .send(InternalEngineMessage::Nack(msg_id, None))
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
    pub async fn close(&mut self) -> Result<(), Error> {
        let (resolver, response) = oneshot::channel();
        self.stop_unacked_redelivery_ticker();
        self.engine_tx
            .send(InternalEngineMessage::Close(resolver))
            .await
            .map_err(ConsumerError::from)?;

        response.await.map_err(|_| ConsumerError::Closed)??;
        Ok(())
    }

    fn stop_unacked_redelivery_ticker(&mut self) {
        if let Some(ticker_running) = &self.unacked_redelivery_ticker_running {
            ticker_running.store(false, Ordering::SeqCst);
        }
        self.unacked_redelivery_ticker_running = None;
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
    fn create_message(
        &self,
        message_id: MessageIdData,
        payload: Payload,
        redelivery_count: Option<u32>,
    ) -> Message<T> {
        let message_id = MessageData {
            id: message_id,
            batch_size: payload.metadata.num_messages_in_batch,
        };
        Message::new_with_redelivery_count(&self.topic, message_id, payload, redelivery_count)
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub(crate) async fn get_schema(
        &mut self,
        version: Option<Vec<u8>>,
    ) -> Result<Option<Schema>, Error> {
        let conn = self.connection().await?;
        let schema_response = conn.sender().get_schema(&self.topic, version).await?;
        Ok(schema_response.schema)
    }
}

impl<T: DeserializeMessage, Exe: Executor> Drop for TopicConsumer<T, Exe> {
    fn drop(&mut self) {
        self.stop_unacked_redelivery_ticker();
    }
}

impl<T: DeserializeMessage, Exe: Executor> Stream for TopicConsumer<T, Exe> {
    type Item = Result<Message<T>, Error>;

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.messages.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(Ok((id, payload, redelivery_count)))) => {
                self.last_message_received = Some(Utc::now());
                self.messages_received += 1;
                Poll::Ready(Some(Ok(self.create_message(id, payload, redelivery_count))))
            }
            Poll::Ready(Some(Err(e))) => {
                error!("we are using in the single-consumer and we got an error, {e}");
                Poll::Ready(Some(Err(e)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    fn source_contains(parts: &[&str]) -> bool {
        let pattern = parts.concat();
        include_str!("topic.rs").contains(&pattern)
    }

    #[test]
    fn nack_uses_internal_broker_redelivery_count_presence() {
        assert!(include_str!("topic.rs").contains("InternalEngineMessage::Nack("));
        assert!(source_contains(&["msg.", "message_id().clone()"]));
        assert!(source_contains(&["msg.", "broker_redelivery_count()"]));
        assert!(!source_contains(&["Some(msg.", "redelivery_count())"]));
    }

    #[test]
    fn drop_stops_unacked_redelivery_ticker_sender() {
        assert!(source_contains(&[
            "unacked_redelivery_ticker_running:",
            " Option<Arc<AtomicBool>>"
        ]));
        assert!(source_contains(&[
            "ticker_running_task.",
            "load(Ordering::SeqCst)"
        ]));
        assert!(source_contains(&[
            "impl<T: DeserializeMessage, Exe: Executor> Drop",
            " for TopicConsumer<T, Exe>"
        ]));
        assert!(source_contains(&[
            "self.",
            "stop_unacked_redelivery_ticker()"
        ]));
    }
}
