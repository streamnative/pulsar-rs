use std::collections::BTreeMap;

use crate::{
    consumer::initial_position::InitialPosition,
    message::proto::{MessageIdData, Schema},
};

/// Configuration options for consumers
#[derive(Clone, Default, Debug)]
pub struct ConsumerOptions {
    pub priority_level: Option<i32>,
    /// Signal whether the subscription should be backed by a
    /// durable cursor or not
    pub durable: Option<bool>,
    /// If specified, the subscription will position the cursor
    /// marked-delete position on the particular message id and
    /// will send messages from that point
    pub start_message_id: Option<MessageIdData>,
    /// Add optional metadata key=value to this consumer
    pub metadata: BTreeMap<String, String>,
    pub read_compacted: Option<bool>,
    pub schema: Option<Schema>,
    /// Signal whether the subscription will initialize on latest
    /// or earliest message (default on latest)
    ///
    /// an enum can be used to initialize it:
    ///
    /// ```rust,ignore
    /// ConsumerOptions {
    ///     initial_position: InitialPosition::Earliest,
    /// }
    /// ```
    pub initial_position: InitialPosition,
    /// Mark the subscription as "replicated". Pulsar will make sure
    /// to periodically sync the state of replicated subscriptions
    /// across different clusters (when using geo-replication).
    pub replicate_subscription_state: Option<bool>,
}

impl ConsumerOptions {
    /// within options, sets the priority level
    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_priority_level(mut self, priority_level: i32) -> Self {
        self.priority_level = Some(priority_level);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = Some(durable);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn starting_on_message(mut self, message_id_data: MessageIdData) -> Self {
        self.start_message_id = Some(message_id_data);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_metadata(mut self, metadata: BTreeMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn read_compacted(mut self, read_compacted: bool) -> Self {
        self.read_compacted = Some(read_compacted);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schema = Some(schema);
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_initial_position(mut self, initial_position: InitialPosition) -> Self {
        self.initial_position = initial_position;
        self
    }

    #[cfg_attr(feature = "telemetry", tracing::instrument(skip_all))]
    pub fn with_replicate_subscription_state(mut self, replicate_subscription_state: bool) -> Self {
        self.replicate_subscription_state = Some(replicate_subscription_state);
        self
    }
}
