use std::{
    collections::{BTreeSet, HashMap},
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{consumer::negative_ack_backoff::NegativeAckBackoff, message::proto::MessageIdData};

pub(crate) const DEFAULT_NACK_REDELIVERY_DELAY: Duration = Duration::from_secs(60);
pub(crate) const NEGATIVE_ACK_REDELIVERY_TICK_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NegativeAckSchedule {
    Immediate,
    Scheduled,
    DuplicateEarlier,
    RetryPending,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NegativeAckDispatchState {
    Pending,
    InFlight,
}

pub(crate) struct NegativeAckTracker {
    fixed_delay: Duration,
    backoff: Option<Arc<dyn NegativeAckBackoff + Send + Sync>>,
    entries: HashMap<NormalizedMessageId, PendingNegativeAck>,
}

impl fmt::Debug for NegativeAckTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NegativeAckTracker")
            .field("fixed_delay", &self.fixed_delay)
            .field("entries", &self.entries)
            .field("has_backoff", &self.backoff.is_some())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct NormalizedMessageId {
    ledger_id: u64,
    entry_id: u64,
    partition: Option<i32>,
}

#[derive(Debug)]
struct PendingNegativeAck {
    due_at: Instant,
    state: NegativeAckDispatchState,
    scope: PendingBatchScope,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PendingBatchScope {
    FullEntry,
    BatchIndexes(BTreeSet<i32>),
}

impl NormalizedMessageId {
    fn from_message_id(message_id: &MessageIdData) -> Self {
        Self {
            ledger_id: message_id.ledger_id,
            entry_id: message_id.entry_id,
            partition: message_id.partition,
        }
    }

    fn into_redelivery_message_id(self) -> MessageIdData {
        MessageIdData {
            ledger_id: self.ledger_id,
            entry_id: self.entry_id,
            partition: self.partition,
            batch_index: None,
            ack_set: vec![],
            batch_size: None,
            first_chunk_message_id: None,
        }
    }
}

impl PendingBatchScope {
    fn from_message_id(message_id: &MessageIdData) -> Self {
        match message_id.batch_index {
            Some(batch_index) => Self::BatchIndexes(BTreeSet::from([batch_index])),
            None => Self::FullEntry,
        }
    }

    fn merge(&mut self, incoming: PendingBatchScope) -> bool {
        match (&mut *self, incoming) {
            (Self::FullEntry, _) => false,
            (scope @ Self::BatchIndexes(_), Self::FullEntry) => {
                *scope = Self::FullEntry;
                true
            }
            (Self::BatchIndexes(existing), Self::BatchIndexes(incoming)) => {
                let mut changed = false;
                for batch_index in incoming {
                    changed |= existing.insert(batch_index);
                }
                changed
            }
        }
    }

    fn cancel_batch_index(&mut self, batch_index: i32) -> bool {
        match self {
            Self::FullEntry => false,
            Self::BatchIndexes(indexes) => {
                indexes.remove(&batch_index);
                indexes.is_empty()
            }
        }
    }

    fn cancel_batch_indexes_through(&mut self, batch_index: i32) -> bool {
        match self {
            Self::FullEntry => false,
            Self::BatchIndexes(indexes) => {
                indexes.retain(|pending| *pending > batch_index);
                indexes.is_empty()
            }
        }
    }
}

impl NegativeAckTracker {
    pub(crate) fn new(
        fixed_delay: Option<Duration>,
        backoff: Option<Arc<dyn NegativeAckBackoff + Send + Sync>>,
    ) -> Self {
        Self {
            fixed_delay: fixed_delay.unwrap_or(DEFAULT_NACK_REDELIVERY_DELAY),
            backoff,
            entries: HashMap::new(),
        }
    }

    pub(crate) fn select_delay(&self, redelivery_count: Option<u32>) -> Duration {
        match (redelivery_count, &self.backoff) {
            (Some(count), Some(backoff)) => backoff.next(count),
            _ => self.fixed_delay,
        }
    }

    pub(crate) fn redelivery_message_id(message_id: &MessageIdData) -> MessageIdData {
        NormalizedMessageId::from_message_id(message_id).into_redelivery_message_id()
    }

    pub(crate) fn is_same_redelivery_entry(left: &MessageIdData, right: &MessageIdData) -> bool {
        NormalizedMessageId::from_message_id(left) == NormalizedMessageId::from_message_id(right)
    }

    pub(crate) fn schedule(
        &mut self,
        message_id: MessageIdData,
        redelivery_count: Option<u32>,
        now: Instant,
    ) -> NegativeAckSchedule {
        let delay = self.select_delay(redelivery_count);
        let key = NormalizedMessageId::from_message_id(&message_id);
        let incoming_scope = PendingBatchScope::from_message_id(&message_id);

        if delay == Duration::ZERO {
            match self.entries.get_mut(&key) {
                None => {
                    self.entries.insert(
                        key,
                        PendingNegativeAck {
                            due_at: now,
                            state: NegativeAckDispatchState::InFlight,
                            scope: incoming_scope,
                        },
                    );
                    return NegativeAckSchedule::Immediate;
                }
                Some(existing) if existing.state == NegativeAckDispatchState::InFlight => {
                    return NegativeAckSchedule::RetryPending;
                }
                Some(existing) => {
                    existing.scope.merge(incoming_scope);
                    existing.due_at = now;
                    existing.state = NegativeAckDispatchState::InFlight;
                    return NegativeAckSchedule::Immediate;
                }
            }
        }

        let due_at = self.due_at(now, delay);

        match self.entries.get_mut(&key) {
            None => {
                self.entries.insert(
                    key,
                    PendingNegativeAck {
                        due_at,
                        state: NegativeAckDispatchState::Pending,
                        scope: incoming_scope,
                    },
                );
                NegativeAckSchedule::Scheduled
            }
            Some(existing) if existing.state == NegativeAckDispatchState::InFlight => {
                NegativeAckSchedule::RetryPending
            }
            Some(existing) => {
                let scope_changed = existing.scope.merge(incoming_scope);
                if due_at < existing.due_at {
                    existing.due_at = due_at;
                    NegativeAckSchedule::Scheduled
                } else if scope_changed {
                    NegativeAckSchedule::Scheduled
                } else {
                    NegativeAckSchedule::DuplicateEarlier
                }
            }
        }
    }

    fn due_at(&self, now: Instant, delay: Duration) -> Instant {
        now.checked_add(delay)
            .or_else(|| now.checked_add(self.fixed_delay))
            .unwrap_or_else(|| now + DEFAULT_NACK_REDELIVERY_DELAY)
    }

    #[cfg(test)]
    pub(crate) fn pending_len(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.state == NegativeAckDispatchState::Pending)
            .count()
    }

    #[cfg(test)]
    pub(crate) fn in_flight_len(&self) -> usize {
        self.entries
            .values()
            .filter(|entry| entry.state == NegativeAckDispatchState::InFlight)
            .count()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn next_due_time(&self) -> Option<Instant> {
        self.entries
            .values()
            .filter(|entry| entry.state == NegativeAckDispatchState::Pending)
            .map(|entry| entry.due_at)
            .min()
    }

    pub(crate) fn collect_due(&mut self, now: Instant) -> Vec<MessageIdData> {
        let mut due = Vec::new();
        for (key, entry) in &mut self.entries {
            if entry.state == NegativeAckDispatchState::Pending && entry.due_at <= now {
                entry.state = NegativeAckDispatchState::InFlight;
                due.push((*key).into_redelivery_message_id());
            }
        }
        due
    }

    pub(crate) fn mark_dispatched(&mut self, message_ids: &[MessageIdData]) {
        for message_id in message_ids {
            self.entries
                .remove(&NormalizedMessageId::from_message_id(message_id));
        }
    }

    pub(crate) fn mark_retry_pending(&mut self, message_ids: &[MessageIdData], retry_at: Instant) {
        for message_id in message_ids {
            if let Some(entry) = self
                .entries
                .get_mut(&NormalizedMessageId::from_message_id(message_id))
            {
                entry.state = NegativeAckDispatchState::Pending;
                entry.due_at = retry_at;
            }
        }
    }

    pub(crate) fn cancel_ack(&mut self, message_id: &MessageIdData) {
        let key = NormalizedMessageId::from_message_id(message_id);
        let Some(batch_index) = message_id.batch_index else {
            self.entries.remove(&key);
            return;
        };
        if self
            .entries
            .get_mut(&key)
            .is_some_and(|entry| entry.scope.cancel_batch_index(batch_index))
        {
            self.entries.remove(&key);
        }
    }

    pub(crate) fn cancel_cumulative_ack(&mut self, message_id: &MessageIdData) {
        let ack_key = NormalizedMessageId::from_message_id(message_id);
        let mut remove_keys = Vec::new();

        for key in self.entries.keys() {
            if key.partition != ack_key.partition {
                continue;
            }
            if key.ledger_id < ack_key.ledger_id
                || (key.ledger_id == ack_key.ledger_id && key.entry_id < ack_key.entry_id)
            {
                remove_keys.push(*key);
            }
        }

        for key in remove_keys {
            self.entries.remove(&key);
        }

        let Some(batch_index) = message_id.batch_index else {
            self.entries.remove(&ack_key);
            return;
        };
        if self
            .entries
            .get_mut(&ack_key)
            .is_some_and(|entry| entry.scope.cancel_batch_indexes_through(batch_index))
        {
            self.entries.remove(&ack_key);
        }
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedBackoff(Duration);

    impl NegativeAckBackoff for FixedBackoff {
        fn next(&self, _redelivery_count: u32) -> Duration {
            self.0
        }
    }

    fn backoff(delay: Duration) -> Arc<dyn NegativeAckBackoff + Send + Sync> {
        Arc::new(FixedBackoff(delay))
    }

    fn message_id(
        ledger_id: u64,
        entry_id: u64,
        partition: Option<i32>,
        batch_index: Option<i32>,
    ) -> MessageIdData {
        MessageIdData {
            ledger_id,
            entry_id,
            partition,
            batch_index,
            ack_set: vec![],
            batch_size: None,
            first_chunk_message_id: None,
        }
    }

    #[test]
    fn missing_fixed_delay_defaults_to_sixty_seconds() {
        let tracker = NegativeAckTracker::new(None, None);

        assert_eq!(tracker.select_delay(None), DEFAULT_NACK_REDELIVERY_DELAY);
        assert_eq!(tracker.select_delay(None), Duration::from_secs(60));
    }

    #[test]
    fn explicit_zero_fixed_delay_selects_immediate_redelivery() {
        let tracker = NegativeAckTracker::new(Some(Duration::ZERO), None);

        assert_eq!(tracker.select_delay(None), Duration::ZERO);
    }

    #[test]
    fn message_redelivery_count_uses_configured_backoff() {
        let tracker = NegativeAckTracker::new(None, Some(backoff(Duration::from_secs(42))));

        assert_eq!(tracker.select_delay(Some(7)), Duration::from_secs(42));
    }

    #[test]
    fn missing_redelivery_count_uses_fixed_delay_even_when_backoff_exists() {
        let tracker = NegativeAckTracker::new(
            Some(Duration::from_secs(5)),
            Some(backoff(Duration::from_secs(42))),
        );

        assert_eq!(tracker.select_delay(None), Duration::from_secs(5));
    }

    #[test]
    fn zero_backoff_delay_selects_immediate_redelivery() {
        let tracker =
            NegativeAckTracker::new(Some(Duration::from_secs(5)), Some(backoff(Duration::ZERO)));

        assert_eq!(tracker.select_delay(Some(7)), Duration::ZERO);
    }

    #[test]
    fn immediate_schedule_is_in_flight_until_dispatch_result() {
        let now = Instant::now();
        let mut tracker = NegativeAckTracker::new(Some(Duration::ZERO), None);

        assert_eq!(
            tracker.schedule(message_id(1, 2, None, None), None, now),
            NegativeAckSchedule::Immediate
        );
        assert_eq!(tracker.pending_len(), 0);
        assert_eq!(tracker.in_flight_len(), 1);

        tracker.mark_retry_pending(
            &[message_id(1, 2, None, None)],
            now + NEGATIVE_ACK_REDELIVERY_TICK_INTERVAL,
        );
        assert_eq!(tracker.pending_len(), 1);
        assert_eq!(tracker.in_flight_len(), 0);
        assert!(tracker
            .collect_due(now + Duration::from_millis(499))
            .is_empty());

        let due = tracker.collect_due(now + NEGATIVE_ACK_REDELIVERY_TICK_INTERVAL);
        tracker.mark_dispatched(&due);
        assert!(tracker.is_empty());
    }

    #[test]
    fn oversized_backoff_delay_falls_back_without_panicking() {
        let now = Instant::now();
        let mut tracker =
            NegativeAckTracker::new(Some(Duration::from_secs(5)), Some(backoff(Duration::MAX)));

        assert_eq!(
            tracker.schedule(message_id(1, 2, None, None), Some(7), now),
            NegativeAckSchedule::Scheduled
        );
        assert_eq!(tracker.next_due_time(), Some(now + Duration::from_secs(5)));
    }

    #[test]
    fn redelivery_message_id_normalizes_batch_specific_fields() {
        let mut original = message_id(1, 2, Some(0), Some(1));
        original.ack_set = vec![1];
        original.batch_size = Some(3);
        original.first_chunk_message_id = Some(Box::new(message_id(1, 1, Some(0), None)));

        let redelivery_id = NegativeAckTracker::redelivery_message_id(&original);

        assert_eq!(redelivery_id.ledger_id, 1);
        assert_eq!(redelivery_id.entry_id, 2);
        assert_eq!(redelivery_id.partition, Some(0));
        assert_eq!(redelivery_id.batch_index, None);
        assert!(redelivery_id.ack_set.is_empty());
        assert_eq!(redelivery_id.batch_size, None);
        assert_eq!(redelivery_id.first_chunk_message_id, None);
    }

    #[test]
    fn same_redelivery_entry_ignores_batch_specific_fields() {
        let mut original = message_id(1, 2, Some(0), Some(1));
        original.ack_set = vec![1];
        original.batch_size = Some(3);

        assert!(NegativeAckTracker::is_same_redelivery_entry(
            &original,
            &message_id(1, 2, Some(0), Some(2))
        ));
        assert!(!NegativeAckTracker::is_same_redelivery_entry(
            &original,
            &message_id(1, 2, Some(1), Some(1))
        ));
        assert!(!NegativeAckTracker::is_same_redelivery_entry(
            &original,
            &message_id(1, 3, Some(0), Some(1))
        ));
    }

    #[test]
    fn batch_ids_share_entry_key_but_track_pending_indexes_separately() {
        let now = Instant::now();
        let mut tracker = NegativeAckTracker::new(Some(Duration::from_secs(10)), None);

        assert_eq!(
            tracker.schedule(message_id(1, 2, Some(0), Some(1)), None, now),
            NegativeAckSchedule::Scheduled
        );
        assert_eq!(
            tracker.schedule(message_id(1, 2, Some(0), Some(2)), None, now),
            NegativeAckSchedule::Scheduled
        );

        assert_eq!(tracker.pending_len(), 1);
        tracker.cancel_ack(&message_id(1, 2, Some(0), Some(1)));
        assert_eq!(tracker.pending_len(), 1);
        tracker.cancel_ack(&message_id(1, 2, Some(0), Some(2)));
        assert!(tracker.is_empty());
    }

    #[test]
    fn duplicate_schedule_keeps_earliest_due_time() {
        let now = Instant::now();
        let mut tracker = NegativeAckTracker::new(Some(Duration::from_secs(10)), None);

        assert_eq!(
            tracker.schedule(message_id(1, 2, None, Some(1)), None, now),
            NegativeAckSchedule::Scheduled
        );
        assert_eq!(
            tracker.schedule(
                message_id(1, 2, None, Some(1)),
                None,
                now + Duration::from_secs(5)
            ),
            NegativeAckSchedule::DuplicateEarlier
        );

        assert_eq!(tracker.pending_len(), 1);
        assert_eq!(tracker.next_due_time(), Some(now + Duration::from_secs(10)));
    }

    #[test]
    fn whole_entry_ack_cancels_all_pending_batch_indexes() {
        let now = Instant::now();
        let mut tracker = NegativeAckTracker::new(Some(Duration::from_secs(10)), None);

        tracker.schedule(message_id(1, 2, None, Some(1)), None, now);
        tracker.schedule(message_id(1, 2, None, Some(2)), None, now);
        tracker.cancel_ack(&message_id(1, 2, None, None));

        assert!(tracker.is_empty());
    }

    #[test]
    fn ack_set_ack_with_batch_index_preserves_sibling_pending_indexes() {
        let now = Instant::now();
        let mut ack = message_id(1, 2, None, Some(1));
        ack.ack_set = vec![0];
        let mut tracker = NegativeAckTracker::new(Some(Duration::from_secs(10)), None);

        tracker.schedule(message_id(1, 2, None, Some(1)), None, now);
        tracker.schedule(message_id(1, 2, None, Some(2)), None, now);
        tracker.cancel_ack(&ack);

        assert_eq!(tracker.pending_len(), 1);
        tracker.cancel_ack(&message_id(1, 2, None, Some(2)));
        assert!(tracker.is_empty());
    }

    #[test]
    fn ack_set_without_batch_index_cancels_the_whole_pending_entry() {
        let now = Instant::now();
        let mut ack = message_id(1, 2, None, None);
        ack.ack_set = vec![0];
        let mut tracker = NegativeAckTracker::new(Some(Duration::from_secs(10)), None);

        tracker.schedule(message_id(1, 2, None, Some(1)), None, now);
        tracker.schedule(message_id(1, 2, None, Some(2)), None, now);
        tracker.cancel_ack(&ack);

        assert!(tracker.is_empty());
    }

    #[test]
    fn cumulative_ack_is_partition_scoped_and_handles_same_entry_batches() {
        let now = Instant::now();
        let mut tracker = NegativeAckTracker::new(Some(Duration::from_secs(10)), None);

        tracker.schedule(message_id(1, 1, Some(0), Some(1)), None, now);
        tracker.schedule(message_id(1, 2, Some(0), Some(1)), None, now);
        tracker.schedule(message_id(1, 2, Some(0), Some(2)), None, now);
        tracker.schedule(message_id(1, 3, Some(0), Some(1)), None, now);
        tracker.schedule(message_id(1, 1, Some(1), Some(1)), None, now);

        tracker.cancel_cumulative_ack(&message_id(1, 2, Some(0), Some(1)));

        assert_eq!(tracker.pending_len(), 3);
        tracker.cancel_ack(&message_id(1, 2, Some(0), Some(2)));
        assert_eq!(tracker.pending_len(), 2);
        tracker.cancel_ack(&message_id(1, 3, Some(0), Some(1)));
        tracker.cancel_ack(&message_id(1, 1, Some(1), Some(1)));
        assert!(tracker.is_empty());
    }

    #[test]
    fn cumulative_ack_set_with_batch_index_preserves_later_sibling_indexes() {
        let now = Instant::now();
        let mut ack = message_id(1, 2, Some(0), Some(1));
        ack.ack_set = vec![0];
        let mut tracker = NegativeAckTracker::new(Some(Duration::from_secs(10)), None);

        tracker.schedule(message_id(1, 1, Some(0), Some(1)), None, now);
        tracker.schedule(message_id(1, 2, Some(0), Some(1)), None, now);
        tracker.schedule(message_id(1, 2, Some(0), Some(2)), None, now);
        tracker.schedule(message_id(1, 3, Some(0), Some(1)), None, now);

        tracker.cancel_cumulative_ack(&ack);

        assert_eq!(tracker.pending_len(), 2);
        tracker.cancel_ack(&message_id(1, 2, Some(0), Some(2)));
        tracker.cancel_ack(&message_id(1, 3, Some(0), Some(1)));
        assert!(tracker.is_empty());
    }

    #[test]
    fn collect_due_marks_entries_in_flight_and_returns_entry_level_ids() {
        let now = Instant::now();
        let mut tracker = NegativeAckTracker::new(Some(Duration::from_secs(10)), None);
        let mut original = message_id(1, 2, Some(0), Some(1));
        original.ack_set = vec![1];
        original.batch_size = Some(3);
        original.first_chunk_message_id = Some(Box::new(message_id(1, 1, Some(0), None)));

        tracker.schedule(original, None, now);

        assert!(tracker.collect_due(now + Duration::from_secs(9)).is_empty());
        let due = tracker.collect_due(now + Duration::from_secs(10));
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].ledger_id, 1);
        assert_eq!(due[0].entry_id, 2);
        assert_eq!(due[0].partition, Some(0));
        assert_eq!(due[0].batch_index, None);
        assert!(due[0].ack_set.is_empty());
        assert_eq!(due[0].batch_size, None);
        assert_eq!(due[0].first_chunk_message_id, None);
        assert_eq!(tracker.pending_len(), 0);
        assert_eq!(tracker.in_flight_len(), 1);
        assert!(tracker
            .collect_due(now + Duration::from_secs(11))
            .is_empty());
    }

    #[test]
    fn dispatch_lifecycle_removes_or_retries_in_flight_entries() {
        let now = Instant::now();
        let mut tracker = NegativeAckTracker::new(Some(Duration::from_secs(10)), None);

        tracker.schedule(message_id(1, 2, None, None), None, now);
        let due = tracker.collect_due(now + Duration::from_secs(10));
        tracker.mark_retry_pending(&due, now + NEGATIVE_ACK_REDELIVERY_TICK_INTERVAL);
        assert_eq!(tracker.pending_len(), 1);
        assert_eq!(tracker.in_flight_len(), 0);
        assert!(tracker
            .collect_due(now + Duration::from_millis(499))
            .is_empty());
        let retried = tracker.collect_due(now + NEGATIVE_ACK_REDELIVERY_TICK_INTERVAL);
        tracker.mark_dispatched(&retried);
        assert!(tracker.is_empty());
    }

    #[test]
    fn clear_removes_pending_and_in_flight_entries() {
        let now = Instant::now();
        let mut tracker = NegativeAckTracker::new(Some(Duration::from_secs(10)), None);

        tracker.schedule(message_id(1, 2, None, None), None, now);
        tracker.schedule(message_id(1, 3, None, None), None, now);
        tracker.collect_due(now + Duration::from_secs(10));
        tracker.clear();

        assert!(tracker.is_empty());
    }

    #[test]
    fn scheduled_nack_is_not_due_before_delay_elapses() {
        let delay = Duration::from_secs(10);
        let now = Instant::now();
        let mut tracker = NegativeAckTracker::new(Some(delay), None);

        tracker.schedule(message_id(1, 2, None, None), None, now);

        assert!(tracker
            .collect_due(now + delay - Duration::from_millis(1))
            .is_empty());
    }

    #[test]
    fn scheduled_nack_is_due_at_or_after_delay_elapses() {
        let delay = Duration::from_secs(10);
        let now = Instant::now();
        let mut tracker = NegativeAckTracker::new(Some(delay), None);

        tracker.schedule(message_id(1, 3, None, None), None, now);

        let due = tracker.collect_due(now + delay);
        assert_eq!(due.len(), 1);
    }

    #[test]
    fn nack_with_id_fixed_delay_fallback_when_backoff_configured() {
        let tracker = NegativeAckTracker::new(
            Some(Duration::from_secs(5)),
            Some(backoff(Duration::from_secs(42))),
        );

        assert_eq!(tracker.select_delay(None), Duration::from_secs(5));
        assert_eq!(tracker.select_delay(Some(1)), Duration::from_secs(42));
    }

    #[test]
    fn dlq_regression_batch_normalization_no_duplicate_scheduling() {
        let now = Instant::now();
        let mut tracker = NegativeAckTracker::new(Some(Duration::from_millis(1)), None);

        tracker.schedule(message_id(1, 2, None, Some(0)), None, now);
        tracker.schedule(message_id(1, 2, None, Some(1)), None, now);

        assert_eq!(tracker.pending_len(), 1);
        let due = tracker.collect_due(now + Duration::from_millis(1));
        assert_eq!(due.len(), 1);
    }
}
