use std::{
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

#[derive(Debug)]
pub(crate) struct NegativeAckTracker {
    fixed_delay: Duration,
    backoff: Option<Arc<dyn NegativeAckBackoff + Send + Sync>>,
}

impl NegativeAckTracker {
    pub(crate) fn new(
        fixed_delay: Option<Duration>,
        backoff: Option<Arc<dyn NegativeAckBackoff + Send + Sync>>,
    ) -> Self {
        Self {
            fixed_delay: fixed_delay.unwrap_or(DEFAULT_NACK_REDELIVERY_DELAY),
            backoff,
        }
    }

    pub(crate) fn select_delay(&self, redelivery_count: Option<u32>) -> Duration {
        match (redelivery_count, &self.backoff) {
            (Some(count), Some(backoff)) => backoff.next(count),
            _ => self.fixed_delay,
        }
    }

    pub(crate) fn schedule(
        &mut self,
        _message_id: MessageIdData,
        _redelivery_count: Option<u32>,
        _now: Instant,
    ) -> NegativeAckSchedule {
        match self.select_delay(_redelivery_count) {
            Duration::ZERO => NegativeAckSchedule::Immediate,
            _ => NegativeAckSchedule::Scheduled,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
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
        assert_eq!(
            tracker.next_due_time(),
            Some(now + Duration::from_secs(10))
        );
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
    fn ack_set_ack_cancels_the_whole_pending_entry() {
        let now = Instant::now();
        let mut ack = message_id(1, 2, None, Some(1));
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
        assert!(tracker.collect_due(now + Duration::from_secs(11)).is_empty());
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
        assert!(tracker.collect_due(now + Duration::from_millis(499)).is_empty());
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
}
