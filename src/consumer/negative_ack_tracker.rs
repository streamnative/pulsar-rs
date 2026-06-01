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
}
