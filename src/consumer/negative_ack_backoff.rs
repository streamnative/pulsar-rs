use std::time::Duration;

use crate::Error;

const MIN_DELAY_MUST_NOT_EXCEED_MAX_DELAY: &str =
    "min_delay must be less than or equal to max_delay";
const MULTIPLIER_MUST_BE_GREATER_THAN_ONE: &str = "multiplier must be greater than 1.0";
const MULTIPLIER_MUST_BE_FINITE: &str = "multiplier must be finite";
const DELAY_DURATION_TOO_LARGE: &str = "delay duration is too large";

/// Policy for computing a negative-acknowledgment redelivery delay from a redelivery count.
pub trait NegativeAckBackoff: Send + Sync + std::fmt::Debug {
    /// Returns the delay to use for the provided redelivery count.
    fn next(&self, redelivery_count: u32) -> Duration;
}

/// Built-in negative-acknowledgment redelivery backoff using a multiplier.
#[derive(Clone, Debug)]
pub struct MultiplierRedeliveryBackoff {
    min_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
}

impl Default for MultiplierRedeliveryBackoff {
    fn default() -> Self {
        Self {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(600),
            multiplier: 2.0,
        }
    }
}

impl MultiplierRedeliveryBackoff {
    /// Creates a builder for configuring a multiplier redelivery backoff policy.
    pub fn builder() -> MultiplierRedeliveryBackoffBuilder {
        MultiplierRedeliveryBackoffBuilder::default()
    }
}

impl NegativeAckBackoff for MultiplierRedeliveryBackoff {
    fn next(&self, redelivery_count: u32) -> Duration {
        if self.min_delay == Duration::ZERO {
            return Duration::ZERO;
        }

        if redelivery_count == 0 {
            return self.min_delay;
        }

        if self.min_delay >= self.max_delay {
            return self.max_delay;
        }

        let min_delay_secs = self.min_delay.as_secs_f64();
        let max_delay_secs = self.max_delay.as_secs_f64();
        let multiplier_log = self.multiplier.ln();
        let cap_after = (max_delay_secs / min_delay_secs).ln() / multiplier_log;

        if !cap_after.is_finite() || redelivery_count as f64 >= cap_after {
            return self.max_delay;
        }

        let delay_secs = min_delay_secs * self.multiplier.powf(redelivery_count as f64);
        if !delay_secs.is_finite() || delay_secs >= max_delay_secs {
            return self.max_delay;
        }

        match Duration::try_from_secs_f64(delay_secs) {
            Ok(delay) if delay < self.max_delay => delay,
            _ => self.max_delay,
        }
    }
}

/// Builder for [`MultiplierRedeliveryBackoff`].
#[derive(Clone, Debug)]
pub struct MultiplierRedeliveryBackoffBuilder {
    min_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
}

impl Default for MultiplierRedeliveryBackoffBuilder {
    fn default() -> Self {
        Self {
            min_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(600),
            multiplier: 2.0,
        }
    }
}

impl MultiplierRedeliveryBackoffBuilder {
    /// Sets the minimum delay returned for redelivery count `0`.
    pub fn min_delay(mut self, min_delay: Duration) -> Self {
        self.min_delay = min_delay;
        self
    }

    /// Sets the maximum delay cap for the backoff sequence.
    pub fn max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Sets the multiplier applied for each redelivery count.
    pub fn multiplier(mut self, multiplier: f64) -> Self {
        self.multiplier = multiplier;
        self
    }

    /// Builds a validated [`MultiplierRedeliveryBackoff`].
    pub fn build(self) -> Result<MultiplierRedeliveryBackoff, Error> {
        validate_delay_duration(self.min_delay)?;
        validate_delay_duration(self.max_delay)?;

        if self.min_delay > self.max_delay {
            return Err(Error::Custom(
                MIN_DELAY_MUST_NOT_EXCEED_MAX_DELAY.to_string(),
            ));
        }

        if !self.multiplier.is_finite() {
            return Err(Error::Custom(MULTIPLIER_MUST_BE_FINITE.to_string()));
        }

        if self.multiplier <= 1.0 {
            return Err(Error::Custom(
                MULTIPLIER_MUST_BE_GREATER_THAN_ONE.to_string(),
            ));
        }

        Ok(MultiplierRedeliveryBackoff {
            min_delay: self.min_delay,
            max_delay: self.max_delay,
            multiplier: self.multiplier,
        })
    }
}

fn validate_delay_duration(delay: Duration) -> Result<(), Error> {
    if delay.as_millis() > u64::MAX as u128 {
        return Err(Error::Custom(DELAY_DURATION_TOO_LARGE.to_string()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn custom_backoff() -> MultiplierRedeliveryBackoff {
        MultiplierRedeliveryBackoff::builder()
            .min_delay(Duration::from_secs(2))
            .max_delay(Duration::from_secs(30))
            .multiplier(3.0)
            .build()
            .unwrap()
    }

    fn assert_custom_error(result: Result<MultiplierRedeliveryBackoff, Error>, expected: &str) {
        match result {
            Err(Error::Custom(message)) => assert_eq!(message, expected),
            other => panic!("expected Error::Custom({expected:?}), got {other:?}"),
        }
    }

    #[test]
    fn default_multiplier_backoff_sequence_matches_pulsar_vectors() {
        let backoff = MultiplierRedeliveryBackoff::default();

        assert_eq!(backoff.next(0), Duration::from_secs(1));
        assert_eq!(backoff.next(1), Duration::from_secs(2));
        assert_eq!(backoff.next(2), Duration::from_secs(4));
        assert_eq!(backoff.next(9), Duration::from_secs(512));
        assert_eq!(backoff.next(10), Duration::from_secs(600));
        assert_eq!(backoff.next(u32::MAX), Duration::from_secs(600));
    }

    #[test]
    fn custom_multiplier_backoff_sequence_caps_at_max_delay() {
        let backoff = custom_backoff();

        assert_eq!(backoff.next(0), Duration::from_secs(2));
        assert_eq!(backoff.next(1), Duration::from_secs(6));
        assert_eq!(backoff.next(2), Duration::from_secs(18));
        assert_eq!(backoff.next(3), Duration::from_secs(30));
    }

    #[test]
    fn zero_min_delay_returns_zero_for_all_counts() {
        let backoff = MultiplierRedeliveryBackoff::builder()
            .min_delay(Duration::ZERO)
            .build()
            .unwrap();

        assert_eq!(backoff.next(0), Duration::ZERO);
        assert_eq!(backoff.next(1), Duration::ZERO);
        assert_eq!(backoff.next(10), Duration::ZERO);
        assert_eq!(backoff.next(u32::MAX), Duration::ZERO);
    }

    #[test]
    fn validation_rejects_min_delay_greater_than_max_delay() {
        let result = MultiplierRedeliveryBackoff::builder()
            .min_delay(Duration::from_secs(2))
            .max_delay(Duration::from_secs(1))
            .build();

        assert_custom_error(result, MIN_DELAY_MUST_NOT_EXCEED_MAX_DELAY);
    }

    #[test]
    fn validation_rejects_multiplier_less_than_or_equal_to_one() {
        let result = MultiplierRedeliveryBackoff::builder()
            .multiplier(1.0)
            .build();

        assert_custom_error(result, MULTIPLIER_MUST_BE_GREATER_THAN_ONE);
    }

    #[test]
    fn validation_rejects_non_finite_multiplier() {
        let nan_result = MultiplierRedeliveryBackoff::builder()
            .multiplier(f64::NAN)
            .build();
        assert_custom_error(nan_result, MULTIPLIER_MUST_BE_FINITE);

        let infinite_result = MultiplierRedeliveryBackoff::builder()
            .multiplier(f64::INFINITY)
            .build();
        assert_custom_error(infinite_result, MULTIPLIER_MUST_BE_FINITE);
    }

    #[test]
    fn validation_rejects_oversized_duration() {
        let oversized = Duration::from_millis(u64::MAX)
            .checked_add(Duration::from_millis(1))
            .unwrap();
        let result = MultiplierRedeliveryBackoff::builder()
            .min_delay(oversized)
            .max_delay(oversized)
            .build();

        assert_custom_error(result, DELAY_DURATION_TOO_LARGE);
    }

    #[test]
    fn custom_negative_ack_backoff_can_be_implemented_without_engine_dependencies() {
        #[derive(Debug)]
        struct FixedBackoff(Duration);

        impl NegativeAckBackoff for FixedBackoff {
            fn next(&self, _redelivery_count: u32) -> Duration {
                self.0
            }
        }

        let backoff = FixedBackoff(Duration::from_secs(42));

        assert_eq!(backoff.next(123), Duration::from_secs(42));
    }
}
