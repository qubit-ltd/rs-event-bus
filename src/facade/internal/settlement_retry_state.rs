// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Per-token settlement attempt accounting and pure retry classification.

use std::time::Duration;

use crate::error::SpiError;
use crate::facade::SettlementRetryConfig;
use crate::facade::internal::settlement_retry_decision::SettlementRetryDecision;
use crate::model::SettlementTermination;

/// Finite settlement budget for one token, independent of clocks and providers.
pub(in crate::facade) struct SettlementRetryState {
    /// Validated attempt, elapsed time, and delay limits.
    config: SettlementRetryConfig,
    /// Attempts admitted so far, including the initial provider invocation.
    attempts: u32,
}

impl SettlementRetryState {
    /// Creates an unused budget for a token with the supplied validated policy.
    ///
    /// # Parameters
    /// - `config`: Validated attempt, total elapsed time, and backoff limits.
    ///
    /// # Returns
    /// A fresh per-token state with no admitted settlement attempts.
    #[must_use]
    #[inline]
    pub(in crate::facade) fn new(config: SettlementRetryConfig) -> Self {
        Self {
            config,
            attempts: 0,
        }
    }

    /// Returns the total number of admitted provider settlement attempts.
    ///
    /// # Returns
    /// The attempt count, including the initial attempt and excluding rejected
    /// admissions.
    #[must_use]
    #[inline]
    pub(in crate::facade) fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Admits the next attempt at `elapsed`, or returns the deadline/attempt
    /// limit.
    ///
    /// Rejected attempts leave the counter unchanged; the deadline takes
    /// priority. Call this before every provider settlement invocation,
    /// including the initial attempt and every attempt following a retry
    /// wait.
    ///
    /// # Parameters
    /// - `elapsed`: Total monotonic time since this token's budget started
    ///   immediately before its initial settlement attempt; use the same origin
    ///   for all calls.
    ///
    /// # Returns
    /// `Ok` with the incremented attempt count when the provider invocation is
    /// admitted.
    ///
    /// # Errors
    /// Returns `DeadlineExceeded` when `elapsed` reaches or exceeds
    /// `max_elapsed`. Otherwise returns `AttemptsExhausted` when the total
    /// attempt limit is reached.
    pub(in crate::facade) fn admit_attempt(
        &mut self,
        elapsed: Duration,
    ) -> Result<u32, SettlementTermination> {
        if elapsed >= self.config.max_elapsed() {
            return Err(SettlementTermination::DeadlineExceeded);
        }
        if self.attempts >= self.config.max_attempts().get() {
            return Err(SettlementTermination::AttemptsExhausted);
        }
        self.attempts += 1;
        Ok(self.attempts)
    }

    /// Classifies `error` before checking budgets at `elapsed` and bounding
    /// delay.
    ///
    /// Only explicitly retryable errors may produce a retry decision.
    /// Call this only after `admit_attempt` succeeds and that admitted provider
    /// settlement invocation returns an error. A successful invocation must
    /// disarm the token instead. Infrastructure failures must terminate in
    /// the owner without being sent through this provider-error classifier.
    ///
    /// # Parameters
    /// - `error`: Failure returned by the admitted provider settlement
    ///   invocation.
    /// - `elapsed`: Total monotonic time since this token's budget started
    ///   immediately before its initial settlement attempt, using the same
    ///   origin as `admit_attempt`.
    ///
    /// # Returns
    /// `Stop` for invalid tokens, provider panics, permanent errors, unknown
    /// retryability, exhausted attempts, or an expired deadline, in that
    /// priority order. Otherwise returns `RetryAfter` with saturating
    /// exponential backoff capped by `max_backoff` and the remaining total
    /// elapsed budget. The owner must admit another attempt after waiting;
    /// reaching the deadline never permits another provider invocation.
    pub(in crate::facade) fn after_error(
        &self,
        error: &SpiError,
        elapsed: Duration,
    ) -> SettlementRetryDecision {
        if matches!(error, SpiError::InvalidSettlementToken { .. }) {
            return SettlementRetryDecision::Stop(SettlementTermination::InvalidToken);
        }
        if error.kind() == "provider_panicked" {
            return SettlementRetryDecision::Stop(SettlementTermination::ProviderPanicked);
        }
        match error.retryable() {
            Some(false) => {
                return SettlementRetryDecision::Stop(SettlementTermination::PermanentError);
            }
            None => {
                return SettlementRetryDecision::Stop(SettlementTermination::RetryabilityUnknown);
            }
            Some(true) => {}
        }
        if self.attempts >= self.config.max_attempts().get() {
            return SettlementRetryDecision::Stop(SettlementTermination::AttemptsExhausted);
        }
        if elapsed >= self.config.max_elapsed() {
            return SettlementRetryDecision::Stop(SettlementTermination::DeadlineExceeded);
        }
        let exponent = self.attempts.saturating_sub(1).min(31);
        let delay = self
            .config
            .initial_backoff()
            .saturating_mul(1u32 << exponent)
            .min(self.config.max_backoff());
        let remaining = self.config.max_elapsed().saturating_sub(elapsed);
        SettlementRetryDecision::RetryAfter(delay.min(remaining))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Error;
    use std::num::NonZeroU32;
    use std::time::Duration;

    use crate::error::SpiError;
    use crate::facade::SettlementRetryConfig;
    use crate::facade::internal::settlement_retry_decision::SettlementRetryDecision;
    use crate::facade::internal::settlement_retry_state::SettlementRetryState;
    use crate::model::SettlementTermination;

    /// Builds a valid policy from explicit finite limits without using a clock.
    fn create_test_config(
        max_attempts: u32,
        max_elapsed: Duration,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> SettlementRetryConfig {
        SettlementRetryConfig::new(
            NonZeroU32::new(max_attempts).expect("test attempt limit must be positive"),
            max_elapsed,
            initial_backoff,
            max_backoff,
        )
        .expect("test policy must be valid")
    }

    /// Creates a provider failure with the desired classification evidence.
    fn create_test_error(kind: &'static str, retryable: Option<bool>) -> SpiError {
        SpiError::Operation {
            provider_id: "test-provider".into(),
            operation: "ack",
            resource: None,
            kind,
            retryable,
            source: Box::new(Error::other("settlement failed")),
        }
    }

    /// A permanent error takes precedence even when both budgets are exhausted.
    #[test]
    fn test_permanent_is_not_retried() {
        let mut state = SettlementRetryState::new(SettlementRetryConfig::default());
        state.attempts = state.config.max_attempts().get();
        assert_eq!(
            state.after_error(&create_test_error("permanent", Some(false)), Duration::MAX),
            SettlementRetryDecision::Stop(SettlementTermination::PermanentError),
        );
    }

    /// Unclassified retryability is terminal before evaluating either budget.
    #[test]
    fn test_unknown_is_terminal() {
        let mut state = SettlementRetryState::new(SettlementRetryConfig::default());
        state.attempts = state.config.max_attempts().get();
        assert_eq!(
            state.after_error(&create_test_error("unclassified", None), Duration::MAX),
            SettlementRetryDecision::Stop(SettlementTermination::RetryabilityUnknown),
        );
    }

    /// The first provider call consumes one attempt and rejected calls consume
    /// none.
    #[test]
    fn test_attempt_limit_includes_first() {
        let config = create_test_config(
            2,
            Duration::from_secs(5),
            Duration::from_millis(10),
            Duration::from_secs(1),
        );
        let mut state = SettlementRetryState::new(config);
        assert_eq!(state.attempts(), 0);
        assert_eq!(state.admit_attempt(Duration::ZERO), Ok(1));
        assert_eq!(
            state.after_error(&create_test_error("transient", Some(true)), Duration::ZERO),
            SettlementRetryDecision::RetryAfter(Duration::from_millis(10))
        );
        assert_eq!(state.admit_attempt(Duration::from_millis(10)), Ok(2));
        assert_eq!(
            state.after_error(&create_test_error("transient", Some(true)), Duration::ZERO),
            SettlementRetryDecision::Stop(SettlementTermination::AttemptsExhausted)
        );
        assert_eq!(
            state.admit_attempt(Duration::from_millis(20)),
            Err(SettlementTermination::AttemptsExhausted)
        );
        assert_eq!(state.attempts(), 2);
    }

    /// A one-attempt policy cannot retry the initial failure.
    #[test]
    fn test_single_attempt_policy() {
        let config = create_test_config(
            1,
            Duration::from_secs(5),
            Duration::from_millis(10),
            Duration::from_secs(1),
        );
        let mut state = SettlementRetryState::new(config);
        assert_eq!(state.admit_attempt(Duration::ZERO), Ok(1));
        assert_eq!(
            state.after_error(&create_test_error("transient", Some(true)), Duration::ZERO),
            SettlementRetryDecision::Stop(SettlementTermination::AttemptsExhausted)
        );
    }

    /// Clipping a wait to the deadline never admits a call at that deadline.
    #[test]
    fn test_deadline_disallows_another_attempt() {
        let config = create_test_config(
            3,
            Duration::from_secs(1),
            Duration::from_millis(100),
            Duration::from_secs(1),
        );
        let mut state = SettlementRetryState::new(config);
        assert_eq!(state.admit_attempt(Duration::ZERO), Ok(1));
        assert_eq!(
            state.after_error(
                &create_test_error("transient", Some(true)),
                Duration::from_millis(950)
            ),
            SettlementRetryDecision::RetryAfter(Duration::from_millis(50))
        );
        assert_eq!(
            state.admit_attempt(Duration::from_secs(1)),
            Err(SettlementTermination::DeadlineExceeded)
        );
        assert_eq!(
            state.admit_attempt(Duration::MAX),
            Err(SettlementTermination::DeadlineExceeded)
        );
        assert_eq!(state.attempts(), 1);
        assert_eq!(
            state.after_error(
                &create_test_error("transient", Some(true)),
                Duration::from_secs(1)
            ),
            SettlementRetryDecision::Stop(SettlementTermination::DeadlineExceeded)
        );
    }

    /// Admission rejects an already expired first attempt without incrementing.
    #[test]
    fn test_deadline_disallows_first_attempt() {
        let mut state = SettlementRetryState::new(SettlementRetryConfig::default());
        assert_eq!(
            state.admit_attempt(state.config.max_elapsed()),
            Err(SettlementTermination::DeadlineExceeded)
        );
        assert_eq!(state.attempts(), 0);
    }

    /// Budget priority differs intentionally between admission and error
    /// handling.
    #[test]
    fn test_budget_termination_priority() {
        let mut state = SettlementRetryState::new(SettlementRetryConfig::default());
        state.attempts = state.config.max_attempts().get();
        assert_eq!(
            state.admit_attempt(Duration::MAX),
            Err(SettlementTermination::DeadlineExceeded)
        );
        assert_eq!(
            state.after_error(&create_test_error("transient", Some(true)), Duration::MAX),
            SettlementRetryDecision::Stop(SettlementTermination::AttemptsExhausted)
        );
    }

    /// Delays grow exponentially and stop at the configured backoff cap.
    #[test]
    fn test_backoff_growth_and_cap() {
        let config = create_test_config(
            5,
            Duration::from_secs(1),
            Duration::from_millis(10),
            Duration::from_millis(25),
        );
        let mut state = SettlementRetryState::new(config);
        for expected_delay in [10, 20, 25, 25] {
            state
                .admit_attempt(Duration::ZERO)
                .expect("attempt must fit test budget");
            assert_eq!(
                state.after_error(&create_test_error("transient", Some(true)), Duration::ZERO),
                SettlementRetryDecision::RetryAfter(Duration::from_millis(expected_delay))
            );
        }
    }

    /// The exponent caps at 31 and multiplication saturates for extreme
    /// durations.
    #[test]
    fn test_backoff_is_saturating() {
        let config = create_test_config(
            u32::MAX,
            Duration::MAX,
            Duration::from_nanos(1),
            Duration::MAX,
        );
        let mut state = SettlementRetryState::new(config);
        for attempts in [32, 33, u32::MAX - 1] {
            state.attempts = attempts;
            assert_eq!(
                state.after_error(&create_test_error("transient", Some(true)), Duration::ZERO),
                SettlementRetryDecision::RetryAfter(Duration::from_nanos(1u64 << 31))
            );
        }
        let config = create_test_config(u32::MAX, Duration::MAX, Duration::MAX, Duration::MAX);
        let mut state = SettlementRetryState::new(config);
        state.attempts = 32;
        assert_eq!(
            state.after_error(&create_test_error("transient", Some(true)), Duration::ZERO),
            SettlementRetryDecision::RetryAfter(Duration::MAX)
        );
    }

    /// At the largest valid counter admission rejects before an overflowing
    /// add.
    #[test]
    fn test_attempt_counter_does_not_overflow() {
        let config = create_test_config(
            u32::MAX,
            Duration::MAX,
            Duration::from_nanos(1),
            Duration::MAX,
        );
        let mut state = SettlementRetryState::new(config);
        state.attempts = u32::MAX - 1;
        assert_eq!(state.admit_attempt(Duration::ZERO), Ok(u32::MAX));
        assert_eq!(
            state.admit_attempt(Duration::ZERO),
            Err(SettlementTermination::AttemptsExhausted)
        );
        assert_eq!(state.attempts(), u32::MAX);
    }

    /// Panic and invalid token evidence override retryability and expired
    /// budgets.
    #[test]
    fn test_provider_panic_and_invalid_token_are_terminal() {
        let mut state = SettlementRetryState::new(SettlementRetryConfig::default());
        state.attempts = state.config.max_attempts().get();
        for retryable in [Some(true), Some(false), None] {
            assert_eq!(
                state.after_error(
                    &create_test_error("provider_panicked", retryable),
                    Duration::MAX
                ),
                SettlementRetryDecision::Stop(SettlementTermination::ProviderPanicked)
            );
            let error = SpiError::InvalidSettlementToken {
                provider_id: "test-provider".into(),
                operation: "ack",
                resource: None,
                reason: "foreign_owner",
                retryable,
                source: Box::new(Error::other("invalid token")),
            };
            assert_eq!(
                state.after_error(&error, Duration::MAX),
                SettlementRetryDecision::Stop(SettlementTermination::InvalidToken)
            );
        }
    }
}
