// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Options controlling event subscription.
// qubit-style: allow multiple-public-types

use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use qubit_retry::RetryPolicy;

use super::dead_letter_record::DeadLetterPayload;
use crate::AckMode;
use crate::Acknowledgement;
use crate::DeadLetterOriginalPayload;
use crate::DeadLetterRecord;
use crate::EventBusError;
use crate::EventBusResult;
use crate::EventEnvelope;
use crate::EventEnvelopeMetadata;
use crate::SubscribeOptionsBuilder;
use crate::Topic;

pub(crate) type EventFilterFn<T> = dyn Fn(&EventEnvelope<T>) -> bool + Send + Sync + 'static;
pub(crate) type SubscribeErrorHandlerFn<T> =
    dyn Fn(&str, &EventEnvelope<T>, &EventBusError, &Acknowledgement) -> EventBusResult<()> + Send + Sync + 'static;

/// Creates dead-letter envelopes for failed subscriber deliveries.
pub trait DeadLetterStrategy<T: 'static>: Send + Sync + 'static {
    /// Creates a dead-letter envelope for one terminal delivery failure.
    ///
    /// # Parameters
    /// - `subscriber_id`: Failing subscriber ID.
    /// - `failed`: Original failed event envelope.
    /// - `error`: Final processing error.
    /// - `options`: Effective subscription options.
    ///
    /// # Returns
    /// Dead-letter envelope, `None` to discard, or a strategy failure.
    fn create_dead_letter(
        &self,
        subscriber_id: &str,
        failed: &EventEnvelope<T>,
        error: &EventBusError,
        options: &SubscribeOptions<T>,
    ) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>>;
}

/// Closure contract accepted by dead-letter strategy builders.
pub trait DeadLetterStrategyCallback<T: 'static>:
    Fn(
        &str,
        &EventEnvelope<T>,
        &EventBusError,
        &SubscribeOptions<T>,
    ) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>>
    + Send
    + Sync
    + 'static
{
}

impl<T, F> DeadLetterStrategyCallback<T> for F
where
    T: 'static,
    F: Fn(
            &str,
            &EventEnvelope<T>,
            &EventBusError,
            &SubscribeOptions<T>,
        ) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>>
        + Send
        + Sync
        + 'static,
{
}

struct ClosureDeadLetterStrategy<F> {
    callback: F,
}

impl<F> ClosureDeadLetterStrategy<F> {
    fn new(callback: F) -> Self {
        Self { callback }
    }
}

impl<T, F> DeadLetterStrategy<T> for ClosureDeadLetterStrategy<F>
where
    T: 'static,
    F: DeadLetterStrategyCallback<T>,
{
    fn create_dead_letter(
        &self,
        subscriber_id: &str,
        failed: &EventEnvelope<T>,
        error: &EventBusError,
        options: &SubscribeOptions<T>,
    ) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>> {
        (self.callback)(subscriber_id, failed, error, options)
    }
}

pub(crate) type DeadLetterStrategyFn<T> = dyn DeadLetterStrategy<T>;

/// Wraps a closure as a dead-letter strategy object.
pub(crate) fn wrap_dead_letter_strategy<T, F>(strategy: F) -> Arc<DeadLetterStrategyFn<T>>
where
    T: 'static,
    F: DeadLetterStrategyCallback<T>,
{
    Arc::new(ClosureDeadLetterStrategy::new(strategy))
}

/// Creates dead-letter envelopes without knowing the original payload type.
pub trait DeadLetterStrategyAny: Send + Sync + 'static {
    /// Creates a dead-letter envelope for one terminal delivery failure.
    ///
    /// # Parameters
    /// - `subscriber_id`: Failing subscriber ID.
    /// - `failed`: Type-erased metadata for the failed event.
    /// - `original_payload`: Type-erased cloned original payload.
    /// - `error`: Final processing error.
    ///
    /// # Returns
    /// Dead-letter envelope, `None` to discard, or a strategy failure.
    fn create_dead_letter(
        &self,
        subscriber_id: &str,
        failed: EventEnvelopeMetadata,
        original_payload: DeadLetterOriginalPayload,
        error: &EventBusError,
    ) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>>;
}

/// Closure contract accepted by global dead-letter strategy builders.
pub trait DeadLetterStrategyAnyCallback:
    Fn(
        &str,
        EventEnvelopeMetadata,
        DeadLetterOriginalPayload,
        &EventBusError,
    ) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>>
    + Send
    + Sync
    + 'static
{
}

impl<F> DeadLetterStrategyAnyCallback for F where
    F: Fn(
            &str,
            EventEnvelopeMetadata,
            DeadLetterOriginalPayload,
            &EventBusError,
        ) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>>
        + Send
        + Sync
        + 'static
{
}

struct ClosureDeadLetterStrategyAny<F> {
    callback: F,
}

impl<F> ClosureDeadLetterStrategyAny<F> {
    fn new(callback: F) -> Self {
        Self { callback }
    }
}

impl<F> DeadLetterStrategyAny for ClosureDeadLetterStrategyAny<F>
where
    F: DeadLetterStrategyAnyCallback,
{
    fn create_dead_letter(
        &self,
        subscriber_id: &str,
        failed: EventEnvelopeMetadata,
        original_payload: DeadLetterOriginalPayload,
        error: &EventBusError,
    ) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>> {
        (self.callback)(subscriber_id, failed, original_payload, error)
    }
}

pub(crate) type DeadLetterStrategyAnyFn = dyn DeadLetterStrategyAny;

/// Wraps a closure as a type-erased dead-letter strategy object.
pub(crate) fn wrap_dead_letter_strategy_any<F>(strategy: F) -> Arc<DeadLetterStrategyAnyFn>
where
    F: DeadLetterStrategyAnyCallback,
{
    Arc::new(ClosureDeadLetterStrategyAny::new(strategy))
}

/// Creates a strategy that discards failed events.
///
/// # Returns
/// Strategy that always returns `Ok(None)`.
pub fn discard_dead_letters<T>() -> impl DeadLetterStrategyCallback<T>
where
    T: 'static,
{
    |_subscriber_id: &str, _failed: &EventEnvelope<T>, _error: &EventBusError, _options: &SubscribeOptions<T>| Ok(None)
}

/// Creates a strategy that routes standard dead-letter payloads to a topic.
///
/// # Parameters
/// - `dead_letter_topic`: Target topic carrying [`DeadLetterPayload`] records.
///
/// # Returns
/// Strategy that stores a [`DeadLetterRecord`] with diagnostic metadata.
pub fn standard_dead_letters_to<T>(dead_letter_topic: Topic<DeadLetterPayload>) -> impl DeadLetterStrategyCallback<T>
where
    T: Clone + Send + Sync + 'static,
{
    move |subscriber_id: &str, failed: &EventEnvelope<T>, error: &EventBusError, _options: &SubscribeOptions<T>| {
        Ok(Some(
            EventEnvelope::create(
                dead_letter_topic.clone(),
                DeadLetterRecord::from_failure(subscriber_id, failed, error),
            )
            .as_dead_letter(),
        ))
    }
}

/// Creates a strategy that routes standard dead letters to prefixed topics.
///
/// # Parameters
/// - `prefix`: Prefix prepended to the original topic name.
///
/// # Returns
/// Strategy that creates a dead-letter topic from the original topic name.
pub fn prefixed_dead_letters<T>(prefix: &str) -> impl DeadLetterStrategyCallback<T>
where
    T: Clone + Send + Sync + 'static,
{
    let prefix = prefix.to_string();
    move |subscriber_id: &str, failed: &EventEnvelope<T>, error: &EventBusError, _options: &SubscribeOptions<T>| {
        let topic = Topic::<DeadLetterPayload>::try_new(format!("{}{}", prefix, failed.topic().name()))?;
        Ok(Some(
            EventEnvelope::create(topic, DeadLetterRecord::from_failure(subscriber_id, failed, error)).as_dead_letter(),
        ))
    }
}

/// Immutable options applied to subscriber processing.
pub struct SubscribeOptions<T: 'static> {
    pub(crate) ack_mode: AckMode,
    pub(crate) ack_mode_configured: bool,
    pub(crate) retry_options: Option<RetryPolicy>,
    pub(crate) filter: Option<Arc<EventFilterFn<T>>>,
    pub(crate) error_handlers: Vec<Arc<SubscribeErrorHandlerFn<T>>>,
    pub(crate) dead_letter_strategy: Option<Arc<DeadLetterStrategyFn<T>>>,
    pub(crate) priority: i32,
    pub(crate) priority_configured: bool,
}

impl<T: 'static> SubscribeOptions<T> {
    /// Creates a subscription options builder.
    ///
    /// # Returns
    /// Builder with default auto acknowledgement and no retry.
    pub fn builder() -> SubscribeOptionsBuilder<T> {
        SubscribeOptionsBuilder::new()
    }

    /// Creates empty subscription options.
    ///
    /// # Returns
    /// Options with auto acknowledgement and no filter.
    pub fn empty() -> Self {
        Self {
            ack_mode: AckMode::Auto,
            ack_mode_configured: false,
            retry_options: None,
            filter: None,
            error_handlers: Vec::new(),
            dead_letter_strategy: None,
            priority: 0,
            priority_configured: false,
        }
    }

    /// Returns the acknowledgement mode.
    ///
    /// # Returns
    /// Configured acknowledgement mode.
    pub const fn ack_mode(&self) -> AckMode {
        self.ack_mode
    }

    /// Returns configured retry options.
    ///
    /// # Returns
    /// `Some` when subscriber retry is configured.
    pub fn retry_options(&self) -> Option<&RetryPolicy> {
        self.retry_options.as_ref()
    }

    /// Returns subscription priority.
    ///
    /// # Returns
    /// Priority value. Higher values are submitted first by the local backend.
    pub const fn priority(&self) -> i32 {
        self.priority
    }

    /// Returns the number of registered subscribe error handlers.
    ///
    /// # Returns
    /// Handler count.
    pub fn error_handler_count(&self) -> usize {
        self.error_handlers.len()
    }

    /// Merges these explicit options with type-level defaults.
    ///
    /// Explicit scalar settings override defaults only when the builder method
    /// was called. Optional policies prefer explicit values, and error handlers
    /// are cumulative with default handlers running first.
    pub(crate) fn merge_defaults(self, defaults: Self) -> Self {
        let mut error_handlers = defaults.error_handlers;
        error_handlers.extend(self.error_handlers);
        Self {
            ack_mode: if self.ack_mode_configured {
                self.ack_mode
            } else {
                defaults.ack_mode
            },
            ack_mode_configured: self.ack_mode_configured || defaults.ack_mode_configured,
            retry_options: self.retry_options.or(defaults.retry_options),
            filter: self.filter.or(defaults.filter),
            error_handlers,
            dead_letter_strategy: self.dead_letter_strategy.or(defaults.dead_letter_strategy),
            priority: if self.priority_configured {
                self.priority
            } else {
                defaults.priority
            },
            priority_configured: self.priority_configured || defaults.priority_configured,
        }
    }

    /// Returns whether this option set has an explicit dead-letter strategy.
    ///
    /// # Returns
    /// `true` when a subscription-level strategy was configured. A configured
    /// strategy that returns `Ok(None)` intentionally disables fallback to any
    /// factory default strategy.
    pub(crate) fn has_dead_letter_strategy(&self) -> bool {
        self.dead_letter_strategy.is_some()
    }

    /// Evaluates the optional event filter.
    ///
    /// # Parameters
    /// - `envelope`: Candidate event.
    ///
    /// # Returns
    /// `true` when the event should be handled. Returns `false` if the filter
    /// panics, so direct callers do not receive user callback unwinds.
    pub fn should_handle(&self, envelope: &EventEnvelope<T>) -> bool {
        self.try_should_handle(envelope).unwrap_or(false)
    }

    /// Evaluates the optional event filter and preserves callback failures.
    ///
    /// # Parameters
    /// - `envelope`: Candidate event.
    ///
    /// # Returns
    /// `Ok(true)` when the event should be handled, `Ok(false)` when it should
    /// be skipped.
    ///
    /// # Errors
    /// Returns [`EventBusError::HandlerFailed`] when the filter panics.
    pub(crate) fn try_should_handle(&self, envelope: &EventEnvelope<T>) -> EventBusResult<bool> {
        let Some(filter) = &self.filter else {
            return Ok(true);
        };
        panic::catch_unwind(AssertUnwindSafe(|| filter(envelope)))
            .map_err(|_| EventBusError::handler_failed("subscriber filter panicked"))
    }

    /// Notifies registered subscribe error handlers until one handles
    /// acknowledgement.
    ///
    /// A NACK set by the subscriber handler before this method is called does
    /// not by itself short-circuit the error handler chain. The chain stops
    /// only when an error handler records a new terminal acknowledgement
    /// decision, or when an error handler changes the decision to ACK.
    ///
    /// # Parameters
    /// - `subscriber_id`: Failing subscriber ID.
    /// - `envelope`: Event that failed.
    /// - `error`: Final handler error.
    /// - `acknowledgement`: Acknowledgement handle for the failed event.
    /// # Returns
    /// Failures raised by subscribe error handlers.
    pub(crate) fn notify_subscribe_error(
        &self,
        subscriber_id: &str,
        envelope: &EventEnvelope<T>,
        error: &EventBusError,
        acknowledgement: &Acknowledgement,
    ) -> Vec<EventBusError> {
        let mut failures = Vec::new();
        for handler in &self.error_handlers {
            let was_completed = acknowledgement.is_completed();
            let was_acked = acknowledgement.is_acked();
            match panic::catch_unwind(AssertUnwindSafe(|| {
                handler(subscriber_id, envelope, error, acknowledgement)
            })) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(EventBusError::error_handler_failed("subscribe", error.to_string())),
                Err(_) => failures.push(EventBusError::error_handler_failed(
                    "subscribe",
                    "subscribe error handler panicked",
                )),
            }
            if (!was_completed && acknowledgement.is_completed()) || (!was_acked && acknowledgement.is_acked()) {
                break;
            }
        }
        failures
    }

    /// Creates a dead-letter envelope through the configured strategy.
    ///
    /// # Parameters
    /// - `subscriber_id`: Failing subscriber ID.
    /// - `envelope`: Original failed envelope.
    /// - `error`: Final processing error.
    ///
    /// # Returns
    /// Optional dead-letter envelope with a type-erased payload.
    ///
    /// # Errors
    /// Returns [`EventBusError::DeadLetterFailed`] when the strategy fails or
    /// panics.
    pub(crate) fn create_dead_letter(
        &self,
        subscriber_id: &str,
        envelope: &EventEnvelope<T>,
        error: &EventBusError,
    ) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>> {
        let Some(strategy) = &self.dead_letter_strategy else {
            return Ok(None);
        };
        match panic::catch_unwind(AssertUnwindSafe(|| {
            strategy.create_dead_letter(subscriber_id, envelope, error, self)
        })) {
            Ok(Ok(dead_letter)) => Ok(dead_letter),
            Ok(Err(error)) => Err(normalize_dead_letter_error(error)),
            Err(_) => Err(EventBusError::dead_letter_failed("dead-letter strategy panicked")),
        }
    }
}

/// Normalizes a strategy failure into a dead-letter failure.
///
/// # Parameters
/// - `error`: Strategy error.
///
/// # Returns
/// Dead-letter failure preserving existing dead-letter errors.
pub(crate) fn normalize_dead_letter_error(error: EventBusError) -> EventBusError {
    if matches!(error, EventBusError::DeadLetterFailed { .. }) {
        error
    } else {
        EventBusError::dead_letter_failed(error.to_string())
    }
}

impl<T: 'static> Clone for SubscribeOptions<T> {
    /// Clones option values and shared callbacks.
    fn clone(&self) -> Self {
        Self {
            ack_mode: self.ack_mode,
            ack_mode_configured: self.ack_mode_configured,
            retry_options: self.retry_options.clone(),
            filter: self.filter.clone(),
            error_handlers: self.error_handlers.clone(),
            dead_letter_strategy: self.dead_letter_strategy.clone(),
            priority: self.priority,
            priority_configured: self.priority_configured,
        }
    }
}

impl<T: 'static> Default for SubscribeOptions<T> {
    /// Creates empty subscription options.
    fn default() -> Self {
        Self::empty()
    }
}
