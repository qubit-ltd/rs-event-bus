/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Options controlling event subscription.

use std::sync::Arc;

use crate::{
    AckMode, Acknowledgement, EventBusError, EventBusResult, EventEnvelope, RetryOptions,
    SubscribeOptionsBuilder,
};

pub(crate) type EventFilterFn<T> = dyn Fn(&EventEnvelope<T>) -> bool + Send + Sync + 'static;
pub(crate) type SubscribeErrorHandlerFn<T> = dyn Fn(&str, &EventEnvelope<T>, &EventBusError, &Acknowledgement) -> EventBusResult<()>
    + Send
    + Sync
    + 'static;
pub(crate) type DeadLetterStrategyFn<T> = dyn Fn(&str, &EventEnvelope<T>, &EventBusError, &SubscribeOptions<T>) -> Option<EventEnvelope<T>>
    + Send
    + Sync
    + 'static;

/// Immutable options applied to subscriber processing.
pub struct SubscribeOptions<T: 'static> {
    pub(crate) ack_mode: AckMode,
    pub(crate) retry_options: Option<RetryOptions>,
    pub(crate) filter: Option<Arc<EventFilterFn<T>>>,
    pub(crate) error_handlers: Vec<Arc<SubscribeErrorHandlerFn<T>>>,
    pub(crate) dead_letter_strategy: Option<Arc<DeadLetterStrategyFn<T>>>,
    pub(crate) priority: i32,
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
            retry_options: None,
            filter: None,
            error_handlers: Vec::new(),
            dead_letter_strategy: None,
            priority: 0,
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
    pub const fn retry_options(&self) -> Option<RetryOptions> {
        self.retry_options
    }

    /// Returns subscription priority.
    ///
    /// # Returns
    /// Priority value. Higher values are currently informational only.
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

    /// Evaluates the optional event filter.
    ///
    /// # Parameters
    /// - `envelope`: Candidate event.
    ///
    /// # Returns
    /// `true` when the event should be handled.
    pub fn should_handle(&self, envelope: &EventEnvelope<T>) -> bool {
        self.filter.as_ref().is_none_or(|filter| filter(envelope))
    }

    /// Notifies registered subscribe error handlers.
    ///
    /// # Parameters
    /// - `subscriber_id`: Failing subscriber ID.
    /// - `envelope`: Event that failed.
    /// - `error`: Final handler error.
    /// - `acknowledgement`: Acknowledgement handle for the failed event.
    pub(crate) fn notify_subscribe_error(
        &self,
        subscriber_id: &str,
        envelope: &EventEnvelope<T>,
        error: &EventBusError,
        acknowledgement: &Acknowledgement,
    ) {
        for handler in &self.error_handlers {
            let _ = handler(subscriber_id, envelope, error, acknowledgement);
        }
    }

    /// Creates a dead-letter envelope through the configured strategy.
    ///
    /// # Parameters
    /// - `subscriber_id`: Failing subscriber ID.
    /// - `envelope`: Original failed envelope.
    /// - `error`: Final processing error.
    ///
    /// # Returns
    /// Optional dead-letter envelope.
    pub(crate) fn create_dead_letter(
        &self,
        subscriber_id: &str,
        envelope: &EventEnvelope<T>,
        error: &EventBusError,
    ) -> Option<EventEnvelope<T>> {
        self.dead_letter_strategy
            .as_ref()
            .and_then(|strategy| strategy(subscriber_id, envelope, error, self))
    }
}

impl<T: 'static> Clone for SubscribeOptions<T> {
    /// Clones option values and shared callbacks.
    fn clone(&self) -> Self {
        Self {
            ack_mode: self.ack_mode,
            retry_options: self.retry_options,
            filter: self.filter.clone(),
            error_handlers: self.error_handlers.clone(),
            dead_letter_strategy: self.dead_letter_strategy.clone(),
            priority: self.priority,
        }
    }
}

impl<T: 'static> Default for SubscribeOptions<T> {
    /// Creates empty subscription options.
    fn default() -> Self {
        Self::empty()
    }
}
