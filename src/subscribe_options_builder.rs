/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Builder for subscribe options.

use std::marker::PhantomData;
use std::sync::Arc;

use crate::subscribe_options::{DeadLetterStrategyFn, EventFilterFn, SubscribeErrorHandlerFn};
use crate::{
    AckMode, Acknowledgement, EventBusError, EventEnvelope, IntoEventBusResult, RetryOptions,
    SubscribeOptions,
};

/// Builder used to create [`SubscribeOptions`].
pub struct SubscribeOptionsBuilder<T: 'static> {
    ack_mode: AckMode,
    retry_options: Option<RetryOptions>,
    filter: Option<Arc<EventFilterFn<T>>>,
    error_handlers: Vec<Arc<SubscribeErrorHandlerFn<T>>>,
    dead_letter_strategy: Option<Arc<DeadLetterStrategyFn<T>>>,
    priority: i32,
    marker: PhantomData<fn() -> T>,
}

impl<T: 'static> SubscribeOptionsBuilder<T> {
    /// Creates an empty subscribe options builder.
    ///
    /// # Returns
    /// Builder using automatic acknowledgement.
    pub(crate) fn new() -> Self {
        Self {
            ack_mode: AckMode::Auto,
            retry_options: None,
            filter: None,
            error_handlers: Vec::new(),
            dead_letter_strategy: None,
            priority: 0,
            marker: PhantomData,
        }
    }

    /// Sets acknowledgement mode.
    ///
    /// # Parameters
    /// - `ack_mode`: Automatic or manual acknowledgement mode.
    ///
    /// # Returns
    /// Updated builder.
    pub fn ack_mode(mut self, ack_mode: AckMode) -> Self {
        self.ack_mode = ack_mode;
        self
    }

    /// Sets subscriber retry options.
    ///
    /// # Parameters
    /// - `retry_options`: Retry settings for handler failures.
    ///
    /// # Returns
    /// Updated builder.
    pub fn retry_options(mut self, retry_options: RetryOptions) -> Self {
        self.retry_options = Some(retry_options);
        self
    }

    /// Sets the subscriber filter.
    ///
    /// # Parameters
    /// - `filter`: Predicate evaluated before event handling.
    ///
    /// # Returns
    /// Updated builder.
    pub fn filter<F>(mut self, filter: F) -> Self
    where
        F: Fn(&EventEnvelope<T>) -> bool + Send + Sync + 'static,
    {
        self.filter = Some(Arc::new(filter));
        self
    }

    /// Adds a subscribe error handler.
    ///
    /// # Parameters
    /// - `handler`: Callback invoked after retry attempts are exhausted.
    ///
    /// # Returns
    /// Updated builder.
    pub fn error_handler<F, R>(mut self, handler: F) -> Self
    where
        F: Fn(&str, &EventEnvelope<T>, &EventBusError, &Acknowledgement) -> R
            + Send
            + Sync
            + 'static,
        R: IntoEventBusResult + 'static,
    {
        self.error_handlers.push(Arc::new(
            move |subscriber_id, envelope, error, acknowledgement| {
                handler(subscriber_id, envelope, error, acknowledgement).into_event_bus_result()
            },
        ));
        self
    }

    /// Sets the dead-letter strategy.
    ///
    /// # Parameters
    /// - `strategy`: Callback that can create a dead-letter envelope.
    ///
    /// # Returns
    /// Updated builder.
    pub fn dead_letter_strategy<F>(mut self, strategy: F) -> Self
    where
        F: Fn(
                &str,
                &EventEnvelope<T>,
                &EventBusError,
                &SubscribeOptions<T>,
            ) -> Option<EventEnvelope<T>>
            + Send
            + Sync
            + 'static,
    {
        self.dead_letter_strategy = Some(Arc::new(strategy));
        self
    }

    /// Sets subscriber priority.
    ///
    /// # Parameters
    /// - `priority`: Informational priority value.
    ///
    /// # Returns
    /// Updated builder.
    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Builds immutable subscribe options.
    ///
    /// # Returns
    /// Subscribe options containing configured callbacks and retry settings.
    pub fn build(self) -> SubscribeOptions<T> {
        SubscribeOptions {
            ack_mode: self.ack_mode,
            retry_options: self.retry_options,
            filter: self.filter,
            error_handlers: self.error_handlers,
            dead_letter_strategy: self.dead_letter_strategy,
            priority: self.priority,
        }
    }
}
