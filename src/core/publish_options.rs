// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Options controlling event publishing.

use std::panic::{
    self,
    AssertUnwindSafe,
};
use std::sync::Arc;

use qubit_retry::RetryPolicy;

use crate::{
    EventBusError,
    EventBusResult,
    EventEnvelope,
    PublishOptionsBuilder,
};

pub(crate) type PublishErrorHandlerFn<T> = dyn Fn(&EventEnvelope<T>, &EventBusError) -> EventBusResult<()>
    + Send
    + Sync
    + 'static;

/// Immutable options applied when publishing events.
pub struct PublishOptions<T: 'static> {
    pub(crate) retry_options: Option<RetryPolicy>,
    pub(crate) error_handlers: Vec<Arc<PublishErrorHandlerFn<T>>>,
}

impl<T: 'static> PublishOptions<T> {
    /// Creates a publish options builder.
    ///
    /// # Returns
    /// Builder with no retry policy and no error handlers.
    pub fn builder() -> PublishOptionsBuilder<T> {
        PublishOptionsBuilder::new()
    }

    /// Creates empty publish options.
    ///
    /// # Returns
    /// Options with default behavior.
    pub fn empty() -> Self {
        Self {
            retry_options: None,
            error_handlers: Vec::new(),
        }
    }

    /// Returns configured retry options.
    ///
    /// # Returns
    /// `Some` when publish retry is configured.
    pub fn retry_options(&self) -> Option<&RetryPolicy> {
        self.retry_options.as_ref()
    }

    /// Returns the number of registered publish error handlers.
    ///
    /// # Returns
    /// Handler count.
    pub fn error_handler_count(&self) -> usize {
        self.error_handlers.len()
    }

    /// Merges these explicit options with type-level defaults.
    ///
    /// Explicit retry options override defaults. Error handlers are cumulative
    /// and default handlers run before explicit handlers.
    pub(crate) fn merge_defaults(self, defaults: Self) -> Self {
        let mut error_handlers = defaults.error_handlers;
        error_handlers.extend(self.error_handlers);
        Self {
            retry_options: self.retry_options.or(defaults.retry_options),
            error_handlers,
        }
    }

    /// Notifies registered publish error handlers.
    ///
    /// # Parameters
    /// - `envelope`: Envelope that failed to publish.
    /// - `error`: Final publish error.
    /// # Returns
    /// Failures raised by publish error handlers.
    pub(crate) fn notify_publish_error(
        &self,
        envelope: &EventEnvelope<T>,
        error: &EventBusError,
    ) -> Vec<EventBusError> {
        let mut failures = Vec::new();
        for handler in &self.error_handlers {
            match panic::catch_unwind(AssertUnwindSafe(|| {
                handler(envelope, error)
            })) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    failures.push(EventBusError::error_handler_failed(
                        "publish",
                        error.to_string(),
                    ))
                }
                Err(_) => failures.push(EventBusError::error_handler_failed(
                    "publish",
                    "publish error handler panicked",
                )),
            }
        }
        failures
    }
}

impl<T: 'static> Clone for PublishOptions<T> {
    /// Clones retry settings and shared handlers.
    fn clone(&self) -> Self {
        Self {
            retry_options: self.retry_options.clone(),
            error_handlers: self.error_handlers.clone(),
        }
    }
}

impl<T: 'static> Default for PublishOptions<T> {
    /// Creates empty publish options.
    fn default() -> Self {
        Self::empty()
    }
}
