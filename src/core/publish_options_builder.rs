// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Builder for publish options.

use std::marker::PhantomData;
use std::sync::Arc;

use qubit_retry::RetryPolicy;
use qubit_retry::RetryRule;

use super::publish_options::PublishErrorHandlerFn;
use crate::EventBusError;
use crate::EventEnvelope;
use crate::IntoEventBusResult;
use crate::PublishOptions;

/// Builder used to create [`PublishOptions`].
pub struct PublishOptionsBuilder<T: 'static> {
    retry_options: Option<RetryPolicy>,
    retry_rule: Option<Arc<dyn RetryRule<EventBusError>>>,
    error_handlers: Vec<Arc<PublishErrorHandlerFn<T>>>,
    marker: PhantomData<fn() -> T>,
}

impl<T: 'static> PublishOptionsBuilder<T> {
    /// Creates an empty publish options builder.
    ///
    /// # Returns
    /// Builder with no retry policy and no error handlers.
    pub(crate) fn new() -> Self {
        Self {
            retry_options: None,
            retry_rule: None,
            error_handlers: Vec::new(),
            marker: PhantomData,
        }
    }

    /// Sets an application retry rule evaluated before
    /// [`crate::EventBusRetryRule`]. `UseDefault` delegates to the default
    /// classification. A retry policy must also be configured; rules do not
    /// enable retries by themselves.
    pub fn retry_rule<R>(mut self, rule: R) -> Self
    where
        R: RetryRule<EventBusError>,
    {
        self.retry_rule = Some(Arc::new(rule));
        self
    }

    /// Sets publish retry options. Backoff blocks the publishing thread.
    ///
    /// # Parameters
    /// - `retry_options`: Retry settings to use while publishing.
    ///
    /// # Returns
    /// Updated builder.
    pub fn retry_options(mut self, retry_options: RetryPolicy) -> Self {
        self.retry_options = Some(retry_options);
        self
    }

    /// Adds a publish error handler.
    ///
    /// # Parameters
    /// - `handler`: Callback invoked after publishing fails.
    ///
    /// # Returns
    /// Updated builder.
    pub fn error_handler<F, R>(mut self, handler: F) -> Self
    where
        F: Fn(&EventEnvelope<T>, &EventBusError) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        self.error_handlers.push(Arc::new(move |envelope, error| {
            handler(envelope, error).into_event_bus_result()
        }));
        self
    }

    /// Builds immutable publish options.
    ///
    /// # Returns
    /// Publish options containing the configured handlers and retry settings.
    pub fn build(self) -> PublishOptions<T> {
        PublishOptions {
            retry_options: self.retry_options,
            retry_rule: self.retry_rule,
            error_handlers: self.error_handlers,
        }
    }
}
