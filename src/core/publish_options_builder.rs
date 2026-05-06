/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Builder for publish options.

use std::marker::PhantomData;
use std::sync::Arc;

use crate::{EventBusError, EventEnvelope, IntoEventBusResult, PublishOptions, RetryOptions};

use super::publish_options::PublishErrorHandlerFn;

/// Builder used to create [`PublishOptions`].
pub struct PublishOptionsBuilder<T: 'static> {
    retry_options: Option<RetryOptions>,
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
            error_handlers: Vec::new(),
            marker: PhantomData,
        }
    }

    /// Sets publish retry options.
    ///
    /// # Parameters
    /// - `retry_options`: Retry settings to use while publishing.
    ///
    /// # Returns
    /// Updated builder.
    pub fn retry_options(mut self, retry_options: RetryOptions) -> Self {
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
            error_handlers: self.error_handlers,
        }
    }
}
