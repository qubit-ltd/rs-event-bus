/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Options controlling event publishing.

use std::sync::Arc;

use crate::{EventBusError, EventBusResult, EventEnvelope, PublishOptionsBuilder, RetryOptions};

pub(crate) type PublishErrorHandlerFn<T> =
    dyn Fn(&EventEnvelope<T>, &EventBusError) -> EventBusResult<()> + Send + Sync + 'static;

/// Immutable options applied when publishing events.
pub struct PublishOptions<T: 'static> {
    pub(crate) retry_options: Option<RetryOptions>,
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
    pub const fn retry_options(&self) -> Option<RetryOptions> {
        self.retry_options
    }

    /// Returns the number of registered publish error handlers.
    ///
    /// # Returns
    /// Handler count.
    pub fn error_handler_count(&self) -> usize {
        self.error_handlers.len()
    }

    /// Notifies registered publish error handlers.
    ///
    /// # Parameters
    /// - `envelope`: Envelope that failed to publish.
    /// - `error`: Final publish error.
    pub(crate) fn notify_publish_error(&self, envelope: &EventEnvelope<T>, error: &EventBusError) {
        for handler in &self.error_handlers {
            let _ = handler(envelope, error);
        }
    }
}

impl<T: 'static> Clone for PublishOptions<T> {
    /// Clones retry settings and shared handlers.
    fn clone(&self) -> Self {
        Self {
            retry_options: self.retry_options,
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
