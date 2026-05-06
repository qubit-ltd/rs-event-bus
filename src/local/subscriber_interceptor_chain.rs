/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Subscriber interceptor chain handle.

use std::sync::Arc;

use crate::{
    EventBusResult,
    EventEnvelope,
};

type HandlerFn<T> = dyn Fn(EventEnvelope<T>) -> EventBusResult<()> + Send + Sync + 'static;

/// Chain handle passed to subscriber interceptors.
///
/// Calling [`proceed`](Self::proceed) invokes the next subscriber interceptor,
/// or the original subscriber handler when the current interceptor is the last
/// one in the chain.
pub struct SubscriberInterceptorChain<T: Clone + Send + Sync + 'static> {
    next: Arc<HandlerFn<T>>,
}

impl<T> SubscriberInterceptorChain<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Creates a chain handle around the next handler.
    ///
    /// # Parameters
    /// - `next`: Handler or interceptor wrapper to invoke next.
    ///
    /// # Returns
    /// Chain handle for one interceptor invocation.
    pub(crate) fn new(
        next: Arc<dyn Fn(EventEnvelope<T>) -> EventBusResult<()> + Send + Sync + 'static>,
    ) -> Self {
        Self { next }
    }

    /// Continues subscriber processing.
    ///
    /// # Parameters
    /// - `envelope`: Envelope to pass to the next chain element.
    ///
    /// # Returns
    /// `Ok(())` when downstream processing succeeds.
    ///
    /// # Errors
    /// Returns the downstream handler or interceptor error.
    pub fn proceed(&self, envelope: EventEnvelope<T>) -> EventBusResult<()> {
        (self.next)(envelope)
    }
}
