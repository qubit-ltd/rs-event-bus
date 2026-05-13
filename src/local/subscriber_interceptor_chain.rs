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
// qubit-style: allow multiple-public-types

use std::panic::{
    self,
    AssertUnwindSafe,
};
use std::sync::{
    Arc,
    Mutex,
};

use crate::{
    EventBusError,
    EventBusResult,
    EventEnvelope,
};

type HandlerFn<T> = dyn Fn(EventEnvelope<T>) -> EventBusResult<()> + Send + Sync + 'static;
pub(crate) type DownstreamErrorSlot = Arc<Mutex<Vec<EventBusError>>>;

/// Chain handle passed to subscriber interceptors.
///
/// Calling [`proceed`](Self::proceed) invokes the next subscriber interceptor,
/// or the original subscriber handler when the current interceptor is the last
/// one in the chain.
pub struct SubscriberInterceptorChain<T: Clone + Send + Sync + 'static> {
    next: Arc<HandlerFn<T>>,
    downstream_error: DownstreamErrorSlot,
}

/// Chain handle passed to global subscriber interceptors.
///
/// Calling [`proceed`](Self::proceed) invokes the next global interceptor,
/// typed interceptor, or original subscriber handler.
pub struct SubscriberInterceptorAnyChain {
    next: Arc<dyn Fn() -> EventBusResult<()> + Send + Sync + 'static>,
    downstream_error: DownstreamErrorSlot,
}

impl SubscriberInterceptorAnyChain {
    /// Creates a chain handle sharing downstream error state with the wrapper.
    ///
    /// # Parameters
    /// - `next`: Handler or interceptor wrapper to invoke next.
    /// - `downstream_error`: Shared slot recording downstream failures.
    ///
    /// # Returns
    /// Chain handle for one global interceptor invocation.
    pub(crate) fn with_downstream_error(
        next: Arc<dyn Fn() -> EventBusResult<()> + Send + Sync + 'static>,
        downstream_error: DownstreamErrorSlot,
    ) -> Self {
        Self {
            next,
            downstream_error,
        }
    }

    /// Continues subscriber processing.
    ///
    /// # Returns
    /// `Ok(())` when downstream processing succeeds.
    ///
    /// # Errors
    /// Returns the downstream handler or interceptor error. If the downstream
    /// handler panics, the panic is isolated and returned as
    /// [`EventBusError::HandlerPanicked`].
    pub fn proceed(&self) -> EventBusResult<()> {
        match panic::catch_unwind(AssertUnwindSafe(|| (self.next)())) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                record_downstream_error(&self.downstream_error, &error);
                Err(error)
            }
            Err(_) => {
                let error = EventBusError::handler_panicked();
                record_downstream_error(&self.downstream_error, &error);
                Err(error)
            }
        }
    }
}

impl<T> SubscriberInterceptorChain<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Creates a chain handle sharing downstream error state with the wrapper.
    ///
    /// # Parameters
    /// - `next`: Handler or interceptor wrapper to invoke next.
    /// - `downstream_error`: Shared slot recording downstream failures.
    ///
    /// # Returns
    /// Chain handle for one interceptor invocation.
    pub(crate) fn with_downstream_error(
        next: Arc<dyn Fn(EventEnvelope<T>) -> EventBusResult<()> + Send + Sync + 'static>,
        downstream_error: DownstreamErrorSlot,
    ) -> Self {
        Self {
            next,
            downstream_error,
        }
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
    /// Returns the downstream handler or interceptor error. If the downstream
    /// handler panics, the panic is isolated and returned as
    /// [`EventBusError::HandlerPanicked`].
    pub fn proceed(&self, envelope: EventEnvelope<T>) -> EventBusResult<()> {
        match panic::catch_unwind(AssertUnwindSafe(|| (self.next)(envelope))) {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => {
                record_downstream_error(&self.downstream_error, &error);
                Err(error)
            }
            Err(_) => {
                let error = EventBusError::handler_panicked();
                record_downstream_error(&self.downstream_error, &error);
                Err(error)
            }
        }
    }
}

/// Creates shared state for one interceptor invocation.
///
/// # Returns
/// Empty downstream error slot.
pub(crate) fn create_downstream_error_slot() -> DownstreamErrorSlot {
    Arc::new(Mutex::new(Vec::new()))
}

/// Returns whether an error came from the downstream chain.
///
/// # Parameters
/// - `downstream_error`: Slot filled by [`SubscriberInterceptorChain::proceed`]
///   or [`SubscriberInterceptorAnyChain::proceed`].
/// - `error`: Error returned by an interceptor.
///
/// # Returns
/// `true` when `error` matches a recorded downstream failure.
pub(crate) fn is_recorded_downstream_error(
    downstream_error: &DownstreamErrorSlot,
    error: &EventBusError,
) -> bool {
    downstream_error
        .lock()
        .map(|recorded| recorded.contains(error))
        .unwrap_or(false)
}

/// Records a downstream failure observed by a chain handle.
///
/// # Parameters
/// - `downstream_error`: Shared error slot.
/// - `error`: Downstream failure to record.
fn record_downstream_error(downstream_error: &DownstreamErrorSlot, error: &EventBusError) {
    if let Ok(mut recorded) = downstream_error.lock()
        && !recorded.contains(error)
    {
        recorded.push(error.clone());
    }
}
