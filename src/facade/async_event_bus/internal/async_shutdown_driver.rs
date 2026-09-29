// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Operations needed to stop and close an asynchronous subscription.

use crate::facade::async_event_bus::Arc;
use crate::facade::async_event_bus::Pin;
use crate::facade::async_event_bus::SubscriptionCloseFailure;
use crate::spi::ShutdownMode;

/// Operations needed by the asynchronous facade to stop and close a
/// subscription.
pub(in crate::facade) trait AsyncShutdownDriver: Send + Sync {
    /// Requests the subscription runner to stop receiving.
    ///
    /// # Parameters
    /// - `mode`: shutdown policy to apply to the runner.
    fn stop(&self, mode: ShutdownMode);

    /// Returns the receiver close failure, if one has been recorded.
    ///
    /// # Returns
    /// `Some` with the canonical close failure, or `None` before a failure.
    fn close_error(&self) -> Option<Arc<SubscriptionCloseFailure>>;

    /// Stores and returns the canonical receiver close failure.
    ///
    /// # Parameters
    /// - `failure`: failure returned by the provider close operation.
    ///
    /// # Returns
    /// The failure retained for subsequent callers.
    fn store_close_error(&self, failure: Arc<SubscriptionCloseFailure>) -> Arc<SubscriptionCloseFailure>;

    /// Closes the provider receiver using the selected mode.
    ///
    /// # Parameters
    /// - `mode`: graceful or immediate receiver shutdown policy.
    ///
    /// # Returns
    /// A future resolving to success or the canonical close failure.
    ///
    /// # Errors
    /// Resolves with the provider receiver close failure, if close fails.
    fn shutdown<'a>(
        &'a self,
        mode: ShutdownMode,
    ) -> Pin<Box<dyn Future<Output = Result<(), Arc<SubscriptionCloseFailure>>> + Send + 'a>>;
}
