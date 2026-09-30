// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal contract for stopping and closing an asynchronous subscription.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::SubscriptionCloseFailure;
use crate::spi::ShutdownMode;

/// Operations needed to stop and close an asynchronous subscription.
pub(in crate::facade) trait AsyncShutdownDriver: Send + Sync {
    /// Requests the subscription runner to stop receiving.
    ///
    /// # Parameters
    /// - `mode`: shutdown policy to apply to the runner.
    fn stop(&self, mode: ShutdownMode);

    /// Returns the canonical receiver close failure, if one has been recorded.
    ///
    /// # Returns
    /// Some with the retained failure, or None before a failure is recorded.
    #[must_use = "Use the returned query result."]
    fn close_error(&self) -> Option<Arc<SubscriptionCloseFailure>>;

    /// Stores and returns the canonical receiver close failure.
    ///
    /// # Parameters
    /// - `failure`: provider close failure to retain.
    ///
    /// # Returns
    /// The failure retained for subsequent callers.
    fn store_close_error(&self, failure: Arc<SubscriptionCloseFailure>) -> Arc<SubscriptionCloseFailure>;

    /// Closes the provider receiver using the selected shutdown policy.
    ///
    /// # Parameters
    /// - `mode`: graceful or immediate receiver shutdown policy.
    ///
    /// # Returns
    /// A future resolving to success or the canonical close failure.
    ///
    /// # Errors
    /// Resolves with the provider receiver close failure if close fails.
    fn shutdown<'a>(
        &'a self,
        mode: ShutdownMode,
    ) -> Pin<Box<dyn Future<Output = Result<(), Arc<SubscriptionCloseFailure>>> + Send + 'a>>;
}
