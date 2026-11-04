// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider-specific conformance checks.

use std::sync::Arc;

/// Optional provider-specific checks for injected failures and cancellation.
///
/// Each populated callback is invoked once by the runner. Missing callbacks
/// become skipped cases, or failures under the strict conformance profile.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use qubit_event_bus::spi::conformance::ConformanceHooks;
///
/// let hooks = ConformanceHooks {
///     settlement: Some(Arc::new(|| Ok(()))),
///     ..ConformanceHooks::default()
/// };
/// assert!(hooks.settlement.is_some());
/// ```
#[derive(Default)]
pub struct ConformanceHooks {
    /// Checks settlement behavior using provider-specific fixtures.
    ///
    /// For providers declaring `AcceptRetryReject`, this fixture should verify
    /// that Retry makes the original delivery available again and Reject
    /// terminates it, including that repeating either disposition adds no
    /// duplicate effect. The runner's structural probes only check returned
    /// results and token errors; a successful return alone does not prove
    /// delivery recovery or permanent termination. Remote providers need a
    /// bounded fixture matching their recovery protocol; a short timeout is
    /// not proof that a rejected message cannot reappear.
    pub settlement: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
    /// Checks cancellation safety using a provider-owned receive fixture.
    pub receive_cancellation: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
    /// Checks that closing with an unsettled durable delivery leaves it
    /// available after reconnecting the same logical subscription.
    pub durable_recovery: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
    /// Checks that closing an ephemeral subscription releases unsettled work.
    pub ephemeral_cleanup: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
    /// Checks cancellation after a settlement operation has taken effect.
    pub settlement_cancellation: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
    /// Checks cancellation safety while closing a provider subscription.
    pub close_cancellation: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
    /// Checks cancellation safety while shutting down the provider.
    pub shutdown_cancellation: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
}
