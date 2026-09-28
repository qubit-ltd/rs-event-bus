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
#[derive(Default)]
pub struct ConformanceHooks {
    /// Checks repeat-settlement idempotence and conflicting dispositions.
    pub settlement: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
    /// Checks cancellation safety using a provider-owned receive fixture.
    pub receive_cancellation: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
    /// Checks that closing with an unsettled durable delivery leaves it
    /// available after reconnecting the same logical subscription.
    pub durable_recovery: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
    /// Checks cancellation after a settlement operation has taken effect.
    pub settlement_cancellation: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
    /// Checks cancellation safety while closing a provider subscription.
    pub close_cancellation: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
    /// Checks cancellation safety while shutting down the provider.
    pub shutdown_cancellation: Option<Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
}
