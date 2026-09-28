// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Nonblocking provider-specific checks for asynchronous conformance runs.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// A provider-specific async conformance check.
pub type AsyncConformanceCheck =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>> + Send + Sync>;

/// Optional provider-specific checks that require asynchronous fixtures.
#[derive(Default)]
pub struct AsyncConformanceHooks {
    /// Checks repeat-settlement idempotence and conflicting dispositions.
    pub settlement: Option<AsyncConformanceCheck>,
    /// Checks cancellation safety using a provider-owned receive fixture.
    pub receive_cancellation: Option<AsyncConformanceCheck>,
    /// Checks that closing with an unsettled durable delivery leaves it
    /// available after reconnecting the same logical subscription.
    pub durable_recovery: Option<AsyncConformanceCheck>,
    /// Checks cancellation after a settlement operation has taken effect.
    pub settlement_cancellation: Option<AsyncConformanceCheck>,
    /// Checks cancellation safety while closing a provider subscription.
    pub close_cancellation: Option<AsyncConformanceCheck>,
    /// Checks cancellation safety while shutting down the provider.
    pub shutdown_cancellation: Option<AsyncConformanceCheck>,
}
