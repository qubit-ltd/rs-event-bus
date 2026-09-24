// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronous single-owner subscription contract.

use std::time::Duration;

use super::DeliveryDisposition;
use super::ReceiveOutcome;
use super::SettlementToken;
use crate::error::SpiError;

/// A backend receiver consumed by exactly one owner.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use qubit_event_bus::spi::EventSubscriptionSpi;
///
/// fn poll_once(receiver: &mut dyn EventSubscriptionSpi) {
///     let outcome = receiver.receive(Duration::ZERO);
///     let _ = outcome;
/// }
/// ```
pub trait EventSubscriptionSpi: Send + 'static {
    /// Receives one message, gap, timeout, or closed outcome.
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError>;

    /// Applies a terminal disposition to a provider-issued settlement token.
    ///
    /// Repeating the same token and disposition must be idempotent and return
    /// the same terminal result. Reusing the token with a different disposition
    /// must return a structured invalid-token error. The token is borrowed so
    /// the facade can retry after an uncertain provider result.
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError>;

    /// Closes this receiver and releases its resources.
    fn close(&mut self) -> Result<(), SpiError>;
}
