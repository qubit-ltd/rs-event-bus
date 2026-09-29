// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Closes a provider subscription at the receiver ownership boundary.

use std::panic::AssertUnwindSafe;

use crate::error::SpiError;
use crate::facade::event_bus::EventBusInner;
use crate::facade::event_bus::failure::panic_message;
use crate::model::SubscriberId;
use crate::spi::EventSubscriptionSpi;

/// Closes one provider receiver and converts a panic into an SPI error.
///
/// # Parameters
/// - `inner`: shared provider identity used to construct an error.
/// - `subscriber_id`: logical subscription identity for error context.
/// - `receiver`: single-owner provider receiver to close.
///
/// # Returns
/// Success when the provider closes the receiver.
///
/// # Errors
/// Returns the provider's close failure or a terminal error when close panics.
pub(in crate::facade) fn close_spi_subscription(
    inner: &EventBusInner,
    subscriber_id: &SubscriberId,
    receiver: &mut dyn EventSubscriptionSpi,
) -> Result<(), SpiError> {
    std::panic::catch_unwind(AssertUnwindSafe(|| receiver.close())).unwrap_or_else(|payload| {
        Err(SpiError::Operation {
            provider_id: inner.provider_id.as_str().into(),
            operation: "close_subscription",
            resource: Some(subscriber_id.as_str().into()),
            kind: "provider_panicked",
            retryable: Some(false),
            source: Box::new(std::io::Error::other(panic_message(payload.as_ref()))),
        })
    })
}
