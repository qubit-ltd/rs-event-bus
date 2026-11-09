// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Configure portable header limits and classify their boundary failures.

use std::num::NonZeroUsize;

use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::HeaderLimits;
use qubit_event_bus::error::HeaderLimitError;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::model::SubscriptionStopReason;

/// Returns a facade configuration with application-specific header boundaries.
pub fn configured_facade() -> EventBusFacadeConfig {
    let limits = HeaderLimits::new(NonZeroUsize::new(64).unwrap(), NonZeroUsize::new(8_192).unwrap());
    EventBusFacadeConfig::new().with_header_limits(limits)
}

/// Returns the structured header limit error from a rejected publication.
pub fn publish_header_error(error: &PublishError) -> Option<&HeaderLimitError> {
    match error {
        PublishError::Headers(error) => Some(error),
        _ => None,
    }
}

/// Detects a receive stop caused by headers exceeding the configured limits.
pub fn stopped_for_headers(reason: &SubscriptionStopReason) -> bool {
    matches!(reason, SubscriptionStopReason::Headers { .. })
}
