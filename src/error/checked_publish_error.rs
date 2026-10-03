// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Errors returned when publication is required to meet an admission condition.

use crate::error::PublishFailure;
use crate::model::AdmissionCheckError;
use crate::model::EventId;
use crate::model::ProviderId;
use crate::model::PublishReceipt;

/// A publication failure, unsupported admission visibility, or a receipt that
/// did not satisfy the requested admission condition.
#[derive(Debug, thiserror::Error)]
pub enum CheckedPublishError {
    /// Per-destination admission was requested from a provider that cannot
    /// report it; publication was not attempted.
    #[error(
        "provider {provider_id:?} cannot report destination admission for event {event_id:?}; publication was not attempted"
    )]
    UnsupportedVisibility {
        /// Original event identifier before publisher interception.
        event_id: EventId,
        /// Provider whose publication visibility is opaque.
        provider_id: ProviderId,
    },
    /// Publication itself failed before a receipt was produced.
    #[error(transparent)]
    Publish(#[from] PublishFailure),
    /// Publication completed, but its receipt did not meet the requirement.
    #[error("publication did not meet admission requirement: {reason}")]
    Admission {
        /// Complete receipt, retained to expose partial acceptance and
        /// duplicate risk.
        receipt: Box<PublishReceipt>,
        /// Failed admission check.
        #[source]
        reason: AdmissionCheckError,
    },
}
