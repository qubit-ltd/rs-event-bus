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
use crate::model::PublishReceipt;

/// A failed publish or a receipt that did not satisfy the requested admission
/// condition.
#[derive(Debug, thiserror::Error)]
pub enum CheckedPublishError {
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
