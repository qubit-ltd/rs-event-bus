// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Check retained history before the final attempt's admission summary.

use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::PublishReceipt;

use crate::republish_action::RepublishAction;

/// Chooses an application action without publishing or altering the receipt.
#[must_use]
#[inline]
pub fn republish_action(receipt: &PublishReceipt) -> RepublishAction {
    if receipt.duplicate_possible() {
        return RepublishAction::ReconcileByEventId;
    }
    match receipt.admission_outcome() {
        AdmissionOutcome::NoDestinations | AdmissionOutcome::NoneAccepted(_) => {
            RepublishAction::RepublishWhole
        }
        AdmissionOutcome::PartiallyAccepted(_) => RepublishAction::RetryRejectedDestinations,
        _ => RepublishAction::NoRepublish,
    }
}
