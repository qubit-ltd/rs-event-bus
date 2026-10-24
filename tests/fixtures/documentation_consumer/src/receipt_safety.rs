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

/// Chooses a retry or reconciliation action from the final receipt.
///
/// Uncertain history takes precedence and requires reconciliation by event ID.
/// With certain history, an empty or fully rejected destination snapshot can
/// be republished as a whole; partial acceptance retries only rejected
/// destinations. Accepted, opaque, and interceptor-dropped publications are
/// not automatically republished.
///
/// This function only classifies the receipt. It does not publish, reconcile,
/// or modify the receipt.
///
/// # Parameters
/// - `receipt`: the final receipt, including any uncertainty retained from
///   earlier attempts.
///
/// # Returns
/// The application action selected from the receipt's history and admission
/// outcome.
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
