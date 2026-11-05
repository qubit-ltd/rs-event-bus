// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Check retained history before the final attempt's admission summary.

use qubit_event_bus::model::AdmissionCheckError;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::PublishReceipt;

use crate::republish_action::RepublishAction;

/// Checks whether at least one destination accepted the event and none rejected
/// it.
///
/// This only reads the receipt. It does not republish, compensate, or retry
/// selected destinations, and it leaves the receipt available to the caller.
///
/// # Examples
///
/// ```
/// use event_bus_documentation_consumer::receipt_safety::check_order_admission;
/// use qubit_event_bus::model::PublishReceipt;
///
/// fn inspect(receipt: &PublishReceipt) -> Result<(), Box<dyn std::error::Error>> {
///     check_order_admission(receipt)?;
///     let _event_id = receipt.input_event_id();
///     Ok(())
/// }
/// ```
pub fn check_order_admission(receipt: &PublishReceipt) -> Result<(), AdmissionCheckError> {
    receipt.check_admission(AdmissionRequirement::AtLeastOneAcceptedAndNoRejected)
}

/// Reviews retained history before applying the strict admission check.
///
/// Uncertain history returns the complete receipt with a reconciliation action
/// and does not run the strict check. For certain history, a failed check
/// returns the complete receipt, the error, and the application action.
pub fn inspect_order_receipt(
    receipt: PublishReceipt,
) -> Result<PublishReceipt, (PublishReceipt, Option<AdmissionCheckError>, RepublishAction)> {
    if receipt.duplicate_possible() {
        return Err((receipt, None, RepublishAction::ReconcileByEventId));
    }
    match check_order_admission(&receipt) {
        Ok(()) => Ok(receipt),
        Err(error) => {
            let action = republish_action(&receipt);
            Err((receipt, Some(error), action))
        }
    }
}

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
        AdmissionOutcome::NoDestinations | AdmissionOutcome::NoneAccepted(_) => RepublishAction::RepublishWhole,
        AdmissionOutcome::PartiallyAccepted(_) => RepublishAction::RetryRejectedDestinations,
        _ => RepublishAction::NoRepublish,
    }
}
