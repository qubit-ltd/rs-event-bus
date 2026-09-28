// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared terminal directive decisions for subscriber retries.

use qubit_retry::RetryCallbackKind;
use qubit_retry::RetryErrorReason;

use crate::model::FailureDirective;

/// Returns whether a retry rule callback stopped the retry flow by panicking.
///
/// # Parameters
///
/// - `reason`: Structured reason produced when the retry flow terminated.
///
/// # Returns
///
/// `true` only for a failed retry rule callback; observer failures and all
/// other terminal reasons return `false`.
pub(crate) fn is_retry_rule_failure(reason: &RetryErrorReason) -> bool {
    matches!(reason, RetryErrorReason::CallbackFailed { callback }
        if callback.callback() == RetryCallbackKind::Rule)
}

/// Selects the final delivery directive after a retry flow terminates.
///
/// A retry rule panic returns the message to the provider. A requested local
/// retry that has already terminated is discarded; other decisions are
/// preserved for their existing settlement or dead-letter handling.
///
/// # Parameters
///
/// - `reason`: Structured reason the retry flow terminated.
/// - `requested`: Delivery action selected before the retry flow terminated.
///
/// # Returns
///
/// A terminal facade action: `Requeue` for a rule panic, `Discard` for an
/// exhausted or otherwise terminated requested retry, or the original action.
pub(crate) fn terminal_directive(reason: &RetryErrorReason, requested: FailureDirective) -> FailureDirective {
    if is_retry_rule_failure(reason) {
        FailureDirective::Requeue
    } else if requested == FailureDirective::Retry {
        FailureDirective::Discard
    } else {
        requested
    }
}
