// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Policy governing retries after uncertain provider admission.

/// Controls whether automatic retries may introduce duplicate events.
///
/// `AllowDuplicates` permits the existing retry policy and rules to decide;
/// it does not itself enable retries or force another attempt.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::DuplicateRiskPolicy;
/// use qubit_event_bus::model::PublishOptions;
///
/// assert_eq!(PublishOptions::<String>::default().duplicate_risk_policy(),
///     DuplicateRiskPolicy::Forbid);
/// let options = PublishOptions::<String>::builder()
///     .duplicate_risk_policy(DuplicateRiskPolicy::AllowDuplicates)
///     .build();
/// assert_eq!(options.duplicate_risk_policy(), DuplicateRiskPolicy::AllowDuplicates);
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DuplicateRiskPolicy {
    /// Stop automatic retries when a failed attempt may have been accepted.
    #[default]
    Forbid,
    /// Permit the configured retry rules to retry uncertain admission.
    AllowDuplicates,
}
