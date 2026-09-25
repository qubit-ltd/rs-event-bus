// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Conditions a caller can require from reported publication admission.

/// The admission condition a caller requires before treating a receipt as
/// usable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionRequirement {
    /// Require at least one destination to accept the event.
    AtLeastOneAccepted,
    /// Require at least one acceptance and no destination rejections.
    AtLeastOneAcceptedAndNoRejected,
}
