// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Counts destination outcomes returned for one publication.

/// Counts the destination outcomes reported in a publication receipt.
///
/// These counts describe admission only, not subscriber handler completion.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::AdmissionSummary;
///
/// let summary = AdmissionSummary { accepted: 2, filtered: 1, rejected: 0 };
/// assert_eq!(summary.accepted + summary.filtered + summary.rejected, 3);
/// ```
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AdmissionSummary {
    /// Number of destinations that accepted the event for dispatch.
    pub accepted: usize,
    /// Number of destinations excluded by subscription filters.
    pub filtered: usize,
    /// Number of destinations that rejected admission.
    pub rejected: usize,
}
