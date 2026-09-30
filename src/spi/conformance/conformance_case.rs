// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One outcome from a conformance check.

use super::ConformanceSkipReason;

/// Result of one named conformance check.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::conformance::ConformanceCase;
///
/// let case = ConformanceCase::Passed { case_id: "publish".into() };
/// assert!(matches!(case, ConformanceCase::Passed { .. }));
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use]
pub enum ConformanceCase {
    /// The check completed successfully.
    Passed {
        /// Stable identifier for this check.
        case_id: String,
    },
    /// The check found a contract violation.
    Failed {
        /// Stable identifier for this check.
        case_id: String,
        /// Description of the observed violation.
        detail: String,
    },
    /// The provider did not supply the optional fixture for this check.
    Skipped {
        /// Stable identifier for this check.
        case_id: String,
        /// Why the check could not be performed.
        reason: ConformanceSkipReason,
    },
}
