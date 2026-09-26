// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! One outcome from a conformance check.

/// Result of one named conformance check.
#[derive(Clone, Debug, Eq, PartialEq)]
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
        reason: String,
    },
}
