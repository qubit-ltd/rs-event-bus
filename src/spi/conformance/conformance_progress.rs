// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Progress snapshots emitted immediately before conformance operations.

use super::conformance_case_context::ConformanceCaseContext;

/// Identifies the operation a conformance runner is about to perform.
///
/// The runner supplies these snapshots to its observer callback immediately
/// before starting the associated operation.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::conformance::ConformanceProgress;
///
/// fn report_progress(progress: &ConformanceProgress) {
///     eprintln!("{}: {}", progress.case_id(), progress.stage());
/// }
///
/// let _observer: &dyn Fn(&ConformanceProgress) = &report_progress;
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConformanceProgress {
    /// Stable identifier for the pending conformance case.
    case_id: &'static str,
    /// Payload and durability information known for the case, if established.
    context: Option<ConformanceCaseContext>,
    /// Name of the operation about to start.
    stage: &'static str,
}

impl ConformanceProgress {
    /// Creates a snapshot emitted immediately before a conformance operation.
    ///
    /// # Parameters
    /// - `case_id`: stable identifier for the pending case.
    /// - `context`: payload and durability information, when available.
    /// - `stage`: stable name of the operation about to start.
    pub(super) fn new(case_id: &'static str, context: Option<ConformanceCaseContext>, stage: &'static str) -> Self {
        Self {
            case_id,
            context,
            stage,
        }
    }

    /// Stable case identifier associated with the pending operation.
    ///
    /// # Returns
    /// The static case identifier.
    pub fn case_id(&self) -> &'static str {
        self.case_id
    }

    /// Payload and durability context, when the runner has established it.
    ///
    /// # Returns
    /// The context borrowed from this snapshot, or `None` before it is known.
    pub fn context(&self) -> Option<&ConformanceCaseContext> {
        self.context.as_ref()
    }

    /// Stable operation name, such as `factory`, `publish`, or `hook`.
    ///
    /// # Returns
    /// The static name of the pending operation.
    pub fn stage(&self) -> &'static str {
        self.stage
    }
}
