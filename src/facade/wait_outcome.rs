// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outcomes returned by facade idle-wait operations.

/// Result of waiting for topic work reported by a provider or tracked by a
/// facade.
///
/// `Idle` means the selected boundary reports no outstanding work. `TimedOut`
/// means the deadline elapsed while work remained; it does not cancel that
/// work.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::WaitOutcome;
///
/// let outcome = WaitOutcome::TimedOut;
/// let status = match outcome {
///     WaitOutcome::Idle => "idle",
///     WaitOutcome::TimedOut => "timed out",
///     _ => "another outcome",
/// };
/// assert_eq!(status, "timed out");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
#[must_use]
pub enum WaitOutcome {
    /// The selected provider or facade reports no outstanding work for the
    /// topic.
    Idle,
    /// The deadline elapsed while outstanding work remained.
    TimedOut,
}
