// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Application decisions after examining publication evidence.

/// A decision for the application; this enum performs no publication itself.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum RepublishAction {
    /// Query by the original event ID or use an idempotent reconciliation path.
    ReconcileByEventId,
    /// No attempt admitted the event; a whole-event retry can be considered.
    RepublishWhole,
    /// Repair only rejected destinations; others have already accepted.
    RetryRejectedDestinations,
    /// Do not automatically repeat accepted, opaque, or intentionally dropped work.
    NoRepublish,
}
