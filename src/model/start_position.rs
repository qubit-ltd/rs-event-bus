// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Requested provider position for creating or recovering a subscription.

/// Requested position from which a capable provider starts or resumes a
/// subscription.
///
/// Providers may use this value only when creating new persistent state. When
/// recovering existing state, a provider can preserve its saved cursor and
/// apply its own policy for unsettled deliveries. The exact behavior is
/// provider-specific; conflicting positions may be rejected.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::StartPosition;
///
/// let position = StartPosition::At("offset-42".into());
/// assert!(matches!(position, StartPosition::At(_)));
/// ```
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
#[must_use]
pub enum StartPosition {
    /// For new provider state, begin with events published after creation.
    ///
    /// This does not require a provider to reset an existing subscription's
    /// cursor when recovering it.
    #[default]
    New,
    /// For new provider state, begin with the earliest retained event.
    ///
    /// Recovery of existing state remains provider-specific; for example, a
    /// provider may retain the saved cursor and recover unsettled deliveries.
    Earliest,
    /// Request a provider-specific offset or cursor.
    ///
    /// A provider may apply this when creating new state, preserve existing
    /// state during recovery, or reject a conflicting request.
    At(
        /// Provider-defined offset or cursor.
        Box<str>,
    ),
}
