// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Initial provider position for a new subscription.

/// Position from which a capable provider starts a subscription.
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
    /// Begin with events published after subscription creation.
    #[default]
    New,
    /// Begin with the earliest retained event.
    Earliest,
    /// Resume at a provider-specific position.
    At(
        /// Provider-defined offset or cursor.
        Box<str>,
    ),
}
