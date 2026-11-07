// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Queue entries shared by weighted and unweighted publishers.

use super::WeightReservation;

/// A payload and its optional outstanding-weight reservation.
///
/// # Type Parameters
/// - `T`: payload type sent to the notification worker.
pub(in crate::notification) struct QueuedNotification<T> {
    /// Reservation retained until publication and observation finish.
    pub(in crate::notification) reservation: Option<WeightReservation>,
    /// Payload published by the worker.
    pub(in crate::notification) payload: T,
}
