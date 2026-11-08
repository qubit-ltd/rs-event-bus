// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Reasons why a notification publisher rejected an entry.

/// Identifies the admission check that rejected a notification.
///
/// Capacity reasons describe the condition observed by the admission attempt.
/// Queue or weight capacity may become available later. An oversized weight
/// cannot fit the configured budget while the request and budget remain the
/// same. `Closed` means the publisher has stopped accepting notifications.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::NotificationRejectionReason;
///
/// fn recover(reason: NotificationRejectionReason) {
///     match reason {
///         NotificationRejectionReason::QueueCapacity
///         | NotificationRejectionReason::WeightCapacity => { /* capacity may recover */ }
///         NotificationRejectionReason::OversizedWeight => { /* adjust the request */ }
///         NotificationRejectionReason::Closed => { /* stop submitting */ }
///         _ => { /* handle reasons added in a future version */ }
///     }
/// }
/// ```
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NotificationRejectionReason {
    /// The bounded queue had no available item slot.
    QueueCapacity,
    /// The outstanding declared-weight budget could not fit this entry.
    WeightCapacity,
    /// This entry's declared weight exceeds the total configured budget.
    OversizedWeight,
    /// The publisher has stopped accepting entries.
    Closed,
}
