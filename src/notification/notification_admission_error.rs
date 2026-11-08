// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Detailed errors returned when notification admission fails.

use std::error::Error;
use std::fmt;

use super::notification_rejection_reason::NotificationRejectionReason;

/// Returns the original payload and reason when admission rejects a
/// notification.
///
/// The error owns the payload and does not require its type to implement
/// `Debug` or `Display`.
///
/// # Type Parameters
/// - `T`: payload returned to the caller after rejection.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::num::NonZeroUsize;
///
/// use qubit_event_bus::EventBus;
/// use qubit_event_bus::NotificationRejectionReason;
/// use qubit_event_bus::WeightedNotificationPublisher;
/// use qubit_event_bus::model::Topic;
///
/// let bus = EventBus::local(Default::default())?;
/// let publisher = WeightedNotificationPublisher::new(
///     bus,
///     Topic::<String>::new("notifications")?,
///     NonZeroUsize::new(1).unwrap(),
///     NonZeroUsize::new(10).unwrap(),
///     |_| {},
/// )?;
/// if let Err(error) = publisher.try_publish_detailed(
///     "notification".to_owned(),
///     NonZeroUsize::new(11).unwrap(),
/// ) {
///     match error.reason() {
///         NotificationRejectionReason::QueueCapacity
///         | NotificationRejectionReason::WeightCapacity => { /* capacity may recover */ }
///         NotificationRejectionReason::OversizedWeight => { /* adjust the request */ }
///         NotificationRejectionReason::Closed => { /* stop submitting */ }
///         _ => { /* handle reasons added in a future version */ }
///     }
///     let original_payload = error.into_payload();
///     assert_eq!("notification", original_payload);
/// }
/// publisher.close()?;
/// # Ok(())
/// # }
/// ```
#[must_use]
pub struct NotificationAdmissionError<T> {
    /// Admission reason observed while processing the entry.
    reason: NotificationRejectionReason,
    /// Original payload returned to the caller.
    payload: T,
}

impl<T> NotificationAdmissionError<T> {
    /// Creates a rejection error for the notification admission module.
    ///
    /// # Parameters
    /// - `reason`: admission reason observed by the publisher.
    /// - `payload`: original payload returned to the caller.
    ///
    /// # Returns
    /// An error owning the reason and rejected payload.
    pub(super) fn new(reason: NotificationRejectionReason, payload: T) -> Self {
        Self { reason, payload }
    }

    /// Returns the reason observed by this admission attempt.
    ///
    /// # Returns
    /// The reason this publisher observed while processing the entry.
    #[must_use]
    pub fn reason(&self) -> NotificationRejectionReason {
        self.reason
    }

    /// Returns the original rejected payload.
    ///
    /// # Returns
    /// The payload originally supplied to the publisher.
    #[must_use]
    pub fn into_payload(self) -> T {
        self.payload
    }
}

impl<T> fmt::Debug for NotificationAdmissionError<T> {
    /// Formats the rejection reason without exposing the payload.
    ///
    /// # Parameters
    /// - `formatter`: destination for the formatted error.
    ///
    /// # Returns
    /// The result of writing the formatted rejection reason.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NotificationAdmissionError")
            .field("reason", &self.reason)
            .field("payload", &"..")
            .finish()
    }
}

impl<T> fmt::Display for NotificationAdmissionError<T> {
    /// Formats the rejection reason without exposing the payload.
    ///
    /// # Parameters
    /// - `formatter`: destination for the formatted error.
    ///
    /// # Returns
    /// The result of writing the rejection message.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self.reason {
            NotificationRejectionReason::QueueCapacity => "notification queue is full",
            NotificationRejectionReason::WeightCapacity => "notification weight capacity is full",
            NotificationRejectionReason::OversizedWeight => "notification weight exceeds the configured limit",
            NotificationRejectionReason::Closed => "notification publisher is closed",
        })
    }
}

impl<T: Send + Sync + 'static> Error for NotificationAdmissionError<T> {}
