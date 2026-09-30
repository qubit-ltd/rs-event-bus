// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Per-destination publication admission information.

use qubit_id::Id;

use super::AdmissionStatus;
use super::SubscriberId;

/// The admission result of one subscriber in the publication snapshot.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::AdmissionStatus;
/// use qubit_event_bus::model::DestinationAdmission;
/// use qubit_event_bus::model::SubscriberId;
/// use qubit_id::Id;
///
/// let admission = DestinationAdmission::new(
///     Id::new(1),
///     SubscriberId::new("audit").unwrap(),
///     AdmissionStatus::Accepted,
/// );
/// assert_eq!(admission.status(), &AdmissionStatus::Accepted);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use]
pub struct DestinationAdmission {
    /// Bus-local subscription identifier for this destination.
    subscription_id: Id,
    /// Logical subscriber associated with the destination.
    subscriber_id: SubscriberId,
    /// Provider's admission decision for the destination.
    status: AdmissionStatus,
}

impl DestinationAdmission {
    /// Creates a destination admission result.
    ///
    /// # Parameters
    /// - `subscription_id`: bus-local subscription identifier.
    /// - `subscriber_id`: logical subscriber identifier.
    /// - `status`: admission decision reported for this destination.
    ///
    /// # Returns
    /// A destination result containing the supplied identifiers and status.
    pub fn new(subscription_id: Id, subscriber_id: SubscriberId, status: AdmissionStatus) -> Self {
        Self {
            subscription_id,
            subscriber_id,
            status,
        }
    }
    /// Returns the bus-local subscription object ID.
    ///
    /// # Returns
    /// The identifier assigned to the subscription by its bus.
    #[must_use = "Use the returned subscription id."]
    #[inline]
    pub fn subscription_id(&self) -> Id {
        self.subscription_id
    }
    /// Returns the logical subscriber ID.
    ///
    /// # Returns
    /// The stable identifier for the logical consumer.
    #[must_use = "Use the returned subscriber id."]
    #[inline]
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }
    /// Returns admission status; it does not report handler completion.
    ///
    /// # Returns
    /// The provider's decision for this destination.
    #[must_use = "Use the returned status."]
    #[inline]
    pub fn status(&self) -> &AdmissionStatus {
        &self.status
    }
}
