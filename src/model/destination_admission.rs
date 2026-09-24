// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Per-destination publication admission information.

use qubit_id::Id;

use super::SubscriberId;

/// Admission outcome for a single destination, before handler execution.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum AdmissionStatus {
    /// The provider accepted the delivery for dispatch.
    Accepted,
    /// A subscription filter excluded the event.
    Filtered,
    /// The provider rejected admission for the stated reason.
    Rejected(Box<str>),
}

/// The admission result of one subscriber in the publication snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DestinationAdmission {
    subscription_id: Id,
    subscriber_id: SubscriberId,
    status: AdmissionStatus,
}

impl DestinationAdmission {
    /// Creates a destination admission result.
    pub fn new(subscription_id: Id, subscriber_id: SubscriberId, status: AdmissionStatus) -> Self {
        Self {
            subscription_id,
            subscriber_id,
            status,
        }
    }
    /// Returns the bus-local subscription object ID.
    pub fn subscription_id(&self) -> Id {
        self.subscription_id
    }
    /// Returns the logical subscriber ID.
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }
    /// Returns admission status; it does not report handler completion.
    pub fn status(&self) -> &AdmissionStatus {
        &self.status
    }
}
