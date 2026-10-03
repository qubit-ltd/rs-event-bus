// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Conditions a caller can require from reported publication admission.

/// The admission condition a caller requires before treating a receipt as
/// usable.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::AdmissionCheckError;
/// use qubit_event_bus::model::AdmissionRequirement;
/// use qubit_event_bus::model::AdmissionStatus;
/// use qubit_event_bus::model::DestinationAdmission;
/// use qubit_event_bus::model::EventId;
/// use qubit_event_bus::model::ProviderId;
/// use qubit_event_bus::model::PublishAcknowledgement;
/// use qubit_event_bus::model::PublishReceipt;
/// use qubit_event_bus::model::SubscriberId;
/// use qubit_id::Id;
///
/// let receipt = PublishReceipt::new(
///     EventId::new("orders-42").unwrap(),
///     Some(EventId::new("orders-42").unwrap()),
///     ProviderId::new("local").unwrap(),
///     PublishAcknowledgement::DestinationAdmissions(vec![
///         DestinationAdmission::new(
///             Id::new(1),
///             SubscriberId::new_static("orders.primary"),
///             AdmissionStatus::Accepted,
///         ),
///         DestinationAdmission::new(
///             Id::new(2),
///             SubscriberId::new_static("orders.audit"),
///             AdmissionStatus::Rejected("queue full".into()),
///         ),
///     ]),
/// );
/// assert_eq!(
///     receipt.check_admission(AdmissionRequirement::AtLeastOneAccepted),
///     Ok(()),
/// );
/// assert_eq!(
///     receipt.check_admission(AdmissionRequirement::ProviderOrDestinationAccepted),
///     Ok(()),
/// );
/// assert!(matches!(
///     receipt.check_admission(AdmissionRequirement::AtLeastOneAcceptedAndNoRejected),
///     Err(AdmissionCheckError::RejectedDestinations { count: 1 }),
/// ));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum AdmissionRequirement {
    /// Accept either provider-level acknowledgement or at least one reported
    /// destination acceptance; destination rejections may also be present.
    ProviderOrDestinationAccepted,
    /// Require at least one destination to accept the event.
    AtLeastOneAccepted,
    /// Require at least one acceptance and no destination rejections.
    AtLeastOneAcceptedAndNoRejected,
}
