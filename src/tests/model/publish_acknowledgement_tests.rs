// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal publish acknowledgement behavior tests.

use crate::model::AdmissionOutcome;
use crate::model::PublishAcknowledgement;

#[test]
fn interceptor_drop_is_distinct_from_an_empty_destination_report() {
    let dropped = PublishAcknowledgement::DroppedByInterceptor;
    let no_destinations = PublishAcknowledgement::DestinationAdmissions(Vec::new());

    assert!(dropped.is_dropped());
    assert_eq!(dropped.admission_outcome(), AdmissionOutcome::Dropped);
    assert!(!no_destinations.is_dropped());
    assert_eq!(no_destinations.admission_outcome(), AdmissionOutcome::NoDestinations);
}
