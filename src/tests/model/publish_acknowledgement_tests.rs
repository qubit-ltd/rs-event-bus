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
fn test_interceptor_drop_is_distinct_from_an_empty_destination_report() {
    let dropped = PublishAcknowledgement::DroppedByInterceptor;
    let no_destinations = PublishAcknowledgement::DestinationAdmissions(Vec::new());

    assert!(dropped.is_dropped(), "interceptor drops must be reported as dropped");
    assert_eq!(
        dropped.admission_outcome(),
        AdmissionOutcome::Dropped,
        "interceptor drops must have the dropped admission outcome",
    );
    assert!(
        !no_destinations.is_dropped(),
        "an empty destination report is not an interceptor drop",
    );
    assert_eq!(
        no_destinations.admission_outcome(),
        AdmissionOutcome::NoDestinations,
        "an empty destination report must have the no-destinations outcome",
    );
}
