// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal publication receipt classification tests.

use crate::model::AdmissionSummary;
use crate::model::EventId;
use crate::model::ProviderId;
use crate::model::PublishAcknowledgement;
use crate::model::PublishReceipt;

fn receipt(acknowledgement: PublishAcknowledgement) -> PublishReceipt {
    PublishReceipt::new(
        EventId::new("receipt-event").expect("valid event ID"),
        None,
        ProviderId::new("receipt-provider").expect("valid provider ID"),
        acknowledgement,
    )
}

#[test]
fn test_empty_visible_snapshot_and_opaque_acceptance_keep_distinct_summaries() {
    let empty = receipt(PublishAcknowledgement::DestinationAdmissions(Vec::new()));
    let opaque = receipt(PublishAcknowledgement::Accepted {
        provider_message_id: None,
        metadata: Default::default(),
    });

    assert_eq!(empty.admission_summary(), Some(AdmissionSummary::default()));
    assert_eq!(opaque.admission_summary(), None);
}
