// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public publish request accessor and ownership tests.

use qubit_event_bus::model::DuplicateRiskPolicy;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::Topic;

#[test]
fn test_header_lookup_and_into_parts_preserve_request_data() {
    let options = PublishOptions::<String>::builder().build();
    let request = PublishRequest::builder()
        .topic(Topic::<String>::new("model.request").expect("valid topic"))
        .payload("payload".to_owned())
        .header("trace-id", "trace-12")
        .options(options)
        .build()
        .expect("complete request");

    assert_eq!(request.header("trace-id"), Some("trace-12"));
    let (envelope, options) = request.into_parts();
    assert_eq!(envelope.payload(), "payload");
    assert_eq!(envelope.header("trace-id"), Some("trace-12"));
    assert!(options.retry_policy().is_none());
}

#[test]
fn test_replacement_options_are_the_values_returned_by_both_access_and_consumption() {
    let options = PublishOptions::<String>::builder()
        .duplicate_risk_policy(DuplicateRiskPolicy::AllowDuplicates)
        .build();
    let request = PublishRequest::new(
        Topic::<String>::new("model.internal.publish").expect("valid topic"),
        "value".into(),
    )
    .expect("request")
    .with_options(options);

    assert_eq!(
        request.options().duplicate_risk_policy(),
        DuplicateRiskPolicy::AllowDuplicates
    );
    let (_, consumed_options) = request.into_parts();
    assert_eq!(
        consumed_options.duplicate_risk_policy(),
        DuplicateRiskPolicy::AllowDuplicates
    );
}
