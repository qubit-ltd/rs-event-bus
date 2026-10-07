// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public publish request accessor and ownership tests.

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
