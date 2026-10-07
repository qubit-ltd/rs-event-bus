// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public subscribe request option replacement tests.

use qubit_event_bus::model::AckMode;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;

#[test]
fn test_option_replacement_is_visible_and_survives_consumption() {
    let options = SubscribeOptions::<String>::builder().ack_mode(AckMode::Manual).build();
    let request = SubscribeRequest::new(
        "model-subscriber",
        Topic::<String>::new("model.subscribe").expect("valid topic"),
    )
    .expect("valid request")
    .with_options(options);

    assert_eq!(request.options().ack_mode(), AckMode::Manual);
    let (subscriber, topic, options) = request.into_parts();
    assert_eq!(subscriber.as_str(), "model-subscriber");
    assert_eq!(topic.name(), "model.subscribe");
    assert_eq!(options.ack_mode(), AckMode::Manual);
}
