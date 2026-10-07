// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal subscribe request identity and policy tests.

use crate::model::AckMode;
use crate::model::SubscribeOptions;
use crate::model::SubscribeRequest;
use crate::model::Topic;

#[test]
fn test_request_identity_and_manual_ack_policy_remain_together() {
    let options = SubscribeOptions::<u64>::builder()
        .ack_mode(AckMode::Manual)
        .build();
    let request = SubscribeRequest::new(
        "internal-model-reader",
        Topic::<u64>::new("model.internal.subscribe").expect("valid topic"),
    )
    .expect("valid request")
    .with_options(options);

    let (subscriber_id, topic, options) = request.into_parts();
    assert_eq!(subscriber_id.as_str(), "internal-model-reader");
    assert_eq!(topic.name(), "model.internal.subscribe");
    assert_eq!(options.ack_mode(), AckMode::Manual);
}
