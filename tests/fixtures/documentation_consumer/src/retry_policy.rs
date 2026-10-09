// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Compiled retry-policy example used by both user guides.

use std::time::Duration;

use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_retry::BackoffPolicy;
use qubit_retry::RetryPolicy;
use qubit_retry::RetryPolicyError;

use crate::orders::events::OrderCreated;

/// Builds the customer-view request with three total attempts and a fixed delay.
pub fn customer_view_retry_request() -> Result<SubscribeRequest<OrderCreated>, RetryPolicyError> {
    let options = SubscribeOptions::<OrderCreated>::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(3)
                .backoff(BackoffPolicy::fixed(Duration::from_millis(200)))
                .build()?,
        )
        .build();
    let request = SubscribeRequest::new(
        "customer-view",
        Topic::<OrderCreated>::new_static("orders.created"),
    )
    .expect("static topic and subscriber ID are valid")
    .with_options(options);
    Ok(request)
}
