// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public contracts for shared event-processing pipelines.

use std::sync::Arc;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::error::DeliveryAttemptError;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::ShutdownMode;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryContext;
use qubit_retry::RetryPolicy;

use crate::support::fake_spi::FakeAsyncEventBusSpi;
use crate::support::manual_async::block_on;
use crate::support::manual_async::poll_once;

mod support;

/// Confirms that an asynchronous retry-rule panic requeues the original token.
#[test]
fn test_async_retry_rule_panic_requeues_delivery() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("test").expect("valid provider ID"), spi.clone())
        .expect("fake provider constructs the facade");
    let topic = Topic::<u32>::new("test.retry-rule-panic").expect("valid topic");
    let options = SubscribeOptions::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(2)
                .build()
                .expect("valid retry policy"),
        )
        .retry_rule(|_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| panic!("synthetic retry rule panic"))
        .build();
    let request = SubscribeRequest::new("retry-rule-panic", topic.clone())
        .expect("valid subscriber ID")
        .with_options(options);
    let mut subscription = block_on(bus.subscribe(request)).expect("subscription is supported");
    block_on(bus.publish(PublishRequest::new(topic, 42_u32).expect("valid request"))).expect("publish is accepted");
    let mut run = Box::pin(subscription.run(|_| async {
        Err(DeliveryError::Handler {
            source: Box::new(std::io::Error::other("synthetic handler failure")),
        })
    }));
    let mut run_pending = false;
    for _ in 0..16 {
        run_pending = poll_once(run.as_mut()).is_pending();
        if !spi.settlement_dispositions().is_empty() || !run_pending {
            break;
        }
    }
    assert!(run_pending, "runner remains active after settling the failed delivery");
    assert_eq!(
        spi.settlement_dispositions(),
        vec![DeliveryDisposition::Retry],
        "retry-rule panic must not terminally reject the received delivery",
    );
    drop(run);
    block_on(subscription.close()).expect("subscription closes after runner cancellation");
    block_on(bus.shutdown(ShutdownMode::Immediate)).expect("bus shuts down");
}
