// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Async subscription behavior for provider-reported delivery gaps.

mod support;

use std::sync::Arc;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::ReceiveError;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionStopReason;
use qubit_event_bus::model::Topic;

use crate::support::fake_spi::FakeAsyncEventBusSpi;
use crate::support::manual_async::block_on;

#[test]
fn default_gap_policy_stops_async_run_with_the_gap_reason() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("gap-test").unwrap(), spi.clone()).unwrap();
    let topic = Topic::<u32>::new("gap.events").unwrap();
    let mut subscription = block_on(bus.subscribe(SubscribeRequest::new("gap-test", topic).unwrap())).unwrap();
    spi.inject_gap();

    let error = block_on(subscription.run(|_| async { Ok::<(), qubit_event_bus::DeliveryError>(()) }))
        .expect_err("default stop policy must terminate after a gap");
    match error {
        ReceiveError::Stopped(reason) => match reason.as_ref() {
            SubscriptionStopReason::Gap { gap } => {
                assert_eq!(gap.reason.as_ref(), "fake gap");
                assert_eq!(gap.missed, Some(1));
            }
            other => panic!("expected gap reason, got {other:?}"),
        },
        other => panic!("expected stopped receive, got {other:?}"),
    }
}
