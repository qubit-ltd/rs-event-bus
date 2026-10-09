// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Sync subscription behavior for provider-reported delivery gaps.

mod support;

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionStopReason;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

use crate::support::fake_spi::FakeEventBusSpi;

#[test]
fn test_default_gap_policy_stops_sync_subscription_with_the_gap_reason() {
    let spi = Arc::new(FakeEventBusSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("gap-test").unwrap(), spi.clone()).unwrap();
    let topic = Topic::<u32>::new("gap.events").unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = calls.clone();
    let subscription = bus
        .subscribe(SubscribeRequest::new("gap-test", topic.clone()).unwrap(), move |_| {
            handler_calls.fetch_add(1, Ordering::SeqCst);
            Ok::<(), DeliveryError>(())
        })
        .unwrap();
    spi.inject_gap();

    let deadline = Instant::now() + Duration::from_secs(3);
    let reason = loop {
        if let Some(reason) = subscription.terminal_failure() {
            break reason;
        }
        assert!(Instant::now() < deadline, "gap was not retained as terminal cause");
        std::thread::yield_now();
    };
    match reason.as_ref() {
        SubscriptionStopReason::Gap { gap } => {
            assert_eq!(gap.reason.as_ref(), "fake gap");
            assert_eq!(gap.missed, Some(1));
        }
        other => panic!("expected gap reason, got {other:?}"),
    }

    let _ = bus.publish(PublishRequest::new(topic, 7).unwrap()).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    subscription.cancel().unwrap();
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
}
