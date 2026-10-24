// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::TypeId;
use std::env::var;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_id::Id;

use crate::support;

/// Polls a future once for test setup that must complete synchronously.
///
/// Panics if the future requires an executor wakeup instead of completing
/// during this deterministic probe.
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("probe operation unexpectedly pending"),
    }
}

/// Waker callback that probes publishing while shutdown wakes the receiver.
struct Reenter {
    bus: AsyncEventBus,
    calls: AtomicUsize,
    shutdown_started: AtomicBool,
}

impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        if !self.shutdown_started.load(Ordering::SeqCst) {
            return;
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        let topic = Topic::<String>::new("review.reentrant").unwrap();
        let request = PublishRequest::new(topic, "after-close".to_owned()).unwrap();
        assert!(
            ready(self.bus.publish(request)).is_err(),
            "publishing after shutdown must be rejected"
        );
    }
}

#[test]
fn test_shutdown_waker_can_reenter_publish() {
    let test_name = "test_shutdown_waker_can_reenter_publish";
    let Ok(case) = var("QUBIT_EVENT_BUS_ISOLATED_CASE") else {
        for case in ["immediate", "graceful"] {
            support::isolated_process::run_case(test_name, case);
        }
        return;
    };
    let spi = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap());
    let bus = AsyncEventBus::from_spi(ProviderId::new("local").unwrap(), spi.clone()).unwrap();
    let topic = Topic::<String>::new("review.reentrant").unwrap();
    let subscription = ready(bus.subscribe(SubscribeRequest::new("consumer", topic).unwrap())).unwrap();
    let reenter = Arc::new(Reenter {
        bus: bus.clone(),
        calls: AtomicUsize::new(0),
        shutdown_started: AtomicBool::new(false),
    });
    let waker = Waker::from(reenter.clone());
    let mut runner = Box::pin(subscription.run(|_| async { Ok(()) }));
    assert!(
        runner.as_mut().poll(&mut Context::from_waker(&waker)).is_pending(),
        "subscription runner waits for a delivery"
    );
    let mode = match case.as_str() {
        "immediate" => ShutdownMode::Immediate,
        "graceful" => ShutdownMode::Graceful {
            timeout: Duration::from_secs(1),
        },
        _ => panic!("unknown isolated case"),
    };
    reenter.shutdown_started.store(true, Ordering::SeqCst);
    assert_eq!(
        ShutdownOutcome::Complete,
        ready(spi.shutdown(mode)).unwrap(),
        "provider shutdown completes"
    );
    assert!(
        reenter.calls.load(Ordering::SeqCst) >= 1,
        "shutdown wakes the reentrant callback"
    );
    drop(runner);
    let report = ready(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    assert_eq!(
        report.outcome,
        ShutdownOutcome::Complete,
        "bus shutdown completes after dropping the runner"
    );
    assert_eq!(
        report.known_abandoned_deliveries, 0,
        "the report has no known abandoned deliveries"
    );
    assert!(
        report.provider_may_have_abandoned_deliveries,
        "the report preserves the provider abandonment possibility"
    );
}

fn subscription_request(id: u64) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        TopicAddress::new("internal.async.lifecycle").expect("valid topic"),
        SubscriberId::new(format!("consumer-{id}")).expect("valid subscriber ID"),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::new(),
        TypeId::of::<u32>(),
    )
}

#[test]
fn test_close_is_idempotent_and_receive_reports_closed() {
    let spi = AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).expect("valid config");
    let mut receiver = ready(spi.subscribe(subscription_request(9001))).expect("subscription is accepted");

    ready(receiver.close()).expect("first close succeeds");
    ready(receiver.close()).expect("repeated close succeeds");
    assert!(
        matches!(
            ready(receiver.receive(Duration::ZERO)).expect("closed receive succeeds"),
            ReceiveOutcome::Closed
        ),
        "receiving after close reports a closed subscription"
    );
}
