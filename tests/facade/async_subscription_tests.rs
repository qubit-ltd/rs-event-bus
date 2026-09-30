// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public ordering handoff and reentrant wake contracts after scheduler
//! migration.

use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

use crate::support::manual_async::block_on;

/// Reenters the current scheduler snapshot API from a runner's notification.
struct SnapshotWake {
    bus: AsyncEventBus,
    calls: AtomicUsize,
}

impl Wake for SnapshotWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let snapshot = self.bus.delivery_metrics();
        assert!(snapshot.running_handlers <= 2);
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

/// A blocked key preserves FIFO, another key progresses, and handoff wakes can
/// reenter the live scheduler without retaining its internal lock.
#[test]
fn test_public_ordering_handoff_allows_reentrant_snapshot_waker() {
    let scheduling = DeliverySchedulingConfig::new(
        NonZeroUsize::new(2).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        NonZeroUsize::new(4).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    )
    .unwrap();
    let provider = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::default()).unwrap());
    let bus = AsyncEventBus::with_config(
        ProviderId::new("ordering-migration").unwrap(),
        provider,
        EventBusFacadeConfig::new().with_delivery_scheduling(scheduling),
    )
    .unwrap();
    let topic = Topic::<u32>::new("ordering.migration").unwrap();
    let mut subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("ordered", topic.clone()).unwrap().with_options(
                SubscribeOptions::builder()
                    .ordering_policy(OrderingPolicy::PerKey)
                    .build(),
            ),
        ),
    )
    .unwrap();
    for (value, key) in [(0_u32, "a"), (1, "a"), (2, "b")] {
        let _ = block_on(
            bus.publish(
                PublishRequest::builder()
                    .topic(topic.clone())
                    .payload(value)
                    .ordering_key(key)
                    .build()
                    .unwrap(),
            ),
        )
        .unwrap();
    }
    let release = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(Mutex::new(Vec::new()));
    let handler_release = release.clone();
    let handler_observed = observed.clone();
    let mut runner = Box::pin(subscription.run(move |delivery: Delivery<u32>| {
        let value = *delivery.payload();
        handler_observed.lock().unwrap().push(value);
        let release = handler_release.clone();
        std::future::poll_fn(move |_| {
            if value == 0 && !release.load(Ordering::SeqCst) {
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        })
    }));
    let wake = Arc::new(SnapshotWake {
        bus: bus.clone(),
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(wake.clone());
    let mut context = Context::from_waker(&waker);
    for _ in 0..16 {
        assert!(runner.as_mut().poll(&mut context).is_pending());
    }
    assert_eq!(*observed.lock().unwrap(), vec![0, 2]);
    let before_handoff = wake.calls.load(Ordering::SeqCst);
    release.store(true, Ordering::SeqCst);
    for _ in 0..16 {
        assert!(runner.as_mut().poll(&mut context).is_pending());
    }
    assert_eq!(*observed.lock().unwrap(), vec![0, 2, 1]);
    assert!(
        wake.calls.load(Ordering::SeqCst) > before_handoff,
        "lane completion must notify through the reentrant waker"
    );
    assert_eq!(bus.delivery_metrics().completed, 3);
    drop(runner);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}
