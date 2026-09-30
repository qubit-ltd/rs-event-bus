// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public regression tests for asynchronous event bus lifecycle contracts.

use std::future::Future;
use std::future::poll_fn;
use std::pin::Pin;
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
use qubit_event_bus::DeliveryError;
use qubit_event_bus::LifecycleError;
use qubit_event_bus::facade::DeliveryAdmissionConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

fn poll_once<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match poll_once(future.as_mut(), Waker::noop()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("expected local operation to complete on first poll"),
    }
}

struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn test_close_drains_all_started_handlers() {
    let bus = ready(AsyncEventBus::local(Default::default())).expect("local bus");
    let topic = Topic::<usize>::new("regression.close-drain").expect("topic");
    let mut subscription = ready(bus.subscribe(SubscribeRequest::new("close-drain", topic.clone()).expect("request")))
        .expect("subscription");
    for value in 0..2 {
        let _ =
            ready(bus.publish(PublishRequest::new(topic.clone(), value).expect("publish request"))).expect("publish");
    }

    let gates = Arc::new([AtomicBool::new(false), AtomicBool::new(false)]);
    let started = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let mut run = {
        let gates = gates.clone();
        let started = started.clone();
        let finished = finished.clone();
        Box::pin(subscription.run(move |delivery| {
            let gates = gates.clone();
            let started = started.clone();
            let finished = finished.clone();
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                poll_fn(|_| {
                    if gates[*delivery.payload()].load(Ordering::SeqCst) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
                finished.fetch_add(1, Ordering::SeqCst);
                Ok::<(), DeliveryError>(())
            }
        }))
    };
    assert!(poll_once(run.as_mut(), Waker::noop()).is_pending());
    assert_eq!(2, started.load(Ordering::SeqCst));
    drop(run);

    gates[0].store(true, Ordering::SeqCst);
    let mut close = Box::pin(subscription.close());
    assert!(poll_once(close.as_mut(), Waker::noop()).is_pending());
    assert_eq!(1, finished.load(Ordering::SeqCst));

    gates[1].store(true, Ordering::SeqCst);
    assert!(poll_once(close.as_mut(), Waker::noop()).is_ready());
    assert_eq!(2, finished.load(Ordering::SeqCst));
    let report = ready(bus.shutdown(ShutdownMode::Immediate)).expect("bus shutdown");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
    assert!(report.provider_may_have_abandoned_deliveries);
}

#[test]
fn test_wait_inside_own_handler_returns_would_deadlock() {
    let bus = ready(AsyncEventBus::local(Default::default())).expect("local bus");
    let topic = Topic::<usize>::new("regression.self-wait").expect("topic");
    let mut subscription = ready(bus.subscribe(SubscribeRequest::new("self-wait", topic.clone()).expect("request")))
        .expect("subscription");
    let _ = ready(bus.publish(PublishRequest::new(topic.clone(), 1).expect("publish request"))).expect("publish");

    let observed = Arc::new(Mutex::new(None));
    let mut run = {
        let bus = bus.clone();
        let topic = topic.clone();
        let observed = observed.clone();
        Box::pin(subscription.run(move |_| {
            let bus = bus.clone();
            let topic = topic.clone();
            let observed = observed.clone();
            async move {
                *observed.lock().expect("result lock") = Some(bus.wait_for_received_deliveries(&topic, None).await);
                Ok::<(), DeliveryError>(())
            }
        }))
    };
    assert!(poll_once(run.as_mut(), Waker::noop()).is_pending());
    assert!(matches!(
        observed.lock().expect("result lock").as_ref(),
        Some(Err(LifecycleError::WouldDeadlock {
            operation: "wait_for_received_deliveries"
        }))
    ));
    drop(run);
    ready(subscription.close()).expect("subscription close");
    let report = ready(bus.shutdown(ShutdownMode::Immediate)).expect("bus shutdown");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
    assert!(report.provider_may_have_abandoned_deliveries);
}

#[test]
fn test_admission_wakes_successor_after_coalesced_permit_releases() {
    let provider = Arc::new(AsyncLocalEventBusSpi::new(&Default::default()).expect("local provider"));
    let config =
        EventBusFacadeConfig::new().with_delivery_admission(DeliveryAdmissionConfig::new(2).expect("admission limit"));
    let bus =
        AsyncEventBus::with_config(ProviderId::new("local").expect("provider ID"), provider, config).expect("facade");
    let topic_a = Topic::<usize>::new("regression.admission.a").expect("topic A");
    let topic_b = Topic::<usize>::new("regression.admission.b").expect("topic B");
    let topic_c = Topic::<usize>::new("regression.admission.c").expect("topic C");
    let mut sub_a = ready(bus.subscribe(SubscribeRequest::new("admission-a", topic_a.clone()).expect("request A")))
        .expect("subscription A");
    let mut sub_b = ready(bus.subscribe(SubscribeRequest::new("admission-b", topic_b.clone()).expect("request B")))
        .expect("subscription B");
    let mut sub_c = ready(bus.subscribe(SubscribeRequest::new("admission-c", topic_c.clone()).expect("request C")))
        .expect("subscription C");
    for (topic, value) in [(topic_a.clone(), 0), (topic_a, 1), (topic_b, 0), (topic_c, 0)] {
        let _ = ready(bus.publish(PublishRequest::new(topic, value).expect("publish request"))).expect("publish");
    }

    let gate_a = Arc::new(AtomicBool::new(false));
    let mut run_a = {
        let gate_a = gate_a.clone();
        Box::pin(sub_a.run(move |_| {
            let gate_a = gate_a.clone();
            async move {
                poll_fn(|_| {
                    if gate_a.load(Ordering::SeqCst) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
                Ok::<(), DeliveryError>(())
            }
        }))
    };
    let gate_b = Arc::new(AtomicBool::new(false));
    let gate_c = Arc::new(AtomicBool::new(false));
    let mut run_b = {
        let gate_b = gate_b.clone();
        Box::pin(sub_b.run(move |_| {
            let gate_b = gate_b.clone();
            async move {
                poll_fn(|_| {
                    if gate_b.load(Ordering::SeqCst) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
                Ok::<(), DeliveryError>(())
            }
        }))
    };
    let mut run_c = {
        let gate_c = gate_c.clone();
        Box::pin(sub_c.run(move |_| {
            let gate_c = gate_c.clone();
            async move {
                poll_fn(|_| {
                    if gate_c.load(Ordering::SeqCst) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
                Ok::<(), DeliveryError>(())
            }
        }))
    };
    let wakes_b = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let wakes_c = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker_b = Waker::from(wakes_b.clone());
    let waker_c = Waker::from(wakes_c.clone());
    assert!(poll_once(run_a.as_mut(), Waker::noop()).is_pending());
    assert!(poll_once(run_b.as_mut(), &waker_b).is_pending());
    assert!(poll_once(run_c.as_mut(), &waker_c).is_pending());

    gate_a.store(true, Ordering::SeqCst);
    assert!(poll_once(run_a.as_mut(), Waker::noop()).is_pending());
    assert!(poll_once(run_b.as_mut(), &waker_b).is_pending());
    assert!(poll_once(run_c.as_mut(), &waker_c).is_pending());
    assert!(wakes_c.0.load(Ordering::SeqCst) > 0);

    gate_b.store(true, Ordering::SeqCst);
    gate_c.store(true, Ordering::SeqCst);
    drop(run_a);
    drop(run_b);
    drop(run_c);
    let report = ready(bus.shutdown(ShutdownMode::Immediate)).expect("bus shutdown");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
    assert!(report.provider_may_have_abandoned_deliveries);
}
