// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public regression tests for asynchronous event bus lifecycle contracts.

mod support;

use std::future::Future;
use std::future::poll_fn;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::AsyncSubscriptionRunState;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::LifecycleError;
use qubit_event_bus::ShutdownError;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use support::fake_spi::FakeAsyncEventBusSpi;

fn poll_once<F: Future>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(waker))
}

/// Polls a local future once and returns its ready output.
///
/// Panics when the operation unexpectedly remains pending on its first poll.
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match poll_once(future.as_mut(), Waker::noop()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("expected local operation to complete on first poll"),
    }
}

struct DropProbe(Arc<AtomicBool>);

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
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
    drop(close);

    let mut close = Box::pin(subscription.close());
    assert!(poll_once(close.as_mut(), Waker::noop()).is_pending());
    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(poll_once(shutdown.as_mut(), Waker::noop()).is_pending());
    drop(shutdown);

    gates[1].store(true, Ordering::SeqCst);
    assert!(poll_once(close.as_mut(), Waker::noop()).is_ready());
    assert_eq!(2, finished.load(Ordering::SeqCst));
    let report = ready(bus.shutdown(ShutdownMode::Immediate)).expect("bus shutdown");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
    assert!(report.provider_may_have_abandoned_deliveries);
}

#[test]
fn test_successful_close_releases_handler_capture_and_keeps_metrics() {
    let bus = ready(AsyncEventBus::local(Default::default())).expect("local bus");
    let topic = Topic::<usize>::new("regression.close-handler-release").expect("topic");
    let mut subscription =
        ready(bus.subscribe(SubscribeRequest::new("close-handler-release", topic).expect("request")))
            .expect("subscription");
    let dropped = Arc::new(AtomicBool::new(false));
    let capture = Arc::new(DropProbe(dropped.clone()));
    let capture_weak: Weak<DropProbe> = Arc::downgrade(&capture);
    let mut run = {
        let capture = capture.clone();
        Box::pin(subscription.run(move |_| {
            let _capture = capture.clone();
            async { Ok::<(), DeliveryError>(()) }
        }))
    };

    assert!(poll_once(run.as_mut(), Waker::noop()).is_pending());
    drop(run);
    drop(capture);
    assert!(capture_weak.upgrade().is_some());
    assert!(!dropped.load(Ordering::SeqCst));
    let metrics = subscription.delivery_metrics();
    let run_state = subscription.run_state();
    assert_eq!(AsyncSubscriptionRunState::Paused, run_state);
    let terminal_failure = subscription.terminal_failure();

    ready(subscription.close()).expect("subscription close");

    assert!(capture_weak.upgrade().is_none());
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(metrics, subscription.delivery_metrics());
    assert_eq!(AsyncSubscriptionRunState::Stopped, subscription.run_state());
    assert_eq!(terminal_failure.is_some(), subscription.terminal_failure().is_some());
    let _ = ready(bus.shutdown(ShutdownMode::Immediate)).expect("bus shutdown");
}

#[test]
fn test_cancelled_run_retains_handler_and_resumes_with_it() {
    let bus = ready(AsyncEventBus::local(Default::default())).expect("local bus");
    let topic = Topic::<usize>::new("regression.paused-handler-retained").expect("topic");
    let mut subscription =
        ready(bus.subscribe(SubscribeRequest::new("paused-handler-retained", topic.clone()).expect("request")))
            .expect("subscription");
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = Arc::new(DropProbe(dropped.clone()));
    let probe_weak = Arc::downgrade(&probe);
    let original_calls = Arc::new(AtomicUsize::new(0));
    let mut run = {
        let probe = probe.clone();
        let original_calls = original_calls.clone();
        Box::pin(subscription.run(move |_| {
            let _probe = probe.clone();
            let original_calls = original_calls.clone();
            async move {
                original_calls.fetch_add(1, Ordering::SeqCst);
                Ok::<(), DeliveryError>(())
            }
        }))
    };
    assert!(poll_once(run.as_mut(), Waker::noop()).is_pending());
    drop(run);
    drop(probe);
    assert!(probe_weak.upgrade().is_some());
    assert!(!dropped.load(Ordering::SeqCst));

    let _ = ready(bus.publish(PublishRequest::new(topic, 7).expect("publish request"))).expect("publish");
    let replacement_dropped = Arc::new(AtomicBool::new(false));
    let replacement_probe = Arc::new(DropProbe(replacement_dropped.clone()));
    let replacement_probe_weak = Arc::downgrade(&replacement_probe);
    let replacement_calls = Arc::new(AtomicUsize::new(0));
    let mut resumed = {
        let replacement_probe = replacement_probe.clone();
        let replacement_calls = replacement_calls.clone();
        Box::pin(subscription.run(move |_| {
            let _replacement_probe = replacement_probe.clone();
            let replacement_calls = replacement_calls.clone();
            async move {
                replacement_calls.fetch_add(1, Ordering::SeqCst);
                Ok::<(), DeliveryError>(())
            }
        }))
    };
    for _ in 0..8 {
        if replacement_calls.load(Ordering::SeqCst) != 0 {
            break;
        }
        assert!(poll_once(resumed.as_mut(), Waker::noop()).is_pending());
    }
    assert_eq!(0, original_calls.load(Ordering::SeqCst));
    assert_eq!(1, replacement_calls.load(Ordering::SeqCst));
    assert!(probe_weak.upgrade().is_none());
    assert!(replacement_probe_weak.upgrade().is_some());
    drop(resumed);
    drop(replacement_probe);
    ready(subscription.close()).expect("subscription close");
    assert!(replacement_probe_weak.upgrade().is_none());
    assert!(replacement_dropped.load(Ordering::SeqCst));
    let _ = ready(bus.shutdown(ShutdownMode::Immediate)).expect("bus shutdown");
}

#[test]
fn test_failed_close_retains_handler_until_successful_retry() {
    let provider = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("close-retry").expect("provider ID"), provider.clone())
        .expect("facade");
    let topic = Topic::<usize>::new("regression.close-handler-retry").expect("topic");
    let mut subscription = ready(bus.subscribe(SubscribeRequest::new("close-handler-retry", topic).expect("request")))
        .expect("subscription");
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = Arc::new(DropProbe(dropped.clone()));
    let probe_weak = Arc::downgrade(&probe);
    let mut run = {
        let probe = probe.clone();
        Box::pin(subscription.run(move |_| {
            let _probe = probe.clone();
            async { Ok::<(), DeliveryError>(()) }
        }))
    };
    assert!(poll_once(run.as_mut(), Waker::noop()).is_pending());
    drop(run);
    drop(probe);

    provider.panic_on_close_call();
    assert!(matches!(
        ready(subscription.close()),
        Err(LifecycleError::SubscriptionClose(_))
    ));
    assert!(probe_weak.upgrade().is_some());
    assert!(!dropped.load(Ordering::SeqCst));

    ready(subscription.close()).expect("successful close retry");
    assert!(probe_weak.upgrade().is_none());
    assert!(dropped.load(Ordering::SeqCst));
    assert!(matches!(
        ready(bus.shutdown(ShutdownMode::Immediate)),
        Err(ShutdownError::SubscriptionClose(_))
    ));
}

#[test]
fn test_bus_shutdown_releases_handler_capture() {
    let bus = ready(AsyncEventBus::local(Default::default())).expect("local bus");
    let topic = Topic::<usize>::new("regression.shutdown-handler-release").expect("topic");
    let _subscription =
        ready(bus.subscribe(SubscribeRequest::new("shutdown-handler-release", topic).expect("request")))
            .expect("subscription");
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = Arc::new(DropProbe(dropped.clone()));
    let probe_weak = Arc::downgrade(&probe);
    let mut run = {
        let probe = probe.clone();
        Box::pin(_subscription.run(move |_| {
            let _probe = probe.clone();
            async { Ok::<(), DeliveryError>(()) }
        }))
    };
    assert!(poll_once(run.as_mut(), Waker::noop()).is_pending());
    drop(run);
    drop(probe);
    assert!(probe_weak.upgrade().is_some());

    let _ = ready(bus.shutdown(ShutdownMode::Immediate)).expect("bus shutdown");

    assert!(probe_weak.upgrade().is_none());
    assert!(dropped.load(Ordering::SeqCst));
}

#[test]
fn test_natural_receive_close_then_subscription_close_releases_handler_capture() {
    let provider = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("natural-close").expect("provider ID"), provider.clone())
        .expect("facade");
    let topic = Topic::<usize>::new("regression.natural-close-handler-release").expect("topic");
    let mut subscription =
        ready(bus.subscribe(SubscribeRequest::new("natural-close", topic).expect("request"))).expect("subscription");
    let dropped = Arc::new(AtomicBool::new(false));
    let probe = Arc::new(DropProbe(dropped.clone()));
    let probe_weak = Arc::downgrade(&probe);
    let mut run = {
        let probe = probe.clone();
        Box::pin(subscription.run(move |_| {
            let _probe = probe.clone();
            async { Ok::<(), DeliveryError>(()) }
        }))
    };

    assert!(poll_once(run.as_mut(), Waker::noop()).is_pending());
    drop(probe);
    provider.close_receiver(subscription.id());
    assert!(matches!(poll_once(run.as_mut(), Waker::noop()), Poll::Ready(Ok(()))));
    drop(run);
    assert!(probe_weak.upgrade().is_none());
    assert!(dropped.load(Ordering::SeqCst));

    ready(subscription.close()).expect("subscription close after provider EOF");
    assert!(probe_weak.upgrade().is_none());
    assert!(dropped.load(Ordering::SeqCst));
    let _ = ready(bus.shutdown(ShutdownMode::Immediate)).expect("bus shutdown");
}

#[test]
fn test_running_close_and_shutdown_share_receiver_close() {
    let provider = Arc::new(FakeAsyncEventBusSpi::new());
    provider.pause_close();
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("close-race").expect("provider ID"), provider.clone()).expect("facade");
    let topic = Topic::<usize>::new("regression.close-shutdown-race").expect("topic");
    let mut subscription = ready(bus.subscribe(SubscribeRequest::new("close-shutdown-race", topic).expect("request")))
        .expect("subscription");

    let mut close = Box::pin(subscription.close());
    assert!(poll_once(close.as_mut(), Waker::noop()).is_pending());
    assert_eq!(1, provider.close_call_count());
    assert_eq!(1, provider.max_concurrent_close_calls());

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(poll_once(shutdown.as_mut(), Waker::noop()).is_pending());
    assert_eq!(1, provider.close_call_count());
    assert_eq!(1, provider.max_concurrent_close_calls());

    provider.release_close();
    assert!(matches!(poll_once(close.as_mut(), Waker::noop()), Poll::Ready(Ok(()))));
    let report = match poll_once(shutdown.as_mut(), Waker::noop()) {
        Poll::Ready(Ok(report)) => report,
        _ => panic!("shutdown should complete after receiver close is released"),
    };
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(1, provider.close_call_count());
    assert_eq!(1, provider.max_concurrent_close_calls());
}

#[test]
fn test_close_rejects_bus_already_closing_after_cancelled_shutdown() {
    let bus = ready(AsyncEventBus::local(Default::default())).expect("local bus");
    let topic = Topic::<usize>::new("regression.close-after-cancelled-shutdown").expect("topic");
    let mut subscription =
        ready(bus.subscribe(SubscribeRequest::new("close-after-cancelled-shutdown", topic.clone()).expect("request")))
            .expect("subscription");
    let _ = ready(bus.publish(PublishRequest::new(topic, 1).expect("publish request"))).expect("publish");

    let started = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(AtomicBool::new(false));
    let mut run = {
        let started = started.clone();
        let finished = finished.clone();
        let gate = gate.clone();
        Box::pin(subscription.run(move |_| {
            let started = started.clone();
            let finished = finished.clone();
            let gate = gate.clone();
            async move {
                started.fetch_add(1, Ordering::SeqCst);
                poll_fn(|_| {
                    if gate.load(Ordering::SeqCst) {
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
    assert_eq!(1, started.load(Ordering::SeqCst));
    drop(run);

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(poll_once(shutdown.as_mut(), Waker::noop()).is_pending());
    drop(shutdown);

    assert!(matches!(ready(subscription.close()), Err(LifecycleError::Closed)));
    assert_eq!(0, finished.load(Ordering::SeqCst));

    gate.store(true, Ordering::SeqCst);
    let report = ready(bus.shutdown(ShutdownMode::Immediate)).expect("retry bus shutdown");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert!(matches!(ready(subscription.close()), Err(LifecycleError::Closed)));
}

#[test]
fn test_close_rejects_bus_after_graceful_shutdown_timeout() {
    let bus = ready(AsyncEventBus::local(Default::default())).expect("local bus");
    let topic = Topic::<usize>::new("regression.close-after-graceful-timeout").expect("topic");
    let mut subscription =
        ready(bus.subscribe(SubscribeRequest::new("close-after-graceful-timeout", topic.clone()).expect("request")))
            .expect("subscription");
    let _ = ready(bus.publish(PublishRequest::new(topic, 1).expect("publish request"))).expect("publish");
    let gate = Arc::new(AtomicBool::new(false));
    let mut run = {
        let gate = gate.clone();
        Box::pin(subscription.run(move |_| {
            let gate = gate.clone();
            async move {
                poll_fn(|_| {
                    if gate.load(Ordering::SeqCst) {
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
    assert!(poll_once(run.as_mut(), Waker::noop()).is_pending());

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::ZERO,
    }));
    assert!(matches!(
        poll_once(shutdown.as_mut(), Waker::noop()),
        Poll::Ready(Err(ShutdownError::TimedOut { .. }))
    ));
    drop(shutdown);
    drop(run);
    assert!(matches!(ready(subscription.close()), Err(LifecycleError::Closed)));

    gate.store(true, Ordering::SeqCst);
    let report = ready(bus.shutdown(ShutdownMode::Immediate)).expect("retry bus shutdown");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
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
    let config = EventBusFacadeConfig::new().with_delivery_scheduling(
        DeliverySchedulingConfig::new(
            NonZeroUsize::new(2).expect("limit"),
            NonZeroUsize::new(256).expect("limit"),
            NonZeroUsize::new(32).expect("limit"),
            NonZeroUsize::new(256).expect("limit"),
        )
        .expect("scheduling"),
    );
    let bus =
        AsyncEventBus::with_config(ProviderId::new("local").expect("provider ID"), provider, config).expect("facade");
    let topic_a = Topic::<usize>::new("regression.admission.a").expect("topic A");
    let topic_b = Topic::<usize>::new("regression.admission.b").expect("topic B");
    let topic_c = Topic::<usize>::new("regression.admission.c").expect("topic C");
    let sub_a = ready(bus.subscribe(SubscribeRequest::new("admission-a", topic_a.clone()).expect("request A")))
        .expect("subscription A");
    let sub_b = ready(bus.subscribe(SubscribeRequest::new("admission-b", topic_b.clone()).expect("request B")))
        .expect("subscription B");
    let sub_c = ready(bus.subscribe(SubscribeRequest::new("admission-c", topic_c.clone()).expect("request C")))
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
