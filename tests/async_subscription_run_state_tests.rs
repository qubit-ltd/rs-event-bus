// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-neutral asynchronous subscription run-state contract tests.

mod support;

use std::future::Future;
use std::sync::Arc;
use std::task::Poll;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::AsyncSubscription;
use qubit_event_bus::AsyncSubscriptionRunState;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::ReceiveError;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

use support::fake_spi::FakeAsyncEventBusSpi;
use support::manual_async::block_on;
use support::manual_async::poll_once;

fn setup() -> (AsyncEventBus, Arc<FakeAsyncEventBusSpi>) {
    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::with_config(
        ProviderId::new("run-state").expect("provider ID"),
        fake.clone(),
        EventBusFacadeConfig::default(),
    )
    .expect("bus");
    (bus, fake)
}

fn subscribe(bus: &AsyncEventBus) -> AsyncSubscription<u32> {
    block_on(bus.subscribe(
        SubscribeRequest::new("run-state", Topic::<u32>::new("test.topic").expect("topic")).expect("request"),
    ))
    .expect("subscription")
}

fn pending_run<'a>(subscription: &'a AsyncSubscription<u32>) -> impl Future<Output = Result<(), ReceiveError>> + 'a {
    subscription.run(|_| async { Ok(()) })
}

#[test]
fn test_unpolled_run_does_not_change_state_and_first_poll_marks_running() {
    let (bus, _fake) = setup();
    let subscription = subscribe(&bus);
    let run = pending_run(&subscription);
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Unstarted);
    let mut run = Box::pin(run);
    assert!(matches!(poll_once(run.as_mut()), Poll::Pending));
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Running);
    drop(run);
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Paused);
    drop(subscription);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
}

#[test]
fn test_paused_run_can_resume_and_shutdown_stops_it() {
    let (bus, _fake) = setup();
    let subscription = subscribe(&bus);
    {
        let mut run = Box::pin(pending_run(&subscription));
        assert!(matches!(poll_once(run.as_mut()), Poll::Pending));
        assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Running);
    }
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Paused);
    {
        let mut run = Box::pin(pending_run(&subscription));
        assert!(matches!(poll_once(run.as_mut()), Poll::Pending));
        assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Running);
    }
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Stopped);
    drop(subscription);
}

#[test]
fn test_shutdown_stops_a_running_subscription_before_its_guard_drops() {
    let (bus, _fake) = setup();
    let subscription = subscribe(&bus);
    let mut run = Box::pin(pending_run(&subscription));
    assert!(matches!(poll_once(run.as_mut()), Poll::Pending));
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Running);

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    let _ = poll_once(shutdown.as_mut());
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Stopped);
    drop(run);
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Stopped);
    let _ = block_on(shutdown).expect("shutdown");
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Stopped);
    drop(subscription);
}

#[test]
fn test_explicit_close_stops_unstarted_subscription() {
    let (bus, _fake) = setup();
    let mut subscription = subscribe(&bus);
    block_on(subscription.close()).expect("close");
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Stopped);
    assert!(block_on(subscription.run(|_| async { Ok(()) })).is_err());
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Stopped);
    drop(subscription);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
}

#[test]
fn test_shutdown_stops_unstarted_subscription() {
    let (bus, _fake) = setup();
    let subscription = subscribe(&bus);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Stopped);
    drop(subscription);
}

#[test]
fn test_receive_error_stops_subscription() {
    let (bus, fake) = setup();
    let subscription = subscribe(&bus);
    fake.fail_next_receive();
    let result = block_on(subscription.run(|_| async { Ok(()) }));
    assert!(result.is_err());
    assert_eq!(subscription.run_state(), AsyncSubscriptionRunState::Stopped);
    drop(subscription);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
}
