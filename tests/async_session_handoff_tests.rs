// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Session lease handoff regressions for canceled asynchronous runners.

mod support;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;
use std::time::Duration;

use qubit_event_bus::AsyncSubscription;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::facade::AsyncEventBus;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::ShutdownMode;

use crate::support::fake_spi::FakeAsyncEventBusSpi;
use crate::support::manual_async::block_on;
use crate::support::manual_async::poll_once;

#[test]
fn test_canceled_runner_hands_session_to_resumed_runner() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let provider: Arc<dyn AsyncEventBusSpi> = spi.clone();
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").expect("provider"), provider)
        .expect("valid provider capabilities");
    let topic = Topic::<u32>::new("session-handoff").expect("topic");
    let first_started = Arc::new(AtomicBool::new(false));
    let allow_completion = Arc::new(AtomicBool::new(false));
    let handler_calls = Arc::new(AtomicUsize::new(0));

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("handoff", topic.clone()).expect("request"))
            .await
            .expect("subscription");
        let _receipt = bus
            .publish(PublishRequest::new(topic, 42).expect("publish request"))
            .await
            .expect("publish");

        let started = Arc::clone(&first_started);
        let completion = Arc::clone(&allow_completion);
        let calls = Arc::clone(&handler_calls);
        let mut first_runner = Box::pin(subscription.run(move |_| {
            let started = Arc::clone(&started);
            let completion = Arc::clone(&completion);
            let calls = Arc::clone(&calls);
            async move {
                started.store(true, Ordering::Release);
                calls.fetch_add(1, Ordering::AcqRel);
                std::future::poll_fn(|cx| {
                    if completion.load(Ordering::Acquire) {
                        std::task::Poll::Ready(Ok(()))
                    } else {
                        cx.waker().wake_by_ref();
                        std::task::Poll::Pending
                    }
                })
                .await
            }
        }));

        for _ in 0..100 {
            let _ = poll_once(first_runner.as_mut());
            if first_started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            first_started.load(Ordering::Acquire),
            "first runner entered its handler"
        );
        drop(first_runner);

        allow_completion.store(true, Ordering::Release);
        let replacement_calls = Arc::new(AtomicUsize::new(0));
        let replacement_calls_in_runner = Arc::clone(&replacement_calls);
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                replacement_calls_in_runner.fetch_add(1, Ordering::AcqRel);
                async { Ok::<(), DeliveryError>(()) }
            }))
        });

        for _ in 0..100 {
            if spi.settlement_count() == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(1, handler_calls.load(Ordering::Acquire));
        assert_eq!(0, replacement_calls.load(Ordering::Acquire));
        let _report = bus.shutdown(ShutdownMode::Immediate).await.expect("shutdown");
        runner.join().expect("runner thread").expect("runner completion");
    });
}

struct DropPanicState {
    panic_on_drop: AtomicBool,
    drops: AtomicUsize,
    wakes: AtomicUsize,
}

unsafe fn clone_drop_panic_waker(data: *const ()) -> RawWaker {
    // SAFETY: Every data pointer is an owned Arc<DropPanicState>.
    unsafe { Arc::<DropPanicState>::increment_strong_count(data.cast()) };
    RawWaker::new(data, &DROP_PANIC_WAKER_VTABLE)
}

unsafe fn wake_drop_panic_waker(data: *const ()) {
    // SAFETY: wake consumes this RawWaker's owned Arc reference.
    let state = unsafe { Arc::<DropPanicState>::from_raw(data.cast()) };
    state.wakes.fetch_add(1, Ordering::Relaxed);
}

unsafe fn wake_drop_panic_waker_by_ref(data: *const ()) {
    // SAFETY: wake_by_ref borrows the live Arc allocation.
    let state = unsafe { &*data.cast::<DropPanicState>() };
    state.wakes.fetch_add(1, Ordering::Relaxed);
}

unsafe fn drop_raw_panic_waker(data: *const ()) {
    // SAFETY: drop consumes this RawWaker's owned Arc reference.
    let state = unsafe { Arc::<DropPanicState>::from_raw(data.cast()) };
    state.drops.fetch_add(1, Ordering::Relaxed);
    assert!(
        !state.panic_on_drop.load(Ordering::Relaxed),
        "intentional waker drop panic"
    );
}

static DROP_PANIC_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_drop_panic_waker,
    wake_drop_panic_waker,
    wake_drop_panic_waker_by_ref,
    drop_raw_panic_waker,
);

fn drop_panic_waker(state: &Arc<DropPanicState>) -> Waker {
    let data = Arc::into_raw(Arc::clone(state)).cast();
    // SAFETY: DROP_PANIC_WAKER_VTABLE maintains one Arc reference per waker.
    unsafe { Waker::from_raw(RawWaker::new(data, &DROP_PANIC_WAKER_VTABLE)) }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReentryResult {
    WouldDeadlock,
    Pending,
    OtherReady,
}

struct ReentryWakerState {
    fired: AtomicBool,
    subscription: Arc<AsyncSubscription<u32>>,
    result: Mutex<Option<ReentryResult>>,
}

unsafe fn clone_reentry_waker(data: *const ()) -> RawWaker {
    // SAFETY: Every raw pointer is an owned Arc<ReentryWakerState>.
    unsafe { Arc::<ReentryWakerState>::increment_strong_count(data.cast()) };
    RawWaker::new(data, &REENTRY_WAKER_VTABLE)
}

unsafe fn wake_reentry_waker(data: *const ()) {
    // SAFETY: wake consumes this RawWaker's owned Arc reference.
    drop(unsafe { Arc::<ReentryWakerState>::from_raw(data.cast()) });
}

unsafe fn wake_reentry_waker_by_ref(_: *const ()) {}

unsafe fn drop_reentry_waker(data: *const ()) {
    // SAFETY: drop consumes this RawWaker's owned Arc reference.
    let state = unsafe { Arc::<ReentryWakerState>::from_raw(data.cast()) };
    if !state.fired.swap(true, Ordering::AcqRel) {
        let mut run = Box::pin(state.subscription.run(|_| async { Ok::<(), DeliveryError>(()) }));
        let result = match poll_once(run.as_mut()) {
            Poll::Pending => ReentryResult::Pending,
            Poll::Ready(Err(qubit_event_bus::ReceiveError::WouldDeadlock)) => ReentryResult::WouldDeadlock,
            Poll::Ready(_) => ReentryResult::OtherReady,
        };
        *state.result.lock().expect("probe result lock") = Some(result);
    }
}

static REENTRY_WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_reentry_waker,
    wake_reentry_waker,
    wake_reentry_waker_by_ref,
    drop_reentry_waker,
);

fn reentry_waker(subscription: Arc<AsyncSubscription<u32>>) -> (Waker, Arc<ReentryWakerState>) {
    let state = Arc::new(ReentryWakerState {
        fired: AtomicBool::new(false),
        subscription,
        result: Mutex::new(None),
    });
    let data = Arc::into_raw(Arc::clone(&state)).cast();
    // SAFETY: REENTRY_WAKER_VTABLE maintains one Arc reference per RawWaker.
    (
        unsafe { Waker::from_raw(RawWaker::new(data, &REENTRY_WAKER_VTABLE)) },
        state,
    )
}

#[test]
fn test_lease_registration_reentry_and_waiter_handoff() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let provider: Arc<dyn AsyncEventBusSpi> = spi.clone();
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").expect("provider"), provider)
        .expect("valid provider capabilities");
    let topic = Topic::<u32>::new("lease-registration-reentry").expect("topic");

    block_on(async {
        let subscription = Arc::new(
            bus.subscribe(SubscribeRequest::new("lease-reentry", topic).expect("request"))
                .await
                .expect("subscription"),
        );
        let (outer_waker, outer_state) = reentry_waker(Arc::clone(&subscription));
        let mut outer_context = Context::from_waker(&outer_waker);
        let mut outer = Box::pin(subscription.run(|_| async { Ok::<(), DeliveryError>(()) }));
        assert!(matches!(
            Future::poll(Pin::as_mut(&mut outer), &mut outer_context),
            Poll::Pending
        ));
        assert_eq!(
            Some(ReentryResult::WouldDeadlock),
            *outer_state.result.lock().expect("probe result lock")
        );

        let (old_waker, old_state) = reentry_waker(Arc::clone(&subscription));
        let (new_waker, new_state) = reentry_waker(Arc::clone(&subscription));
        let mut old_context = Context::from_waker(&old_waker);
        let mut new_context = Context::from_waker(&new_waker);
        let mut waiting = Box::pin(subscription.run(|_| async { Ok::<(), DeliveryError>(()) }));
        assert!(matches!(
            Future::poll(Pin::as_mut(&mut waiting), &mut old_context),
            Poll::Pending
        ));
        assert!(matches!(
            Future::poll(Pin::as_mut(&mut waiting), &mut new_context),
            Poll::Pending
        ));
        assert_eq!(
            Some(ReentryResult::WouldDeadlock),
            *old_state.result.lock().expect("probe result lock")
        );

        drop(waiting);
        assert_eq!(
            Some(ReentryResult::WouldDeadlock),
            *new_state.result.lock().expect("probe result lock")
        );
        drop(outer);
        assert_eq!(
            qubit_event_bus::AsyncSubscriptionRunState::Paused,
            subscription.run_state()
        );

        let mut successor = Box::pin(subscription.run(|_| async { Ok::<(), DeliveryError>(()) }));
        assert!(matches!(poll_once(successor.as_mut()), Poll::Pending));
        assert_eq!(
            qubit_event_bus::AsyncSubscriptionRunState::Running,
            subscription.run_state()
        );
        drop(successor);
        let _report = bus.shutdown(ShutdownMode::Immediate).await.expect("shutdown");
        drop(outer_waker);
        drop(old_waker);
        drop(new_waker);
    });
}

#[test]
fn test_run_handoff_contains_waker_drop_panics_and_preserves_session() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let provider: Arc<dyn AsyncEventBusSpi> = spi.clone();
    let bus = AsyncEventBus::from_spi(ProviderId::new("fake").expect("provider"), provider)
        .expect("valid provider capabilities");
    let topic = Topic::<u32>::new("session-waker-drop").expect("topic");

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new("waker-drop", topic.clone()).expect("request"))
            .await
            .expect("subscription");

        let mut first_runner = Box::pin(subscription.run(|_| async { Ok::<(), DeliveryError>(()) }));
        assert!(matches!(poll_once(first_runner.as_mut()), Poll::Pending));

        let resumed_calls = Arc::new(AtomicUsize::new(0));
        let resumed_calls_in_runner = Arc::clone(&resumed_calls);
        let allow_resumed_completion = Arc::new(AtomicBool::new(false));
        let allow_completion_in_runner = Arc::clone(&allow_resumed_completion);
        let mut waiting_runner = Box::pin(subscription.run(move |_| {
            resumed_calls_in_runner.fetch_add(1, Ordering::AcqRel);
            let allow_completion = Arc::clone(&allow_completion_in_runner);
            async move {
                std::future::poll_fn(|cx| {
                    if allow_completion.load(Ordering::Acquire) {
                        Poll::Ready(Ok(()))
                    } else {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await
            }
        }));

        let old_state = Arc::new(DropPanicState {
            panic_on_drop: AtomicBool::new(true),
            drops: AtomicUsize::new(0),
            wakes: AtomicUsize::new(0),
        });
        let old_waker = drop_panic_waker(&old_state);
        let mut old_context = Context::from_waker(&old_waker);
        assert!(matches!(
            Future::poll(Pin::as_mut(&mut waiting_runner), &mut old_context),
            Poll::Pending
        ));

        let new_state = Arc::new(DropPanicState {
            panic_on_drop: AtomicBool::new(false),
            drops: AtomicUsize::new(0),
            wakes: AtomicUsize::new(0),
        });
        let new_waker = drop_panic_waker(&new_state);
        let mut new_context = Context::from_waker(&new_waker);
        assert!(matches!(
            Future::poll(Pin::as_mut(&mut waiting_runner), &mut new_context),
            Poll::Pending
        ));
        assert_eq!(1, old_state.drops.load(Ordering::Relaxed));

        drop(first_runner);
        assert_eq!(1, new_state.wakes.load(Ordering::Relaxed));
        let _receipt = bus
            .publish(PublishRequest::new(topic, 7).expect("publish request"))
            .await
            .expect("publish");
        new_state.panic_on_drop.store(true, Ordering::Relaxed);
        for attempt in 0..100 {
            let resumed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                Future::poll(Pin::as_mut(&mut waiting_runner), &mut new_context)
            }));
            assert!(matches!(
                resumed.expect("waker cleanup panic is contained"),
                Poll::Pending
            ));
            if attempt == 0 {
                new_state.panic_on_drop.store(false, Ordering::Relaxed);
            }
            if resumed_calls.load(Ordering::Acquire) == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(1, resumed_calls.load(Ordering::Acquire));

        allow_resumed_completion.store(true, Ordering::Release);
        old_state.panic_on_drop.store(false, Ordering::Relaxed);
        new_state.panic_on_drop.store(false, Ordering::Relaxed);
        drop(waiting_runner);
        drop(old_waker);
        drop(new_waker);
        let mut subscription = subscription;
        subscription.close().await.expect("close resumed session");
        let _report = bus.shutdown(ShutdownMode::Immediate).await.expect("shutdown bus");
    });
}
