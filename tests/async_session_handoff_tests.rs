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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;
use std::time::Duration;

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
        let mut waiting_runner = Box::pin(subscription.run(move |_| {
            resumed_calls_in_runner.fetch_add(1, Ordering::AcqRel);
            async { std::future::pending::<Result<(), DeliveryError>>().await }
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
        let resumed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Future::poll(Pin::as_mut(&mut waiting_runner), &mut new_context)
        }));
        assert!(matches!(
            resumed.expect("waker cleanup panic is contained"),
            Poll::Pending
        ));
        assert_eq!(1, resumed_calls.load(Ordering::Acquire));

        new_state.panic_on_drop.store(false, Ordering::Relaxed);
        drop(waiting_runner);
        drop(old_waker);
        drop(new_waker);
        let mut subscription = subscription;
        subscription.close().await.expect("close resumed session");
        let _report = bus.shutdown(ShutdownMode::Immediate).await.expect("shutdown bus");
    });
}
