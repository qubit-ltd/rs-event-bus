// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================
//! Session lease handoff regressions for canceled asynchronous runners.

mod support;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
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
