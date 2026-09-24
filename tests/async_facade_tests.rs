// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-neutral asynchronous facade contract tests.

mod support;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::SystemTime;

use qubit_event_bus::CodecError;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::Diagnostic;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::facade::AsyncEventBus;
use qubit_event_bus::facade::DeliveryAdmissionConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::AckMode;
use qubit_event_bus::model::AsyncSubscriberNext;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::InboundMessage;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_retry::RetryPolicy;
use support::fake_spi::FakeAsyncEventBusSpi;
use support::manual_async::block_on;

fn topic() -> Topic<u32> {
    Topic::new("test.topic").unwrap()
}

struct WakeOnSignal(std::sync::mpsc::Sender<()>);

impl std::task::Wake for WakeOnSignal {
    fn wake(self: Arc<Self>) {
        let _ = self.0.send(());
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let _ = self.0.send(());
    }
}

#[test]
fn async_facade_publishes_single_and_ordered_batch_without_runtime_dependency() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());

    block_on(async {
        let request = PublishRequest::new(topic(), 7).unwrap();
        let input_event_id = request.envelope().id().clone();
        let receipt = bus.publish(request).await.unwrap();
        assert_eq!(receipt.input_event_id(), &input_event_id);

        let batch = bus
            .publish_all([
                PublishRequest::new(topic(), 11).unwrap(),
                PublishRequest::new(topic(), 12).unwrap(),
            ])
            .await;
        assert_eq!(batch.total_count(), 2);
        assert_eq!(batch.accepted_count(), 2);

        let string_topic = Topic::<String>::new("test.string.topic").unwrap();
        let string_batch = bus
            .publish_all([
                PublishRequest::new(string_topic.clone(), String::from("first")).unwrap(),
                PublishRequest::new(string_topic.clone(), String::from("second")).unwrap(),
            ])
            .await;
        assert_eq!(string_batch.total_count(), 2);
        assert_eq!(string_batch.accepted_count(), 2);

        spi.fail_next_publish();
        let failed_options = qubit_event_bus::model::PublishOptions::<String>::builder()
            .retry_policy(RetryPolicy::builder().max_attempts(1).build().unwrap())
            .build();
        let mixed_batch = bus
            .publish_all([
                PublishRequest::new(string_topic.clone(), String::from("rejected"))
                    .unwrap()
                    .with_options(failed_options),
                PublishRequest::new(string_topic, String::from("accepted")).unwrap(),
            ])
            .await;
        assert_eq!(mixed_batch.total_count(), 2);
        assert_eq!(mixed_batch.accepted_count(), 1);
        assert_eq!(mixed_batch.failure_count(), 1);
        assert!(mixed_batch.items()[0].is_err());
        assert!(mixed_batch.items()[1].is_ok());

        let outcome = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        assert_eq!(outcome, ShutdownOutcome::Complete);
    });

    assert_eq!(spi.shutdown_transition_count(), 1);
    assert_eq!(spi.operation_log().iter().filter(|op| **op == "publish").count(), 7);
}

#[test]
fn async_facade_publisher_interceptor_can_drop_without_spi_publish() {
    use qubit_event_bus::facade::EventBusFacadeConfig;
    use qubit_event_bus::model::PublishMetadata;

    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let typed_order = order.clone();
    let global_order = order.clone();
    let config = EventBusFacadeConfig::new().publisher_interceptor(move |metadata: &mut PublishMetadata| {
        global_order.lock().unwrap().push("global");
        assert_eq!(metadata.header("origin"), Some("typed"));
        metadata.set_header("trace", "global")?;
        Ok(false)
    });
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config);
    let options = qubit_event_bus::model::PublishOptions::<u32>::builder()
        .interceptor(move |mut envelope| {
            typed_order.lock().unwrap().push("typed");
            envelope.set_header("origin", "typed").expect("valid header");
            Ok(Some(envelope))
        })
        .build();

    block_on(async {
        let receipt = bus
            .publish(PublishRequest::new(topic(), 17).unwrap().with_options(options))
            .await
            .unwrap();
        assert!(receipt.acknowledgement().is_dropped());
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });

    assert_eq!(*order.lock().unwrap(), ["typed", "global"]);
    assert!(!spi.operation_log().contains(&"publish"));
}

#[test]
fn async_terminal_publish_retry_error_retains_reason_attempt_and_spi_source() {
    use qubit_event_bus::error::PublishError;
    use qubit_event_bus::model::PublishOptions;
    use qubit_retry::AttemptFailure;
    use qubit_retry::RetryContext;
    use qubit_retry::RetryDecision;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.fail_next_publish();
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let options = PublishOptions::<u32>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(1).build().unwrap())
        .retry_rule(
            |_: &AttemptFailure<qubit_event_bus::error::PublishAttemptError>, _: &RetryContext| RetryDecision::Retry,
        )
        .build();

    block_on(async {
        let error = bus
            .publish(PublishRequest::new(topic(), 91).unwrap().with_options(options))
            .await
            .unwrap_err();
        let PublishError::Retry(retry) = error else {
            panic!("typed RetryError must reach async facade caller");
        };
        assert!(matches!(
            retry.reason(),
            qubit_retry::RetryErrorReason::Exhausted { .. }
        ));
        assert_eq!(retry.context().attempts(), 1);
        let attempt_error = retry.last_error().expect("attempt error retained");
        assert_eq!(attempt_error.kind(), "fake_failure");
        let mut source = std::error::Error::source(attempt_error);
        let mut found = false;
        while let Some(error) = source {
            if error.to_string().contains("fake") {
                found = true;
                break;
            }
            source = error.source();
        }
        assert!(found, "provider source chain must survive terminal retry mapping");
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });
}

#[test]
fn async_subscription_is_created_without_spawning_until_run_is_driven() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let request = SubscribeRequest::new(SubscriberId::new("async-test").unwrap(), topic());

    block_on(async {
        let subscription = bus.subscribe(request).await.unwrap();
        assert_eq!(spi.operation_log(), ["subscribe"]);
        assert_eq!(subscription.subscriber_id().as_str(), "async-test");
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });
    assert_eq!(spi.operation_log(), ["subscribe", "close", "shutdown"]);
}

#[test]
fn async_facade_bounds_in_flight_deliveries_across_subscriptions() {
    use std::task::Waker;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let config = EventBusFacadeConfig::new().with_delivery_admission(DeliveryAdmissionConfig::new(1).unwrap());
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi, config);
    let shared = Arc::new((
        std::sync::Mutex::new((false, false, 0_usize, Vec::<Waker>::new())),
        std::sync::Condvar::new(),
    ));

    let (mut first, mut second) = block_on(async {
        let first = bus
            .subscribe(SubscribeRequest::new(SubscriberId::new("first").unwrap(), topic()))
            .await
            .unwrap();
        let second = bus
            .subscribe(SubscribeRequest::new(SubscriberId::new("second").unwrap(), topic()))
            .await
            .unwrap();
        bus.publish(PublishRequest::new(topic(), 5).unwrap()).await.unwrap();
        (first, second)
    });

    let first_state = shared.clone();
    let first_runner = std::thread::spawn(move || {
        block_on(first.run(move |_| {
            let state = first_state.clone();
            async move {
                let (lock, changed) = &*state;
                {
                    let mut state = lock.lock().unwrap();
                    state.2 += 1;
                    state.1 = true;
                    changed.notify_all();
                }
                std::future::poll_fn(move |context| {
                    let mut state = lock.lock().unwrap();
                    if state.0 {
                        return std::task::Poll::Ready(());
                    }
                    state.3.push(context.waker().clone());
                    std::task::Poll::Pending
                })
                .await;
                Ok::<(), DeliveryError>(())
            }
        }))
    });

    let second_state = shared.clone();
    let second_runner = std::thread::spawn(move || {
        block_on(second.run(move |_| {
            let state = second_state.clone();
            async move {
                let (lock, changed) = &*state;
                {
                    let mut state = lock.lock().unwrap();
                    state.2 += 1;
                    changed.notify_all();
                }
                std::future::poll_fn(move |context| {
                    let mut state = lock.lock().unwrap();
                    if state.0 {
                        return std::task::Poll::Ready(());
                    }
                    state.3.push(context.waker().clone());
                    std::task::Poll::Pending
                })
                .await;
                Ok::<(), DeliveryError>(())
            }
        }))
    });

    let (lock, changed) = &*shared;
    let state = lock.lock().unwrap();
    let (state, _) = changed
        .wait_timeout_while(state, std::time::Duration::from_millis(100), |state| !state.1)
        .unwrap();
    drop(state);
    std::thread::sleep(std::time::Duration::from_millis(30));
    let observed_concurrency = shared.0.lock().unwrap().2;

    {
        let (lock, _) = &*shared;
        let mut state = lock.lock().unwrap();
        state.0 = true;
        let wakers = std::mem::take(&mut state.3);
        drop(state);
        for waker in wakers {
            waker.wake();
        }
    }
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    first_runner.join().unwrap().unwrap();
    second_runner.join().unwrap().unwrap();
    assert_eq!(observed_concurrency, 1, "bus-wide cap must include all subscriptions");
}

#[test]
fn async_admission_waiter_keeps_polling_existing_tasks_until_a_slot_is_released() {
    use std::task::Waker;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let config = EventBusFacadeConfig::new().with_delivery_admission(DeliveryAdmissionConfig::new(1).unwrap());
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi, config);
    let mut subscription = block_on(bus.subscribe(SubscribeRequest::new(
        SubscriberId::new("admission-progress").unwrap(),
        topic(),
    )))
    .unwrap();
    block_on(async {
        bus.publish(PublishRequest::new(topic(), 1).unwrap()).await.unwrap();
        bus.publish(PublishRequest::new(topic(), 2).unwrap()).await.unwrap();
    });
    let state = Arc::new(std::sync::Mutex::new((false, 0_usize, Vec::<Waker>::new())));
    let handler_state = state.clone();
    let runner = std::thread::spawn(move || {
        block_on(subscription.run(move |_| {
            let state = handler_state.clone();
            async move {
                let call = {
                    let mut state = state.lock().unwrap();
                    state.1 += 1;
                    state.1
                };
                if call == 1 {
                    std::future::poll_fn(move |context| {
                        let mut state = state.lock().unwrap();
                        if state.0 {
                            return std::task::Poll::Ready(Ok::<(), DeliveryError>(()));
                        }
                        state.2.push(context.waker().clone());
                        std::task::Poll::Pending
                    })
                    .await?;
                }
                Ok::<(), DeliveryError>(())
            }
        }))
    });
    for _ in 0..100 {
        if state.lock().unwrap().1 == 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    assert_eq!(state.lock().unwrap().1, 1);
    let wakers = {
        let mut state = state.lock().unwrap();
        state.0 = true;
        std::mem::take(&mut state.2)
    };
    for waker in wakers {
        waker.wake();
    }
    for _ in 0..100 {
        if state.lock().unwrap().1 == 2 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    let second_started = state.lock().unwrap().1 == 2;
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    runner.join().unwrap().unwrap();
    assert!(
        second_started,
        "the active task must make progress while another delivery awaits admission"
    );
}

#[test]
fn idle_async_subscription_does_not_consume_delivery_admission() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let config = EventBusFacadeConfig::new().with_delivery_admission(DeliveryAdmissionConfig::new(1).unwrap());
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config);
    let (mut active, mut idle) = block_on(async {
        let active = bus
            .subscribe(SubscribeRequest::new(SubscriberId::new("active").unwrap(), topic()))
            .await
            .unwrap();
        let idle = bus
            .subscribe(SubscribeRequest::new(SubscriberId::new("idle").unwrap(), topic()))
            .await
            .unwrap();
        bus.publish(PublishRequest::new(topic(), 7).unwrap()).await.unwrap();
        (active, idle)
    });
    let active_started = Arc::new(AtomicBool::new(false));
    let active_started_from_runner = active_started.clone();
    let active_runner = std::thread::spawn(move || {
        block_on(active.run(move |_| {
            active_started_from_runner.store(true, Ordering::Release);
            async { Ok::<(), DeliveryError>(()) }
        }))
    });
    let idle_runner = std::thread::spawn(move || block_on(idle.run(|_| async { Ok::<(), DeliveryError>(()) })));

    for _ in 0..100 {
        if active_started.load(Ordering::Acquire) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    let was_dispatched = active_started.load(Ordering::Acquire);
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    active_runner.join().unwrap().unwrap();
    idle_runner.join().unwrap().unwrap();
    assert!(
        was_dispatched,
        "an idle receive must not hold a facade-wide delivery slot"
    );
}

#[test]
fn async_subscription_runs_different_ordering_keys_concurrently() {
    use std::task::Waker;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let options = SubscribeOptions::<u32>::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let mut subscription =
        block_on(bus.subscribe(
            SubscribeRequest::new(SubscriberId::new("parallel-keys").unwrap(), topic()).with_options(options),
        ))
        .unwrap();
    for (payload, key) in [(1, "left"), (2, "right")] {
        block_on(
            bus.publish(
                PublishRequest::builder()
                    .topic(topic())
                    .payload(payload)
                    .ordering_key(key)
                    .build()
                    .unwrap(),
            ),
        )
        .unwrap();
    }
    let state = Arc::new(std::sync::Mutex::new((false, 0_usize, Vec::<Waker>::new())));
    let handler_state = state.clone();
    let runner = std::thread::spawn(move || {
        block_on(subscription.run(move |_| {
            let state = handler_state.clone();
            async move {
                {
                    state.lock().unwrap().1 += 1;
                }
                std::future::poll_fn(move |context| {
                    let mut state = state.lock().unwrap();
                    if state.0 {
                        return std::task::Poll::Ready(());
                    }
                    state.2.push(context.waker().clone());
                    std::task::Poll::Pending
                })
                .await;
                Ok::<(), DeliveryError>(())
            }
        }))
    });

    std::thread::sleep(std::time::Duration::from_millis(100));
    let observed = state.lock().unwrap().1;
    let wakers = {
        let mut state = state.lock().unwrap();
        state.0 = true;
        std::mem::take(&mut state.2)
    };
    for waker in wakers {
        waker.wake();
    }
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    runner.join().unwrap().unwrap();
    assert_eq!(observed, 2, "different ordering keys should make progress in parallel");
}

#[test]
fn async_subscription_preserves_order_for_the_same_ordering_key() {
    use std::task::Waker;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi);
    let options = SubscribeOptions::<u32>::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let mut subscription = block_on(
        bus.subscribe(SubscribeRequest::new(SubscriberId::new("serial-key").unwrap(), topic()).with_options(options)),
    )
    .unwrap();
    for payload in [1, 2] {
        block_on(
            bus.publish(
                PublishRequest::builder()
                    .topic(topic())
                    .payload(payload)
                    .ordering_key("same")
                    .build()
                    .unwrap(),
            ),
        )
        .unwrap();
    }
    let state = Arc::new(std::sync::Mutex::new((false, 0_usize, Vec::<Waker>::new())));
    let handler_state = state.clone();
    let runner = std::thread::spawn(move || {
        block_on(subscription.run(move |_| {
            let state = handler_state.clone();
            async move {
                state.lock().unwrap().1 += 1;
                std::future::poll_fn(move |context| {
                    let mut state = state.lock().unwrap();
                    if state.0 {
                        return std::task::Poll::Ready(());
                    }
                    state.2.push(context.waker().clone());
                    std::task::Poll::Pending
                })
                .await;
                Ok::<(), DeliveryError>(())
            }
        }))
    });
    std::thread::sleep(std::time::Duration::from_millis(50));
    let observed = state.lock().unwrap().1;
    let wakers = {
        let mut state = state.lock().unwrap();
        state.0 = true;
        std::mem::take(&mut state.2)
    };
    for waker in wakers {
        waker.wake();
    }
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    runner.join().unwrap().unwrap();
    assert_eq!(
        observed, 1,
        "a later message with the same key must wait for its predecessor"
    );
}

#[test]
fn immediate_shutdown_drops_same_key_lane_waiters_but_finishes_started_handler() {
    use std::task::Waker;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let options = SubscribeOptions::<u32>::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let mut subscription = block_on(bus.subscribe(
        SubscribeRequest::new(SubscriberId::new("immediate-lane-waiter").unwrap(), topic()).with_options(options),
    ))
    .unwrap();
    for payload in [1, 2] {
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            format!("immediate-lane-{payload}"),
        ))));
    }
    let state = Arc::new(std::sync::Mutex::new((false, 0_usize, Vec::<Waker>::new())));
    let handler_state = state.clone();
    let runner = std::thread::spawn(move || {
        block_on(subscription.run(move |_| {
            let state = handler_state.clone();
            async move {
                state.lock().unwrap().1 += 1;
                std::future::poll_fn(move |context| {
                    let mut state = state.lock().unwrap();
                    if state.0 {
                        return std::task::Poll::Ready(Ok(()));
                    }
                    state.2.push(context.waker().clone());
                    std::task::Poll::Pending
                })
                .await
            }
        }))
    });

    for _ in 0..100 {
        if state.lock().unwrap().1 > 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    assert_eq!(state.lock().unwrap().1, 1);

    let shutdown_bus = bus.clone();
    let shutdown = std::thread::spawn(move || block_on(shutdown_bus.shutdown(ShutdownMode::Immediate)));
    std::thread::sleep(std::time::Duration::from_millis(30));
    assert_eq!(state.lock().unwrap().1, 1, "Immediate must not start lane-waiting work");
    let wakers = {
        let mut state = state.lock().unwrap();
        state.0 = true;
        std::mem::take(&mut state.2)
    };
    for waker in wakers {
        waker.wake();
    }
    shutdown.join().unwrap().unwrap();
    runner.join().unwrap().unwrap();
    assert_eq!(
        spi.settlement_count(),
        1,
        "only the started delivery is settled before receiver close"
    );
    let operations = spi.operation_log();
    assert!(
        operations.contains(&"close"),
        "receiver close delegates recovery of unsettled delivery to the SPI"
    );
}

#[test]
fn immediate_shutdown_does_not_start_lane_waiter_when_predecessor_finishes() {
    use std::task::Poll;
    use std::task::Waker;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let options = SubscribeOptions::<u32>::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let mut subscription = block_on(bus.subscribe(
        SubscribeRequest::new(SubscriberId::new("immediate-lane-race").unwrap(), topic()).with_options(options),
    ))
    .unwrap();
    for payload in [1_u32, 2_u32] {
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            format!("immediate-race-{payload}"),
        ))));
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let release_first = Arc::new(AtomicBool::new(false));
    let waiters = Arc::new(std::sync::Mutex::new(Vec::<Waker>::new()));
    let calls_for_handler = calls.clone();
    let release_for_handler = release_first.clone();
    let waiters_for_handler = waiters.clone();
    let mut run = Box::pin(subscription.run(move |_| {
        let call = calls_for_handler.fetch_add(1, Ordering::AcqRel);
        let release = release_for_handler.clone();
        let waiters = waiters_for_handler.clone();
        async move {
            if call == 0 {
                std::future::poll_fn(move |context| {
                    if release.load(Ordering::Acquire) {
                        Poll::Ready(Ok(()))
                    } else {
                        waiters.lock().unwrap().push(context.waker().clone());
                        Poll::Pending
                    }
                })
                .await
            } else {
                Ok(())
            }
        }
    }));
    for _ in 0..100 {
        let _ = support::manual_async::poll_once(run.as_mut());
        if calls.load(Ordering::Acquire) == 1 && spi.operation_log().iter().filter(|op| **op == "receive").count() >= 2
        {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(calls.load(Ordering::Acquire), 1);
    drop(run);

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(matches!(
        support::manual_async::poll_once(shutdown.as_mut()),
        Poll::Pending
    ));
    release_first.store(true, Ordering::Release);
    for waker in std::mem::take(&mut *waiters.lock().unwrap()) {
        waker.wake();
    }
    let mut outcome = None;
    for _ in 0..100 {
        if let Poll::Ready(result) = support::manual_async::poll_once(shutdown.as_mut()) {
            outcome = Some(result);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    outcome
        .expect("shutdown should finish after the started handler exits")
        .unwrap();
    assert_eq!(
        calls.load(Ordering::Acquire),
        1,
        "queued lane work must not start after Immediate"
    );
    assert_eq!(spi.settlement_count(), 1, "only the started delivery should be settled");
}

#[test]
fn shutdown_skips_an_unstarted_subscription_dropped_by_its_owner() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());

    block_on(async {
        let subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("dropped-before-run").unwrap(),
                topic(),
            ))
            .await
            .unwrap();
        drop(subscription);
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });

    assert_eq!(spi.operation_log(), ["subscribe", "shutdown"]);
}

#[test]
fn cancelling_shutdown_while_unstarted_receiver_close_is_pending_allows_retry() {
    use std::task::Poll;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.pause_close();
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());

    block_on(async {
        let _subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("cancel-pending-close").unwrap(),
                topic(),
            ))
            .await
            .unwrap();

        let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
        assert!(matches!(
            support::manual_async::poll_once(shutdown.as_mut()),
            Poll::Pending
        ));
        assert_eq!(spi.operation_log(), ["subscribe", "close"]);
        drop(shutdown);

        spi.release_close();
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });

    assert_eq!(spi.operation_log(), ["subscribe", "close", "close", "shutdown"]);
}

#[test]
fn graceful_shutdown_timeout_bounds_pending_unstarted_receiver_close() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.pause_close();
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let timeout = std::time::Duration::from_millis(100);

    block_on(async {
        let _subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("graceful-pending-close").unwrap(),
                topic(),
            ))
            .await
            .unwrap();

        assert!(matches!(
            bus.shutdown(ShutdownMode::Graceful { timeout }).await,
            Err(qubit_event_bus::ShutdownError::TimedOut { timeout: elapsed }) if elapsed == timeout
        ));
        assert_eq!(spi.operation_log(), ["subscribe", "close"]);

        spi.release_close();
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });

    assert_eq!(spi.operation_log(), ["subscribe", "close", "close", "shutdown"]);
}

#[test]
fn shutdown_waits_for_in_flight_subscribe_to_close_late_receiver_first() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.pause_subscribe();
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let (subscribe_sender, subscribe_receiver) = std::sync::mpsc::channel();
    let subscribing_bus = bus.clone();
    let subscriber = std::thread::spawn(move || {
        let result = block_on(subscribing_bus.subscribe(SubscribeRequest::new(
            SubscriberId::new("subscribe-shutdown-race").unwrap(),
            topic(),
        )));
        let _ = subscribe_sender.send(result);
    });
    for _ in 0..100 {
        if spi.operation_log().contains(&"subscribe") {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    assert!(spi.operation_log().contains(&"subscribe"));

    let (shutdown_sender, shutdown_receiver) = std::sync::mpsc::channel();
    let shutdown_bus = bus.clone();
    let shutdown_thread = std::thread::spawn(move || {
        let result = block_on(shutdown_bus.shutdown(ShutdownMode::Immediate));
        let _ = shutdown_sender.send(result);
    });
    let early_shutdown = shutdown_receiver
        .recv_timeout(std::time::Duration::from_millis(30))
        .ok();
    let shutdown_was_early = early_shutdown.is_some();
    spi.release_subscribe();

    let subscribe_result = subscribe_receiver
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    let shutdown_result = early_shutdown.unwrap_or_else(|| {
        shutdown_receiver
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap()
    });
    subscriber.join().unwrap();
    shutdown_thread.join().unwrap();

    assert!(matches!(subscribe_result, Err(qubit_event_bus::SubscribeError::Closed)));
    assert_eq!(shutdown_result.unwrap(), ShutdownOutcome::Complete);
    assert!(
        !shutdown_was_early,
        "provider shutdown must wait for in-flight subscribe cleanup"
    );
    let operations = spi.operation_log();
    let close = operations.iter().position(|operation| *operation == "close").unwrap();
    let shutdown = operations
        .iter()
        .position(|operation| *operation == "shutdown")
        .unwrap();
    assert!(close < shutdown, "late receiver must close before provider shutdown");
}

#[test]
fn cancelling_pending_subscribe_releases_admission_for_shutdown() {
    use std::task::Poll;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.pause_subscribe();
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let request = SubscribeRequest::new(SubscriberId::new("cancel-pending-subscribe").unwrap(), topic());
    let mut subscribe = Box::pin(bus.subscribe(request));
    assert!(matches!(
        support::manual_async::poll_once(subscribe.as_mut()),
        Poll::Pending
    ));
    drop(subscribe);

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    let outcome = match support::manual_async::poll_once(shutdown.as_mut()) {
        Poll::Ready(result) => result.unwrap(),
        Poll::Pending => panic!("cancelled subscribe must release its admission guard"),
    };
    assert_eq!(outcome, ShutdownOutcome::Complete);
    assert_eq!(spi.shutdown_transition_count(), 1);
    assert!(
        !spi.operation_log().contains(&"close"),
        "no receiver was returned to the facade"
    );
}

#[test]
fn asynchronous_facade_rejects_sync_subscriber_interceptors_before_provider_subscription() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let options = SubscribeOptions::<u32>::builder()
        .interceptor(|delivery, next| next(delivery))
        .build();
    let request =
        SubscribeRequest::new(SubscriberId::new("sync-only-interceptor").unwrap(), topic()).with_options(options);

    let error = match block_on(bus.subscribe(request)) {
        Ok(_) => panic!("async facade must reject sync middleware"),
        Err(error) => error,
    };
    match error {
        qubit_event_bus::error::SubscribeError::Configuration(
            qubit_event_bus::error::ConfigurationError::InvalidField { field, .. },
        ) => assert_eq!(field, "sync_subscriber_interceptor"),
        other => panic!("expected runtime-model configuration error, got {other}"),
    }
    assert!(spi.operation_log().is_empty());
}

#[test]
fn async_handler_cannot_await_either_shutdown_mode_on_its_own_bus() {
    use std::future::Future;
    use std::task::Poll;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let observed = Arc::new(AtomicBool::new(false));

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("self-shutdown").unwrap(),
                topic(),
            ))
            .await
            .unwrap();
        spi.enqueue(support::fake_spi::inbound_message(None));
        let observed_by_handler = observed.clone();
        let bus_by_handler = bus.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let bus = bus_by_handler.clone();
                let observed = observed_by_handler.clone();
                async move {
                    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful {
                        timeout: std::time::Duration::from_secs(60),
                    }));
                    let mut immediate = Box::pin(bus.shutdown(ShutdownMode::Immediate));
                    std::future::poll_fn(move |context| {
                        let would_deadlock = |result| {
                            matches!(
                                result,
                                Poll::Ready(Err(qubit_event_bus::ShutdownError::Lifecycle(
                                    qubit_event_bus::LifecycleError::WouldDeadlock { operation: "shutdown" }
                                )))
                            )
                        };
                        let graceful_rejected = would_deadlock(graceful.as_mut().poll(context));
                        let immediate_rejected = would_deadlock(immediate.as_mut().poll(context));
                        observed.store(graceful_rejected && immediate_rejected, Ordering::Release);
                        Poll::Ready(())
                    })
                    .await;
                    Ok(())
                }
            }))
        });
        for _ in 0..100 {
            if observed.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
    assert!(observed.load(Ordering::Acquire));
}

#[test]
fn async_idle_wait_timeout_wakes_without_other_bus_activity() {
    use std::future::Future;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let handler_started = Arc::new(AtomicBool::new(false));
    let release_handler = Arc::new(AtomicBool::new(false));
    let handler_waker = Arc::new(std::sync::Mutex::new(None::<Waker>));
    let delivered_topic = topic();

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("idle-timeout").unwrap(),
                delivered_topic.clone(),
            ))
            .await
            .unwrap();
        spi.enqueue(support::fake_spi::inbound_message(None));
        let started = handler_started.clone();
        let release = release_handler.clone();
        let saved_waker = handler_waker.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let started = started.clone();
                let release = release.clone();
                let saved_waker = saved_waker.clone();
                async move {
                    started.store(true, Ordering::Release);
                    std::future::poll_fn(move |context| {
                        if release.load(Ordering::Acquire) {
                            return Poll::Ready(Ok(()));
                        }
                        *saved_waker.lock().unwrap() = Some(context.waker().clone());
                        Poll::Pending
                    })
                    .await
                }
            }))
        });
        for _ in 0..100 {
            if handler_started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(handler_started.load(Ordering::Acquire));

        let (wake_sender, wake_receiver) = std::sync::mpsc::channel();
        let waker = Waker::from(Arc::new(WakeOnSignal(wake_sender)));
        let mut context = Context::from_waker(&waker);
        let mut waiting = Box::pin(bus.wait_for_idle(&delivered_topic, Some(std::time::Duration::from_millis(15))));
        let first_poll_pending = matches!(waiting.as_mut().poll(&mut context), Poll::Pending);
        let timer_woke = wake_receiver
            .recv_timeout(std::time::Duration::from_millis(200))
            .is_ok();
        let timeout_result = timer_woke.then(|| waiting.as_mut().poll(&mut context));
        drop(waiting);

        release_handler.store(true, Ordering::Release);
        if let Some(waker) = handler_waker.lock().unwrap().take() {
            waker.wake();
        }
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();

        assert!(first_poll_pending);
        assert!(timer_woke, "timeout must wake the waiter without another bus signal");
        assert!(matches!(
            timeout_result,
            Some(Poll::Ready(Ok(qubit_event_bus::WaitOutcome::TimedOut)))
        ));
    });
}

#[test]
fn injected_manual_timer_wakes_idle_timeout_and_cleans_up_waiter() {
    use std::future::Future;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use qubit_clock::ManualMonotonicClock;
    use qubit_clock::MonotonicClock;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = ManualMonotonicClock::new_shared();
    let bus = AsyncEventBus::with_timer(ProviderId::new("fake").unwrap(), spi.clone(), clock.new_timer());
    let delivered_topic = topic();
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let handler_waker = Arc::new(std::sync::Mutex::new(None::<std::task::Waker>));

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("manual-timeout").unwrap(),
                delivered_topic.clone(),
            ))
            .await
            .unwrap();
        spi.enqueue(support::fake_spi::inbound_message(None));
        let started_by_handler = started.clone();
        let release_by_handler = release.clone();
        let waker_by_handler = handler_waker.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let started = started_by_handler.clone();
                let release = release_by_handler.clone();
                let waker = waker_by_handler.clone();
                async move {
                    started.store(true, Ordering::Release);
                    std::future::poll_fn(move |cx| {
                        if release.load(Ordering::Acquire) {
                            return std::task::Poll::Ready(Ok(()));
                        }
                        *waker.lock().unwrap() = Some(cx.waker().clone());
                        std::task::Poll::Pending
                    })
                    .await
                }
            }))
        });
        for _ in 0..100 {
            if started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(started.load(Ordering::Acquire));

        let (cancel_sender, _cancel_receiver) = std::sync::mpsc::channel();
        let cancel_waker = Waker::from(Arc::new(WakeOnSignal(cancel_sender)));
        let mut cancel_context = Context::from_waker(&cancel_waker);
        let mut cancelled_wait =
            Box::pin(bus.wait_for_idle(&delivered_topic, Some(std::time::Duration::from_secs(60))));
        assert!(matches!(
            cancelled_wait.as_mut().poll(&mut cancel_context),
            Poll::Pending
        ));
        assert_eq!(clock.pending_waiters(), 1);
        drop(cancelled_wait);
        assert_eq!(
            clock.pending_waiters(),
            0,
            "dropping a wait must cancel its timer registration"
        );

        let bus_for_wait = bus.clone();
        let topic_for_wait = delivered_topic.clone();
        let waiter = std::thread::spawn(move || {
            block_on(bus_for_wait.wait_for_idle(&topic_for_wait, Some(std::time::Duration::from_secs(30))))
        });
        assert!(clock.wait_for_waiters(1, std::time::Duration::from_secs(1)));
        assert_eq!(clock.pending_waiters(), 1);
        clock.advance(std::time::Duration::from_secs(30)).unwrap();
        assert!(matches!(
            waiter.join().unwrap(),
            Ok(qubit_event_bus::WaitOutcome::TimedOut)
        ));
        assert!(clock.wait_for_waiters(0, std::time::Duration::from_secs(1)));
        assert_eq!(clock.pending_waiters(), 0);

        release.store(true, Ordering::Release);
        if let Some(waker) = handler_waker.lock().unwrap().take() {
            waker.wake();
        }
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn async_failure_with_unsupported_reject_reports_unavailable_without_spi_call() {
    let spi = Arc::new(FakeAsyncEventBusSpi::with_capabilities(
        support::fake_spi::native_no_settlement_capabilities(),
    ));
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let diagnostics = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = diagnostics.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| captured.lock().unwrap().push(diagnostic.clone()));

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new(SubscriberId::new("no-reject").unwrap(), topic()))
            .await
            .unwrap();
        spi.enqueue(InboundMessage::new(
            TopicAddress::new("test.topic").unwrap(),
            EventId::new("unsettled-event").unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Native(Arc::new(5_u32)),
            Some(SettlementToken::new(subscription.id(), "no-reject-token")),
            Default::default(),
        ));
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(|_| async {
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("expected")),
                })
            }))
        });
        for _ in 0..100 {
            if diagnostics
                .lock()
                .unwrap()
                .iter()
                .any(|item| matches!(item, Diagnostic::SettlementUnavailable { .. }))
            {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
    assert_eq!(spi.settlement_count(), 0);
    assert!(diagnostics.lock().unwrap().iter().any(|item| matches!(item,
        Diagnostic::SettlementUnavailable { event_id, requested: qubit_event_bus::spi::DeliveryDisposition::Reject, .. }
            if event_id.as_str() == "unsettled-event"
    )));
}

#[test]
fn async_wrong_settlement_token_is_diagnosed_before_capability_gate() {
    let spi = Arc::new(FakeAsyncEventBusSpi::with_capabilities(
        support::fake_spi::native_no_settlement_capabilities(),
    ));
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let diagnostics = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = diagnostics.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| captured.lock().unwrap().push(diagnostic.clone()));

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("wrong-token").unwrap(),
                topic(),
            ))
            .await
            .unwrap();
        spi.enqueue(InboundMessage::new(
            TopicAddress::new("test.topic").unwrap(),
            EventId::new("wrong-token-event").unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Native(Arc::new(1_u32)),
            Some(SettlementToken::new(qubit_id::Id::new(9_999), "wrong-owner")),
            Default::default(),
        ));
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(|_| async {
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("expected")),
                })
            }))
        });
        for _ in 0..100 {
            if diagnostics.lock().unwrap().iter().any(
                |item| matches!(item, Diagnostic::InternalFailure { origin, .. } if origin.as_ref() == "settlement"),
            ) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
    assert!(diagnostics.lock().unwrap().iter().any(|item| matches!(item,
        Diagnostic::InternalFailure { origin, message } if origin.as_ref() == "settlement" && message.contains("another subscription")
    )));
    assert_eq!(spi.settlement_count(), 0);
}

#[test]
fn inbound_dead_letter_marker_prevents_recursive_async_dead_letter_publish() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let mut headers = Headers::new();
    headers.insert("x-qubit-event-bus-dead-letter".into(), "v1".into());
    let options = SubscribeOptions::builder()
        .error_handler(|_, _| qubit_event_bus::model::FailureDirective::DeadLetter)
        .dead_letter(DeadLetterPolicy::topic("test.dead").unwrap())
        .build();

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new(SubscriberId::new("marked").unwrap(), topic()).with_options(options))
            .await
            .unwrap();
        spi.enqueue(InboundMessage::new(
            TopicAddress::new("test.topic").unwrap(),
            EventId::new("marked-event").unwrap(),
            SystemTime::UNIX_EPOCH,
            headers,
            None,
            TransportPayload::Native(Arc::new(42_u32)),
            Some(SettlementToken::new(subscription.id(), "marked-dead-letter")),
            Default::default(),
        ));
        let observed = Arc::new(AtomicBool::new(false));
        let observed_by_handler = observed.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |delivery: Delivery<u32>| {
                observed_by_handler.store(delivery.context().is_dead_letter(), Ordering::SeqCst);
                async {
                    Err(DeliveryError::Handler {
                        source: Box::new(std::io::Error::other("again")),
                    })
                }
            }))
        });
        for _ in 0..100 {
            if observed.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
        assert!(observed.load(Ordering::SeqCst));
        assert_eq!(spi.operation_log().iter().filter(|call| **call == "publish").count(), 0);
        assert_eq!(
            spi.settlement_dispositions(),
            [qubit_event_bus::spi::DeliveryDisposition::Reject]
        );
    });
}

#[test]
fn async_run_processes_deliveries_on_the_callers_executor_and_shutdown_cancels_receive() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let request = SubscribeRequest::new(SubscriberId::new("async-run").unwrap(), topic());
    let delivered = Arc::new(AtomicUsize::new(0));

    block_on(async {
        let mut subscription = bus.subscribe(request).await.unwrap();
        let delivered_by_handler = delivered.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |delivery| {
                let delivered = delivered_by_handler.clone();
                async move {
                    assert_eq!(*delivery.payload(), 42);
                    delivered.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                }
            }))
        });

        bus.publish(PublishRequest::new(topic(), 42).unwrap()).await.unwrap();
        for _ in 0..100 {
            if delivered.load(Ordering::Acquire) == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(delivered.load(Ordering::Acquire), 1);
        assert_eq!(
            bus.wait_for_idle(&topic(), None).await.unwrap(),
            qubit_event_bus::WaitOutcome::Idle
        );
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn async_middleware_wraps_the_handler_in_registration_order() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = order.clone();
    let options = SubscribeOptions::builder()
        .async_interceptor(move |delivery: Delivery<u32>, next: AsyncSubscriberNext<u32>| {
            let seen = seen.clone();
            Box::pin(async move {
                seen.lock().unwrap().push("before");
                next(delivery).await?;
                seen.lock().unwrap().push("after");
                Ok(())
            }) as SpiFuture<'static, Result<(), DeliveryError>>
        })
        .build();
    let request = SubscribeRequest::new(SubscriberId::new("async-middleware").unwrap(), topic()).with_options(options);

    block_on(async {
        let mut subscription = bus.subscribe(request).await.unwrap();
        let seen_by_handler = order.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let seen = seen_by_handler.clone();
                async move {
                    seen.lock().unwrap().push("handler");
                    Ok(())
                }
            }))
        });
        bus.publish(PublishRequest::new(topic(), 42).unwrap()).await.unwrap();
        for _ in 0..100 {
            if order.lock().unwrap().len() == 3 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
    assert_eq!(*order.lock().unwrap(), ["before", "handler", "after"]);
}

#[test]
fn async_facade_global_middleware_wraps_typed_middleware_and_handler() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let global_calls = calls.clone();
    let config = EventBusFacadeConfig::new().async_subscriber_interceptor(
        move |delivery: Delivery<u32>, next: AsyncSubscriberNext<u32>| {
            let calls = global_calls.clone();
            Box::pin(async move {
                calls.lock().unwrap().push("global-before");
                next(delivery).await?;
                calls.lock().unwrap().push("global-after");
                Ok(())
            }) as SpiFuture<'static, Result<(), DeliveryError>>
        },
    );
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config);
    let typed_calls = calls.clone();
    let options = SubscribeOptions::<u32>::builder()
        .async_interceptor(move |delivery, next| {
            let calls = typed_calls.clone();
            Box::pin(async move {
                calls.lock().unwrap().push("typed-before");
                next(delivery).await?;
                calls.lock().unwrap().push("typed-after");
                Ok(())
            }) as SpiFuture<'static, Result<(), DeliveryError>>
        })
        .build();

    block_on(async {
        let mut subscription = bus
            .subscribe(
                SubscribeRequest::new(SubscriberId::new("async-global-chain").unwrap(), topic()).with_options(options),
            )
            .await
            .unwrap();
        let handler_calls = calls.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let calls = handler_calls.clone();
                async move {
                    calls.lock().unwrap().push("handler");
                    Ok(())
                }
            }))
        });
        bus.publish(PublishRequest::new(topic(), 42).unwrap()).await.unwrap();
        for _ in 0..100 {
            if calls.lock().unwrap().len() == 5 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(
            *calls.lock().unwrap(),
            [
                "global-before",
                "typed-before",
                "handler",
                "typed-after",
                "global-after"
            ]
        );
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn async_facade_rejects_sync_global_subscriber_middleware() {
    let config = EventBusFacadeConfig::new()
        .subscriber_interceptor(|_delivery: Delivery<u32>, _next: qubit_event_bus::model::SubscriberNext<u32>| Ok(()));
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config);
    block_on(async {
        let result = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("bad-async-global").unwrap(),
                topic(),
            ))
            .await;
        assert!(matches!(
            result,
            Err(qubit_event_bus::SubscribeError::Configuration(
                qubit_event_bus::ConfigurationError::InvalidField {
                    field: "sync_subscriber_interceptor",
                    ..
                }
            ))
        ));
        assert!(spi.operation_log().is_empty());
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });
}

#[test]
fn async_handler_future_panic_is_reported_and_does_not_escape_the_runner() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi);
    let failed = Arc::new(AtomicBool::new(false));
    let observed = failed.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::DeliveryFailed { .. }) {
            observed.store(true, Ordering::Release);
        }
    });
    let request = SubscribeRequest::new(SubscriberId::new("async-panic").unwrap(), topic());

    block_on(async {
        let mut subscription = bus.subscribe(request).await.unwrap();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(|_| async move {
                panic!("handler future panic");
                #[allow(unreachable_code)]
                Ok(())
            }))
        });
        bus.publish(PublishRequest::new(topic(), 42).unwrap()).await.unwrap();
        for _ in 0..100 {
            if failed.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(failed.load(Ordering::Acquire));
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn async_retry_reinvokes_the_handler_and_uses_the_configured_qubit_retry_policy() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi);
    let attempts = Arc::new(AtomicUsize::new(0));
    let options = SubscribeOptions::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .error_handler(|_, _| qubit_event_bus::model::FailureDirective::Retry)
        .build();
    let request = SubscribeRequest::new(SubscriberId::new("async-retry").unwrap(), topic()).with_options(options);

    block_on(async {
        let mut subscription = bus.subscribe(request).await.unwrap();
        let attempts_in_handler = attempts.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let attempts = attempts_in_handler.clone();
                async move {
                    if attempts.fetch_add(1, Ordering::AcqRel) == 0 {
                        Err(DeliveryError::Handler {
                            source: Box::new(std::io::Error::other("retry me")),
                        })
                    } else {
                        Ok(())
                    }
                }
            }))
        });
        bus.publish(PublishRequest::new(topic(), 42).unwrap()).await.unwrap();
        for _ in 0..100 {
            if attempts.load(Ordering::Acquire) == 2 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(attempts.load(Ordering::Acquire), 2);
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

/// Verifies asynchronous manual mode accepts only explicit positive
/// acknowledgements.
#[test]
fn async_manual_acknowledgement_requires_an_explicit_ack() {
    for (subscriber, action, expected) in [
        ("async-manual-ack", 1, qubit_event_bus::spi::DeliveryDisposition::Accept),
        ("async-manual-nack", 2, qubit_event_bus::spi::DeliveryDisposition::Retry),
        (
            "async-manual-pending",
            0,
            qubit_event_bus::spi::DeliveryDisposition::Reject,
        ),
    ] {
        let spi = Arc::new(FakeAsyncEventBusSpi::new());
        let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
        let options = SubscribeOptions::<u32>::builder()
            .ack_mode(AckMode::Manual)
            .error_handler(move |_, _| {
                if action == 2 {
                    FailureDirective::Requeue
                } else {
                    FailureDirective::Discard
                }
            })
            .build();
        block_on(async {
            let mut subscription = bus
                .subscribe(SubscribeRequest::new(SubscriberId::new(subscriber).unwrap(), topic()).with_options(options))
                .await
                .unwrap();
            spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
                subscription.id(),
                subscriber,
            ))));
            let runner = std::thread::spawn(move || {
                block_on(subscription.run(move |delivery| async move {
                    match action {
                        1 => delivery.acknowledgement().ack().unwrap(),
                        2 => delivery.acknowledgement().nack().unwrap(),
                        _ => {}
                    }
                    Ok(())
                }))
            });
            for _ in 0..100 {
                if spi.settlement_count() > 0 {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            assert_eq!(spi.settlement_count(), 1);
            assert_eq!(spi.settlement_dispositions(), [expected]);
            bus.shutdown(ShutdownMode::Immediate).await.unwrap();
            runner.join().unwrap().unwrap();
        });
    }
}

/// Verifies asynchronous middleware and error callbacks surround every retry
/// attempt.
#[test]
fn async_interceptor_and_error_handler_wrap_each_failed_retry_attempt() {
    use qubit_retry::AttemptFailure;
    use qubit_retry::RetryContext;
    use qubit_retry::RetryDecision;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let before = calls.clone();
    let after = calls.clone();
    let error = calls.clone();
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_by_handler = attempts.clone();
    let handler_log = calls.clone();
    let options = SubscribeOptions::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .retry_rule(
            |_: &AttemptFailure<qubit_event_bus::error::DeliveryAttemptError>, _: &RetryContext| RetryDecision::Retry,
        )
        .async_interceptor(move |delivery, next: AsyncSubscriberNext<u32>| {
            before.lock().unwrap().push("before");
            let after = after.clone();
            Box::pin(async move {
                let result = next(delivery).await;
                after.lock().unwrap().push("after");
                result
            }) as qubit_event_bus::spi::SpiFuture<'static, Result<(), DeliveryError>>
        })
        .error_handler(move |_, _| {
            error.lock().unwrap().push("error");
            FailureDirective::Retry
        })
        .build();
    block_on(async {
        let mut subscription = bus
            .subscribe(
                SubscribeRequest::new(SubscriberId::new("async-middleware-retry").unwrap(), topic())
                    .with_options(options),
            )
            .await
            .unwrap();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "middleware-retry",
        ))));
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let attempt = attempts_by_handler.fetch_add(1, Ordering::AcqRel);
                handler_log
                    .lock()
                    .unwrap()
                    .push(if attempt == 0 { "handler-1" } else { "handler-2" });
                async move {
                    if attempt == 0 {
                        Err(DeliveryError::Handler {
                            source: Box::new(std::io::Error::other("retry")),
                        })
                    } else {
                        Ok(())
                    }
                }
            }))
        });
        for _ in 0..100 {
            if spi.settlement_count() > 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(attempts.load(Ordering::Acquire), 2);
        assert_eq!(
            spi.settlement_dispositions(),
            [qubit_event_bus::spi::DeliveryDisposition::Accept]
        );
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
    assert_eq!(
        *calls.lock().unwrap(),
        ["before", "handler-1", "after", "error", "before", "handler-2", "after"]
    );
}

/// Verifies failed asynchronous deliveries map directives to provider
/// settlement outcomes.
#[test]
fn async_failure_directives_settle_requeue_discard_and_dead_letter_outcomes() {
    fn run_case(
        directive: FailureDirective,
        fail_dead_letter_publish: bool,
    ) -> (Vec<qubit_event_bus::spi::DeliveryDisposition>, Vec<&'static str>) {
        let spi = Arc::new(FakeAsyncEventBusSpi::new());
        let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
        let mut builder = SubscribeOptions::<u32>::builder().error_handler(move |_, _| directive);
        if directive == FailureDirective::DeadLetter {
            builder = builder.dead_letter(DeadLetterPolicy::topic("test.dead").unwrap());
        }
        block_on(async {
            let mut subscription = bus
                .subscribe(
                    SubscribeRequest::new(SubscriberId::new("async-directive").unwrap(), topic())
                        .with_options(builder.build()),
                )
                .await
                .unwrap();
            if fail_dead_letter_publish {
                spi.fail_next_publish();
            }
            spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
                subscription.id(),
                "directive-event",
            ))));
            let runner = std::thread::spawn(move || {
                block_on(subscription.run(|_| async {
                    Err(DeliveryError::Handler {
                        source: Box::new(std::io::Error::other("directive failure")),
                    })
                }))
            });
            for _ in 0..100 {
                if spi.settlement_count() > 0 {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            assert!(spi.settlement_count() > 0, "directive reaches provider settlement");
            let dispositions = spi.settlement_dispositions();
            let operations = spi.operation_log();
            bus.shutdown(ShutdownMode::Immediate).await.unwrap();
            runner.join().unwrap().unwrap();
            (dispositions, operations)
        })
    }

    let (requeued, _) = run_case(FailureDirective::Requeue, false);
    assert_eq!(requeued, [qubit_event_bus::spi::DeliveryDisposition::Retry]);
    let (discarded, _) = run_case(FailureDirective::Discard, false);
    assert_eq!(discarded, [qubit_event_bus::spi::DeliveryDisposition::Reject]);
    let (dead_lettered, success_operations) = run_case(FailureDirective::DeadLetter, false);
    assert!(dead_lettered.contains(&qubit_event_bus::spi::DeliveryDisposition::Reject));
    assert!(
        success_operations.contains(&"publish"),
        "dead-letter success publishes the record"
    );
    let (requeued_after_failure, failure_operations) = run_case(FailureDirective::DeadLetter, true);
    assert_eq!(
        requeued_after_failure,
        [qubit_event_bus::spi::DeliveryDisposition::Retry]
    );
    assert!(
        failure_operations.contains(&"publish"),
        "dead-letter failure is attempted before requeue"
    );
}

#[test]
fn cancelling_the_run_future_preserves_an_already_received_delivery_for_resume() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let request = SubscribeRequest::new(SubscriberId::new("async-resume").unwrap(), topic());
    let first_started = Arc::new(AtomicBool::new(false));
    let allow_completion = Arc::new(AtomicBool::new(false));
    let handler_calls = Arc::new(AtomicUsize::new(0));

    block_on(async {
        let mut subscription = bus.subscribe(request).await.unwrap();
        bus.publish(PublishRequest::new(topic(), 42).unwrap()).await.unwrap();
        let first_started_by_handler = first_started.clone();
        let allow_completion_by_handler = allow_completion.clone();
        let handler_calls_by_handler = handler_calls.clone();
        let mut first_run = Box::pin(subscription.run(move |_| {
            let started = first_started_by_handler.clone();
            let allow_completion = allow_completion_by_handler.clone();
            let handler_calls = handler_calls_by_handler.clone();
            async move {
                started.store(true, Ordering::Release);
                handler_calls.fetch_add(1, Ordering::AcqRel);
                std::future::poll_fn(|context| {
                    if allow_completion.load(Ordering::Acquire) {
                        std::task::Poll::Ready(Ok(()))
                    } else {
                        context.waker().wake_by_ref();
                        std::task::Poll::Pending
                    }
                })
                .await
            }
        }));
        for _ in 0..100 {
            let _ = support::manual_async::poll_once(first_run.as_mut());
            if first_started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(first_started.load(Ordering::Acquire));
        drop(first_run);

        allow_completion.store(true, Ordering::Release);
        let replacement_calls = Arc::new(AtomicUsize::new(0));
        let replacement_calls_by_handler = replacement_calls.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                replacement_calls_by_handler.fetch_add(1, Ordering::AcqRel);
                async { Ok(()) }
            }))
        });
        for _ in 0..100 {
            if spi.settlement_count() == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(
            handler_calls.load(Ordering::Acquire),
            1,
            "resume must continue the original handler future"
        );
        assert_eq!(
            replacement_calls.load(Ordering::Acquire),
            0,
            "a resumed task must not use the replacement handler"
        );
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn dropping_a_paused_subscription_drops_receiver_and_recovers_unsettled_delivery() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let handler_started = Arc::new(AtomicBool::new(false));

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("drop-paused").unwrap(),
                topic(),
            ))
            .await
            .unwrap();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "drop-recovery",
        ))));
        let started = handler_started.clone();
        let mut run = Box::pin(subscription.run(move |_| {
            let started = started.clone();
            async move {
                started.store(true, Ordering::Release);
                std::future::pending::<Result<(), DeliveryError>>().await
            }
        }));
        for _ in 0..100 {
            let _ = support::manual_async::poll_once(run.as_mut());
            if handler_started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(handler_started.load(Ordering::Acquire));
        drop(run);
        drop(subscription);
    });

    assert_eq!(spi.settlement_count(), 0, "drop must not settle the delivery");
    assert_eq!(
        spi.receiver_drop_recoveries(),
        1,
        "dropping the paused session must drop its receiver and recover its in-flight delivery"
    );
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn dropping_subscription_during_shutdown_takeover_releases_the_active_session() {
    use std::task::Poll;
    use std::task::Waker;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let started = Arc::new(AtomicBool::new(false));
    let finish = Arc::new(AtomicBool::new(false));
    let handler_wakers = Arc::new(std::sync::Mutex::new(Vec::<Waker>::new()));
    let mut subscription = block_on(bus.subscribe(SubscribeRequest::new(
        SubscriberId::new("drop-during-takeover").unwrap(),
        topic(),
    )))
    .unwrap();
    spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "drop-takeover",
    ))));

    let started_by_handler = started.clone();
    let finish_by_handler = finish.clone();
    let wakers_by_handler = handler_wakers.clone();
    let mut run = Box::pin(subscription.run(move |_| {
        let started = started_by_handler.clone();
        let finish = finish_by_handler.clone();
        let wakers = wakers_by_handler.clone();
        async move {
            started.store(true, Ordering::Release);
            std::future::poll_fn(move |context| {
                if finish.load(Ordering::Acquire) {
                    Poll::Ready(Ok(()))
                } else {
                    wakers.lock().unwrap().push(context.waker().clone());
                    Poll::Pending
                }
            })
            .await
        }
    }));
    for _ in 0..100 {
        let _ = support::manual_async::poll_once(run.as_mut());
        if started.load(Ordering::Acquire) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert!(started.load(Ordering::Acquire));
    drop(run);

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(matches!(
        support::manual_async::poll_once(shutdown.as_mut()),
        Poll::Pending
    ));
    drop(subscription);
    finish.store(true, Ordering::Release);
    for waker in std::mem::take(&mut *handler_wakers.lock().unwrap()) {
        waker.wake();
    }
    let mut outcome = None;
    for _ in 0..100 {
        if let Poll::Ready(result) = support::manual_async::poll_once(shutdown.as_mut()) {
            outcome = Some(result);
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    outcome
        .expect("shutdown should finish after the active handler exits")
        .unwrap();
    assert_eq!(spi.settlement_count(), 1);
    assert!(spi.operation_log().contains(&"close"));
}

#[test]
fn async_subscription_decodes_encoded_payload_with_the_topic_codec() {
    struct Utf8Codec(ContentType);

    impl EventCodec<String> for Utf8Codec {
        fn content_type(&self) -> &ContentType {
            &self.0
        }

        fn schema_id(&self) -> Option<&qubit_event_bus::model::SchemaId> {
            None
        }

        fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
            Ok(Arc::from(value.as_bytes()))
        }

        fn decode(&self, bytes: &[u8]) -> Result<String, CodecError> {
            String::from_utf8(bytes.to_vec()).map_err(|source| CodecError::Decode {
                source: Box::new(source),
            })
        }
    }

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let topic = Topic::with_codec(
        "async.encoded-subscription",
        Utf8Codec(ContentType::new("text/plain").unwrap()),
    )
    .unwrap();
    let received = Arc::new(std::sync::Mutex::new(None));

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("encoded-subscription").unwrap(),
                topic,
            ))
            .await
            .unwrap();
        spi.enqueue(InboundMessage::new(
            TopicAddress::new("async.encoded-subscription").unwrap(),
            EventId::new("encoded-subscription-event").unwrap(),
            SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            TransportPayload::Encoded(EncodedPayload::new(
                Arc::from(b"decoded from transport".as_slice()),
                ContentType::new("text/plain").unwrap(),
                None,
            )),
            Some(SettlementToken::new(subscription.id(), "encoded-subscription-token")),
            Default::default(),
        ));
        let received_by_handler = received.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |delivery| {
                *received_by_handler.lock().unwrap() = Some(delivery.payload().clone());
                async { Ok(()) }
            }))
        });
        for _ in 0..100 {
            if received.lock().unwrap().is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert_eq!(*received.lock().unwrap(), Some("decoded from transport".to_owned()));
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });

    assert_eq!(spi.settlement_count(), 1);
}

#[test]
fn shutdown_takes_over_a_paused_async_session_and_finishes_its_owned_task() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let started = Arc::new(AtomicBool::new(false));
    let finish = Arc::new(AtomicBool::new(false));
    let handler_calls = Arc::new(AtomicUsize::new(0));

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("paused-shutdown").unwrap(),
                topic(),
            ))
            .await
            .unwrap();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "paused-shutdown-token",
        ))));
        let started_by_handler = started.clone();
        let finish_by_handler = finish.clone();
        let calls_by_handler = handler_calls.clone();
        let mut run = Box::pin(subscription.run(move |_| {
            let started = started_by_handler.clone();
            let finish = finish_by_handler.clone();
            let calls = calls_by_handler.clone();
            async move {
                calls.fetch_add(1, Ordering::AcqRel);
                started.store(true, Ordering::Release);
                std::future::poll_fn(|context| {
                    if finish.load(Ordering::Acquire) {
                        std::task::Poll::Ready(Ok(()))
                    } else {
                        context.waker().wake_by_ref();
                        std::task::Poll::Pending
                    }
                })
                .await
            }
        }));
        for _ in 0..100 {
            let _ = support::manual_async::poll_once(run.as_mut());
            if started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(started.load(Ordering::Acquire));
        drop(run);
        finish.store(true, Ordering::Release);

        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });

    assert_eq!(handler_calls.load(Ordering::Acquire), 1);
    assert_eq!(
        spi.settlement_count(),
        1,
        "shutdown must settle the paused session's completed task"
    );
}

#[test]
fn async_success_settles_the_provider_token_after_handler_completion() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let request = SubscribeRequest::new(SubscriberId::new("async-settle").unwrap(), topic());

    block_on(async {
        let mut subscription = bus.subscribe(request).await.unwrap();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "settlement-1",
        ))));
        let runner = std::thread::spawn(move || block_on(subscription.run(|_| async { Ok(()) })));
        for _ in 0..100 {
            if spi.settlement_count() == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(spi.settlement_count(), 1);
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn cancelling_run_during_settle_keeps_token_for_idempotent_retry() {
    use std::future::Future;
    use std::task::Poll;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let handler_calls = Arc::new(AtomicUsize::new(0));
    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("settle-cancel").unwrap(),
                topic(),
            ))
            .await
            .unwrap();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "cancelled-settle",
        ))));
        spi.pause_next_settle();
        {
            let first_calls = handler_calls.clone();
            let mut first_run = Box::pin(subscription.run(move |_| {
                first_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }));
            std::future::poll_fn(|cx| {
                let _ = first_run.as_mut().poll(cx);
                if spi.operation_log().iter().filter(|op| **op == "settle").count() > 0 {
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            })
            .await;
        }
        let next_calls = handler_calls.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                next_calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }))
        });
        for _ in 0..100 {
            if spi.operation_log().iter().filter(|op| **op == "settle").count() >= 2 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(spi.operation_log().iter().filter(|op| **op == "settle").count(), 2);
        assert_eq!(spi.settlement_count(), 1);
        assert_eq!(
            handler_calls.load(Ordering::SeqCst),
            1,
            "resuming settlement must not rerun a completed handler"
        );
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn dropping_idle_wait_unregisters_signal_waker() {
    use std::future::Future;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let handler_waker = Arc::new(std::sync::Mutex::new(None::<Waker>));
    let runner = {
        let bus = bus.clone();
        let started = started.clone();
        let release = release.clone();
        let handler_waker = handler_waker.clone();
        let spi = spi.clone();
        std::thread::spawn(move || {
            block_on(async move {
                let mut subscription = bus
                    .subscribe(SubscribeRequest::new(
                        SubscriberId::new("stale-waker").unwrap(),
                        topic(),
                    ))
                    .await
                    .unwrap();
                spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
                    subscription.id(),
                    "stale-waker-token",
                ))));
                subscription
                    .run(move |_| {
                        let started = started.clone();
                        let release = release.clone();
                        let waker = handler_waker.clone();
                        async move {
                            started.store(true, Ordering::Release);
                            std::future::poll_fn(move |cx| {
                                if release.load(Ordering::Acquire) {
                                    return Poll::Ready(Ok(()));
                                }
                                *waker.lock().unwrap() = Some(cx.waker().clone());
                                Poll::Pending
                            })
                            .await
                        }
                    })
                    .await
            })
        })
    };
    for _ in 0..100 {
        if started.load(Ordering::Acquire) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    assert!(started.load(Ordering::Acquire));
    let (sender, receiver) = std::sync::mpsc::channel();
    let waker = Waker::from(Arc::new(WakeOnSignal(sender)));
    let mut context = Context::from_waker(&waker);
    let waiting_topic = topic();
    let mut waiting = Box::pin(bus.wait_for_idle(&waiting_topic, None));
    assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
    drop(waiting);

    release.store(true, Ordering::Release);
    if let Some(waker) = handler_waker.lock().unwrap().take() {
        waker.wake();
    }
    block_on(async {
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });
    runner.join().unwrap().unwrap();
    assert!(
        receiver.try_recv().is_err(),
        "dropped waiters must not leave stale wakers registered"
    );
}

#[test]
fn async_settlement_failure_retries_the_same_token_without_rerunning_handler() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let settlement_failed = Arc::new(AtomicBool::new(false));
    let observed = settlement_failed.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::SettlementFailed { .. }) {
            observed.store(true, Ordering::Release);
        }
    });
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let request = SubscribeRequest::new(SubscriberId::new("async-settle-failure").unwrap(), topic());

    block_on(async {
        let mut subscription = bus.subscribe(request).await.unwrap();
        spi.fail_next_settle();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "settlement-fail",
        ))));
        let calls = handler_calls.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }))
        });
        for _ in 0..100 {
            if settlement_failed.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(settlement_failed.load(Ordering::Acquire));
        for _ in 0..100 {
            if spi.settlement_count() == 1 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert_eq!(spi.settlement_count(), 1, "the retained token is retried");
        assert_eq!(
            handler_calls.load(Ordering::SeqCst),
            1,
            "settlement retry does not rerun the handler"
        );
        assert_eq!(spi.operation_log().iter().filter(|op| **op == "settle").count(), 2);
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
    });
}

#[test]
fn cancelling_pending_provider_shutdown_can_be_retried_after_partial_progress() {
    use std::task::Poll;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    spi.pause_shutdown();
    let mut first_shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));

    assert!(matches!(
        support::manual_async::poll_once(first_shutdown.as_mut()),
        Poll::Pending
    ));
    assert_eq!(1, spi.shutdown_transition_count());
    drop(first_shutdown);

    spi.release_shutdown();
    assert_eq!(
        ShutdownOutcome::Complete,
        block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap()
    );
    assert_eq!(
        1,
        spi.shutdown_transition_count(),
        "the provider resumes its original shutdown"
    );
    assert_eq!(
        2,
        spi.operation_log()
            .iter()
            .filter(|operation| **operation == "shutdown")
            .count()
    );
}

#[test]
fn async_shutdown_stops_permanent_settlement_retry_after_receiver_close() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("permanent-settlement-failure").unwrap(),
                topic(),
            ))
            .await
            .unwrap();
        spi.fail_all_settles();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "permanent-failure-token",
        ))));
        let runner = std::thread::spawn(move || block_on(subscription.run(|_| async { Ok(()) })));
        for _ in 0..100 {
            if spi.operation_log().contains(&"settle") {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(spi.operation_log().contains(&"settle"));
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
        assert_eq!(spi.settlement_count(), 0, "provider never accepted this token");
        assert!(
            spi.operation_log().contains(&"close"),
            "receiver close owns final cleanup"
        );
    });
}

#[test]
fn permanent_settlement_failure_yields_to_same_executor_shutdown() {
    use std::task::Poll;

    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let mut subscription = block_on(bus.subscribe(SubscribeRequest::new(
        SubscriberId::new("same-executor-settlement-retry").unwrap(),
        topic(),
    )))
    .unwrap();
    spi.fail_all_settles();
    spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
        subscription.id(),
        "same-executor-token",
    ))));

    let mut runner = Box::pin(subscription.run(|_| async { Ok(()) }));
    assert!(matches!(
        support::manual_async::poll_once(runner.as_mut()),
        Poll::Pending
    ));
    assert!(matches!(
        support::manual_async::poll_once(runner.as_mut()),
        Poll::Pending
    ));

    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(matches!(
        support::manual_async::poll_once(shutdown.as_mut()),
        Poll::Pending
    ));
    assert!(matches!(
        support::manual_async::poll_once(runner.as_mut()),
        Poll::Ready(Ok(()))
    ));
    assert!(matches!(
        support::manual_async::poll_once(shutdown.as_mut()),
        Poll::Ready(Ok(_))
    ));
    assert_eq!(
        1,
        spi.operation_log()
            .iter()
            .filter(|operation| **operation == "settle")
            .count()
    );
}

#[test]
fn async_decode_settlement_failure_diagnostic_keeps_inbound_identity_without_event() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let observed = Arc::new(std::sync::Mutex::new(None));
    let observed_by_callback = observed.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::SettlementFailed { event_id, topic, .. } = diagnostic {
            *observed_by_callback.lock().unwrap() = Some((event_id.as_str().to_owned(), topic.to_string()));
        }
    });
    let string_topic = Topic::<String>::new("test.topic").unwrap();
    let request = SubscribeRequest::new(SubscriberId::new("decode-settle-failure").unwrap(), string_topic);

    block_on(async {
        let mut subscription = bus.subscribe(request).await.unwrap();
        spi.fail_next_settle();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "decode-fail",
        ))));
        let runner = std::thread::spawn(move || block_on(subscription.run(|_| async { Ok(()) })));
        for _ in 0..100 {
            if observed.lock().unwrap().is_some() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        runner.join().unwrap().unwrap();
        assert_eq!(
            *observed.lock().unwrap(),
            Some(("event-test".into(), "test.topic".into()))
        );
    });
}

#[test]
fn async_spi_receive_poll_panic_is_converted_to_a_structured_error_and_closed() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let request = SubscribeRequest::new(SubscriberId::new("async-spi-panic").unwrap(), topic());

    block_on(async {
        let mut subscription = bus.subscribe(request).await.unwrap();
        spi.panic_next_receive();
        let error = subscription.run(|_| async { Ok(()) }).await.unwrap_err();
        assert!(matches!(error, qubit_event_bus::ReceiveError::Spi(error) if error.kind() == "provider_panicked"));
        assert!(spi.operation_log().contains(&"close"));
        bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });
}

#[test]
fn concurrent_async_shutdown_calls_close_the_provider_once() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let first = bus.clone();
    let second = bus.clone();

    let first = std::thread::spawn(move || block_on(first.shutdown(ShutdownMode::Immediate)));
    let second = std::thread::spawn(move || block_on(second.shutdown(ShutdownMode::Immediate)));

    assert_eq!(first.join().unwrap().unwrap(), ShutdownOutcome::Complete);
    assert_eq!(second.join().unwrap().unwrap(), ShutdownOutcome::Complete);
    assert_eq!(
        spi.operation_log()
            .iter()
            .filter(|operation| **operation == "shutdown")
            .count(),
        1
    );
}

#[test]
fn immediate_shutdown_waits_for_runner_settlement_and_close_before_provider_shutdown() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::new(ProviderId::new("fake").unwrap(), spi.clone());
    let started = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let handlers = Arc::new(AtomicUsize::new(0));

    block_on(async {
        let mut subscription = bus
            .subscribe(SubscribeRequest::new(
                SubscriberId::new("shutdown-order").unwrap(),
                topic(),
            ))
            .await
            .unwrap();
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "shutdown-order",
        ))));
        spi.enqueue(support::fake_spi::inbound_message(Some(SettlementToken::new(
            subscription.id(),
            "shutdown-queued",
        ))));
        let handler_started = started.clone();
        let handler_release = release.clone();
        let handler_count = handlers.clone();
        let runner = std::thread::spawn(move || {
            block_on(subscription.run(move |_| {
                let handler_started = handler_started.clone();
                let handler_release = handler_release.clone();
                let handler_count = handler_count.clone();
                async move {
                    handler_count.fetch_add(1, Ordering::AcqRel);
                    handler_started.store(true, Ordering::Release);
                    while !handler_release.load(Ordering::Acquire) {
                        std::thread::yield_now();
                    }
                    Ok(())
                }
            }))
        });
        for _ in 0..100 {
            if started.load(Ordering::Acquire) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        assert!(started.load(Ordering::Acquire));
        let shutdown_bus = bus.clone();
        let shutdown = std::thread::spawn(move || block_on(shutdown_bus.shutdown(ShutdownMode::Immediate)));
        std::thread::sleep(std::time::Duration::from_millis(20));
        let shutdown_started_early = spi.operation_log().contains(&"shutdown");
        release.store(true, Ordering::Release);
        runner.join().unwrap().unwrap();
        shutdown.join().unwrap().unwrap();
        assert!(
            !shutdown_started_early,
            "provider shutdown must wait for the active handler"
        );
        let operations = spi.operation_log();
        let settle = operations.iter().position(|operation| *operation == "settle").unwrap();
        let close = operations.iter().position(|operation| *operation == "close").unwrap();
        let shutdown = operations
            .iter()
            .position(|operation| *operation == "shutdown")
            .unwrap();
        assert!(
            settle < close && close < shutdown,
            "expected settle < close < shutdown, got {operations:?}"
        );
        assert_eq!(
            handlers.load(Ordering::Acquire),
            1,
            "queued delivery must not start after shutdown request"
        );
        assert_eq!(spi.settlement_count(), 1);
    });
}
