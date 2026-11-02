// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public subscriber middleware ordering and panic-boundary contracts.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::facade::AsyncEventBus;
use qubit_event_bus::model::AsyncSubscriberNext;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberNext;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::SpiFuture;

mod support;

use support::fake_spi::FakeAsyncEventBusSpi;
use support::fake_spi::FakeEventBusSpi;

fn wait_until(mut predicate: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while !predicate() && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(predicate(), "delivery did not reach its expected state");
}

#[test]
fn test_sync_chain_preserves_registration_order_and_layer_counts() {
    for layer_count in [0, 1, 8, 32] {
        let spi = Arc::new(FakeEventBusSpi::new());
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut config = EventBusFacadeConfig::new();
        for layer in 0..layer_count {
            let order = order.clone();
            config = config.subscriber_interceptor(move |delivery: Delivery<u32>, next: SubscriberNext<u32>| {
                order.lock().unwrap().push(layer);
                next(delivery)
            });
        }
        let bus = EventBus::with_config(ProviderId::new("middleware-sync").unwrap(), spi.clone(), config)
            .expect("sync middleware config is valid");
        let request = SubscribeRequest::new("middleware-sync", Topic::<u32>::new("test.topic").unwrap()).unwrap();
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let handler_count = handler_calls.clone();
        let subscription = bus
            .subscribe(request, move |_| {
                handler_count.fetch_add(1, Ordering::AcqRel);
            })
            .unwrap();
        spi.enqueue(support::fake_spi::inbound_message(None));
        wait_until(|| handler_calls.load(Ordering::Acquire) == 1);
        assert_eq!(*order.lock().unwrap(), (0..layer_count).collect::<Vec<_>>());
        subscription.cancel().unwrap();
        let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
    }
}

#[test]
fn test_sync_global_and_typed_middleware_order_and_short_circuit() {
    let spi = Arc::new(FakeEventBusSpi::new());
    let order = Arc::new(Mutex::new(Vec::new()));
    let global_order = order.clone();
    let config = EventBusFacadeConfig::new().subscriber_interceptor(
        move |delivery: Delivery<u32>, next: SubscriberNext<u32>| {
            global_order.lock().unwrap().push("global-before");
            next(delivery)?;
            global_order.lock().unwrap().push("global-after");
            Ok(())
        },
    );
    let bus = EventBus::with_config(ProviderId::new("middleware-sync-order").unwrap(), spi.clone(), config).unwrap();
    let typed_order = order.clone();
    let options = SubscribeOptions::<u32>::builder()
        .interceptor(move |delivery, next| {
            typed_order.lock().unwrap().push("typed-before");
            next(delivery)?;
            typed_order.lock().unwrap().push("typed-after");
            Ok(())
        })
        .build();
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let handler_count = handler_calls.clone();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("middleware-sync-order", Topic::<u32>::new("test.topic").unwrap())
                .unwrap()
                .with_options(options),
            move |_| {
                handler_count.fetch_add(1, Ordering::AcqRel);
            },
        )
        .unwrap();
    spi.enqueue(support::fake_spi::inbound_message(None));
    wait_until(|| order.lock().unwrap().len() == 4);
    assert_eq!(handler_calls.load(Ordering::Acquire), 1);
    assert_eq!(
        *order.lock().unwrap(),
        ["global-before", "typed-before", "typed-after", "global-after"]
    );
    subscription.cancel().unwrap();
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();

    let spi = Arc::new(FakeEventBusSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("middleware-short-circuit").unwrap(), spi.clone()).unwrap();
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let handler_count = handler_calls.clone();
    let short_circuits = Arc::new(AtomicUsize::new(0));
    let short_circuit_count = short_circuits.clone();
    let options = SubscribeOptions::<u32>::builder()
        .interceptor(move |_, _| {
            short_circuit_count.fetch_add(1, Ordering::AcqRel);
            Ok(())
        })
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("middleware-short-circuit", Topic::<u32>::new("test.topic").unwrap())
                .unwrap()
                .with_options(options),
            move |_| {
                handler_count.fetch_add(1, Ordering::AcqRel);
            },
        )
        .unwrap();
    spi.enqueue(support::fake_spi::inbound_message(None));
    wait_until(|| short_circuits.load(Ordering::Acquire) == 1);
    assert_eq!(
        handler_calls.load(Ordering::Acquire),
        0,
        "short-circuit must skip inner work"
    );
    subscription.cancel().unwrap();
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn test_sync_middleware_panic_is_reported_and_retry_rebuilds_the_chain() {
    let spi = Arc::new(FakeEventBusSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("middleware-sync-retry").unwrap(), spi.clone()).unwrap();
    let middleware_calls = Arc::new(AtomicUsize::new(0));
    let middleware_count = middleware_calls.clone();
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let handler_count = handler_calls.clone();
    let options = SubscribeOptions::<u32>::builder()
        .retry_policy(qubit_retry::RetryPolicy::builder().max_attempts(2).build().unwrap())
        .error_handler(|_, _| FailureDirective::Retry)
        .interceptor(move |delivery, next| {
            if middleware_count.fetch_add(1, Ordering::AcqRel) == 0 {
                panic!("first middleware attempt");
            }
            next(delivery)
        })
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("middleware-sync-retry", Topic::<u32>::new("test.topic").unwrap())
                .unwrap()
                .with_options(options),
            move |_| {
                handler_count.fetch_add(1, Ordering::AcqRel);
            },
        )
        .unwrap();
    spi.enqueue(support::fake_spi::inbound_message(None));
    wait_until(|| handler_calls.load(Ordering::Acquire) == 1);
    assert_eq!(
        middleware_calls.load(Ordering::Acquire),
        2,
        "each retry should rebuild the middleware invocation"
    );
    subscription.cancel().unwrap();
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn test_handler_failure_retries_through_the_middleware_chain() {
    let spi = Arc::new(FakeEventBusSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("middleware-handler-retry").unwrap(), spi.clone()).unwrap();
    let middleware_calls = Arc::new(AtomicUsize::new(0));
    let middleware_count = middleware_calls.clone();
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let handler_count = handler_calls.clone();
    let options = SubscribeOptions::<u32>::builder()
        .retry_policy(qubit_retry::RetryPolicy::builder().max_attempts(2).build().unwrap())
        .error_handler(|_, _| FailureDirective::Retry)
        .interceptor(move |delivery, next| {
            middleware_count.fetch_add(1, Ordering::AcqRel);
            next(delivery)
        })
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("middleware-handler-retry", Topic::<u32>::new("test.topic").unwrap())
                .unwrap()
                .with_options(options),
            move |_| {
                if handler_count.fetch_add(1, Ordering::AcqRel) == 0 {
                    Err(DeliveryError::Handler {
                        source: Box::new(std::io::Error::other("retry handler once")),
                    })
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
    spi.enqueue(support::fake_spi::inbound_message(None));
    wait_until(|| handler_calls.load(Ordering::Acquire) == 2);
    assert_eq!(middleware_calls.load(Ordering::Acquire), 2);
    subscription.cancel().unwrap();
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn test_async_chain_preserves_registration_order_and_layer_counts() {
    for layer_count in [0, 1, 8, 32] {
        let spi = Arc::new(FakeAsyncEventBusSpi::new());
        let order = Arc::new(Mutex::new(Vec::new()));
        let mut config = EventBusFacadeConfig::new();
        for layer in 0..layer_count {
            let order = order.clone();
            config =
                config.async_subscriber_interceptor(move |delivery: Delivery<u32>, next: AsyncSubscriberNext<u32>| {
                    let order = order.clone();
                    Box::pin(async move {
                        order.lock().unwrap().push(layer);
                        next(delivery).await
                    }) as SpiFuture<'static, Result<(), DeliveryError>>
                });
        }
        let bus =
            AsyncEventBus::with_config(ProviderId::new("middleware-async").unwrap(), spi.clone(), config).unwrap();
        let request = SubscribeRequest::new("middleware-async", Topic::<u32>::new("test.topic").unwrap()).unwrap();
        let handler_calls = Arc::new(AtomicUsize::new(0));
        let handler_count = handler_calls.clone();
        let subscription = support::manual_async::block_on(bus.subscribe(request)).unwrap();
        let runner = std::thread::spawn(move || {
            support::manual_async::block_on(subscription.run(move |_| {
                let handler_count = handler_count.clone();
                async move {
                    handler_count.fetch_add(1, Ordering::AcqRel);
                    Ok(())
                }
            }))
        });
        let _ = support::manual_async::block_on(bus.publish(
            qubit_event_bus::model::PublishRequest::new(Topic::<u32>::new("test.topic").unwrap(), 42).unwrap(),
        ))
        .unwrap();
        wait_until(|| handler_calls.load(Ordering::Acquire) == 1);
        assert_eq!(*order.lock().unwrap(), (0..layer_count).collect::<Vec<_>>());
        let _ = support::manual_async::block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
        runner.join().unwrap().unwrap();
    }
}

#[test]
fn test_async_middleware_construction_and_poll_panics_are_isolated() {
    for panic_during_poll in [false, true] {
        let spi = Arc::new(FakeAsyncEventBusSpi::new());
        let middleware =
            move |_: Delivery<u32>, _: AsyncSubscriberNext<u32>| -> SpiFuture<'static, Result<(), DeliveryError>> {
                if panic_during_poll {
                    Box::pin(async { panic!("middleware poll panic") })
                } else {
                    panic!("middleware construction panic")
                }
            };
        let config = EventBusFacadeConfig::new().async_subscriber_interceptor(middleware);
        let bus = AsyncEventBus::with_config(ProviderId::new("middleware-async-panic").unwrap(), spi.clone(), config)
            .unwrap();
        let failed = Arc::new(AtomicUsize::new(0));
        let failed_by_error_handler = failed.clone();
        let options = SubscribeOptions::<u32>::builder()
            .error_handler(move |_, _| {
                failed_by_error_handler.fetch_add(1, Ordering::AcqRel);
                FailureDirective::Discard
            })
            .build();
        let subscription = support::manual_async::block_on(
            bus.subscribe(
                SubscribeRequest::new("middleware-async-panic", Topic::<u32>::new("test.topic").unwrap())
                    .unwrap()
                    .with_options(options),
            ),
        )
        .unwrap();
        let runner =
            std::thread::spawn(move || support::manual_async::block_on(subscription.run(|_| async { Ok(()) })));
        let _ = support::manual_async::block_on(bus.publish(
            qubit_event_bus::model::PublishRequest::new(Topic::<u32>::new("test.topic").unwrap(), 42).unwrap(),
        ))
        .unwrap();
        wait_until(|| failed.load(Ordering::Acquire) > 0);
        let _ = support::manual_async::block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
        runner.join().unwrap().unwrap();
    }
}
