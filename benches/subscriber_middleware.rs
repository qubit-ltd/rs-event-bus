// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Compares facade delivery with different subscriber middleware chain lengths.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::model::AsyncSubscriberNext;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberNext;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::SpiFuture;

#[path = "../tests/support/fake_spi.rs"]
#[allow(dead_code)]
mod fake_spi;
#[path = "../tests/support/manual_async.rs"]
#[allow(dead_code)]
mod manual_async;

const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const ITERATIONS: usize = 500;

fn wait_for(count: &AtomicUsize, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while count.load(Ordering::Acquire) < expected && Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(count.load(Ordering::Acquire), expected, "delivery benchmark stalled");
}

fn sample_sync(layer_count: usize) -> u128 {
    let spi = Arc::new(fake_spi::FakeEventBusSpi::new());
    let mut config = EventBusFacadeConfig::new();
    for _ in 0..layer_count {
        config = config.subscriber_interceptor(|delivery: Delivery<u32>, next: SubscriberNext<u32>| next(delivery));
    }
    let bus = EventBus::with_config(ProviderId::new("middleware-bench-sync").unwrap(), spi.clone(), config).unwrap();
    let handled = Arc::new(AtomicUsize::new(0));
    let handled_by_handler = handled.clone();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("middleware-bench-sync", Topic::<u32>::new("test.topic").unwrap()).unwrap(),
            move |_: Delivery<u32>| {
                handled_by_handler.fetch_add(1, Ordering::AcqRel);
            },
        )
        .unwrap();

    let start = Instant::now();
    for index in 0..ITERATIONS {
        let _ = black_box(
            bus.publish(black_box(
                qubit_event_bus::model::PublishRequest::new(Topic::<u32>::new("test.topic").unwrap(), index as u32)
                    .unwrap(),
            ))
            .unwrap(),
        );
        wait_for(&handled, index + 1);
    }
    let elapsed = start.elapsed().as_nanos();
    subscription.cancel().unwrap();
    let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
    elapsed / ITERATIONS as u128
}

fn sample_async(layer_count: usize) -> u128 {
    let spi = Arc::new(fake_spi::FakeAsyncEventBusSpi::new());
    let mut config = EventBusFacadeConfig::new();
    for _ in 0..layer_count {
        config = config.async_subscriber_interceptor(
            |delivery: Delivery<u32>,
             next: AsyncSubscriberNext<u32>|
             -> SpiFuture<'static, Result<(), DeliveryError>> {
                Box::pin(async move { next(delivery).await })
            },
        );
    }
    let bus = AsyncEventBus::with_config(ProviderId::new("middleware-bench-async").unwrap(), spi, config).unwrap();
    let handled = Arc::new(AtomicUsize::new(0));
    let handled_by_handler = handled.clone();
    let subscription =
        manual_async::block_on(bus.subscribe(
            SubscribeRequest::new("middleware-bench-async", Topic::<u32>::new("test.topic").unwrap()).unwrap(),
        ))
        .unwrap();
    let runner = std::thread::spawn(move || {
        manual_async::block_on(subscription.run(move |_| {
            let handled = handled_by_handler.clone();
            async move {
                handled.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
        }))
    });

    let start = Instant::now();
    for index in 0..ITERATIONS {
        let _ = black_box(
            manual_async::block_on(
                bus.publish(black_box(
                    qubit_event_bus::model::PublishRequest::new(Topic::<u32>::new("test.topic").unwrap(), index as u32)
                        .unwrap(),
                )),
            )
            .unwrap(),
        );
        wait_for(&handled, index + 1);
    }
    let elapsed = start.elapsed().as_nanos();
    let _ = manual_async::block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    runner.join().unwrap().unwrap();
    elapsed / ITERATIONS as u128
}

fn print_samples(kind: &str, layer_count: usize, mut samples: Vec<u128>) {
    println!(
        "{kind} layers={layer_count} raw_sample_mean_ns={samples:?} iterations={ITERATIONS} boundary=publish_and_one_subscriber_delivery"
    );
    samples.sort_unstable();
    println!(
        "{kind} layers={layer_count}: median={} ns/op p95={} ns/op",
        samples[SAMPLES / 2],
        samples[(SAMPLES * 95).div_ceil(100) - 1]
    );
}

fn main() {
    for kind in ["sync", "async"] {
        for layers in [0, 1, 8, 32] {
            for _ in 0..WARMUPS {
                black_box(if kind == "sync" {
                    sample_sync(layers)
                } else {
                    sample_async(layers)
                });
            }
            let samples = (0..SAMPLES)
                .map(|_| {
                    if kind == "sync" {
                        sample_sync(layers)
                    } else {
                        sample_async(layers)
                    }
                })
                .collect();
            print_samples(kind, layers, samples);
        }
    }
}
