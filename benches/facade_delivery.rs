// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures caller-driven facade admission on the local in-process SPI.
//!
//! These timings do not measure remote provider or handler completion latency.

use std::future::Future;
use std::hint::black_box;
use std::mem::size_of;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::yield_now;
use std::time::Instant;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::CheckedPublishError;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::NotificationPublisher;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionCheckError;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::PublishReceipt;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const ITERATIONS: usize = 1_000;
const DESTINATIONS: [usize; 4] = [0, 1, 16, 128];

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => yield_now(),
        }
    }
}

fn config() -> LocalEventBusConfig {
    LocalEventBusConfig::new()
        .queue_capacity(ITERATIONS)
        .max_total_outstanding(128_000)
}

fn requests(topic: &Topic<u64>) -> Vec<PublishRequest<u64>> {
    (0..ITERATIONS)
        .map(|n| PublishRequest::new(topic.clone(), n as u64).expect("valid benchmark request"))
        .collect()
}

fn async_bus(destinations: usize, topic: &Topic<u64>) -> (AsyncEventBus, Vec<qubit_event_bus::AsyncSubscription<u64>>) {
    let spi = Arc::new(AsyncLocalEventBusSpi::new(&config()).expect("local SPI"));
    let bus = AsyncEventBus::from_spi(qubit_event_bus::model::ProviderId::new("bench").unwrap(), spi).unwrap();
    let mut subscriptions = Vec::with_capacity(destinations);
    for index in 0..destinations {
        let subscriber_id = format!("sink-{index}");
        let request = SubscribeRequest::new(&subscriber_id, topic.clone()).unwrap();
        subscriptions.push(block_on(bus.subscribe(request)).expect("subscription"));
    }
    (bus, subscriptions)
}

fn sync_bus(destinations: usize, topic: &Topic<u64>) -> (EventBus, Vec<qubit_event_bus::Subscription>) {
    let scheduling = DeliverySchedulingConfig::new(
        NonZeroUsize::new(128).unwrap(),
        NonZeroUsize::new(128_000).unwrap(),
        NonZeroUsize::new(128_000).unwrap(),
        NonZeroUsize::new(128).unwrap(),
    )
    .unwrap();
    let (bus, _) = EventBus::local_with_config_and_monitor(
        config(),
        EventBusFacadeConfig::default().with_delivery_scheduling(scheduling),
    )
    .expect("local event bus");
    let mut subscriptions = Vec::with_capacity(destinations);
    for index in 0..destinations {
        let subscriber_id = format!("sink-{index}");
        let request = SubscribeRequest::new(&subscriber_id, topic.clone()).unwrap();
        subscriptions.push(
            bus.subscribe(request, |_| Ok::<(), qubit_event_bus::DeliveryError>(()))
                .unwrap(),
        );
    }
    (bus, subscriptions)
}

fn emit(name: &str, destinations: usize, samples: Vec<u128>, boundary: &str) {
    println!(
        "case={name} destinations={destinations} raw_sample_ns={samples:?} messages={ITERATIONS} boundary={boundary}"
    );
}

fn run_async(destinations: usize, checked: bool) {
    let mut samples = Vec::new();
    for batch_index in 0..(WARMUPS + SAMPLES) {
        let topic = Topic::<u64>::new("bench.facade").unwrap();
        let (bus, subscriptions_live_for_sample) = async_bus(destinations, &topic);
        let batch = requests(&topic);
        let started = Instant::now();
        for request in batch {
            if checked {
                let result =
                    block_on(bus.publish_checked(black_box(request), AdmissionRequirement::AtLeastOneAccepted));
                if destinations == 0 {
                    assert!(matches!(
                        result,
                        Err(CheckedPublishError::Admission {
                            reason: AdmissionCheckError::NoAcceptedDestination,
                            ..
                        })
                    ));
                } else {
                    let receipt = result.expect("checked admission succeeds");
                    assert_eq!(receipt.admission_summary().unwrap().accepted, destinations);
                }
            } else {
                let receipt = block_on(bus.publish(black_box(request))).expect("ordinary publish succeeds");
                assert_eq!(
                    receipt.admission_summary().map_or(0, |summary| summary.accepted),
                    destinations
                );
            }
        }
        let elapsed = started.elapsed().as_nanos();
        if batch_index >= WARMUPS {
            samples.push(elapsed);
        }
        drop(subscriptions_live_for_sample);
        let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    }
    emit(
        if checked { "async_checked" } else { "async_publish" },
        destinations,
        samples,
        "facade_publish_loop_including_local_admission",
    );
}

fn run_sync(destinations: usize, checked: bool) {
    let mut samples = Vec::new();
    for batch_index in 0..(WARMUPS + SAMPLES) {
        let topic = Topic::<u64>::new("bench.facade").unwrap();
        let (bus, subscriptions) = sync_bus(destinations, &topic);
        let batch = requests(&topic);
        let started = Instant::now();
        for request in batch {
            if checked {
                let result = bus.publish_checked(black_box(request), AdmissionRequirement::AtLeastOneAccepted);
                if destinations == 0 {
                    assert!(matches!(
                        result,
                        Err(CheckedPublishError::Admission {
                            reason: AdmissionCheckError::NoAcceptedDestination,
                            ..
                        })
                    ));
                } else {
                    assert_eq!(
                        result
                            .expect("checked admission succeeds")
                            .admission_summary()
                            .unwrap()
                            .accepted,
                        destinations
                    );
                }
            } else {
                let receipt = bus.publish(black_box(request)).expect("ordinary publish succeeds");
                assert_eq!(
                    receipt.admission_summary().map_or(0, |summary| summary.accepted),
                    destinations
                );
            }
        }
        let elapsed = started.elapsed().as_nanos();
        if batch_index >= WARMUPS {
            samples.push(elapsed);
        }
        drop(subscriptions);
        let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
    }
    emit(
        if checked { "sync_checked" } else { "sync_publish" },
        destinations,
        samples,
        "facade_publish_loop_including_local_admission",
    );
}

fn run_notification(destinations: usize) {
    let mut enqueue_samples = Vec::new();
    let mut drain_samples = Vec::new();
    for sample in 0..(WARMUPS + SAMPLES) {
        let topic = Topic::<u64>::new("bench.notification").unwrap();
        let (bus, subscriptions) = sync_bus(destinations, &topic);
        let publisher = NotificationPublisher::new(
            bus.clone(),
            topic.clone(),
            NonZeroUsize::new(ITERATIONS).unwrap(),
            |_| {},
        )
        .unwrap();
        let started = Instant::now();
        for value in 0..ITERATIONS {
            publisher
                .try_publish(value as u64)
                .expect("notification queue must accept all messages");
        }
        let enqueue = started.elapsed().as_nanos();
        let drain_started = Instant::now();
        publisher.close().unwrap();
        let drain = drain_started.elapsed().as_nanos();
        let stats = publisher.stats();
        assert_eq!(stats.published(), ITERATIONS as u64);
        assert_eq!(
            stats.publish_errors()
                + stats.request_errors()
                + stats.queue_full()
                + stats.queue_closed()
                + stats.observer_panicked()
                + stats.worker_panicked(),
            0
        );
        assert_eq!(stats.enqueued(), ITERATIONS as u64);
        if destinations == 0 {
            assert_eq!(stats.admission_no_destinations(), ITERATIONS as u64);
        } else {
            assert_eq!(stats.admission_accepted(), ITERATIONS as u64);
        }
        if sample >= WARMUPS {
            enqueue_samples.push(enqueue);
            drain_samples.push(enqueue + drain);
        }
        drop(subscriptions);
        let _ = bus.shutdown(ShutdownMode::Immediate).unwrap();
    }
    emit(
        "notification_enqueue",
        destinations,
        enqueue_samples,
        "try_publish_loop_only",
    );
    emit(
        "notification_drain_inclusive",
        destinations,
        drain_samples,
        "enqueue_loop_through_close_and_worker_drain",
    );
}

fn main() {
    println!("publish_receipt_size_bytes={}", size_of::<PublishReceipt>());
    // Preserve the original no-subscriber measurement with two warmups and
    // seven raw samples of 10,000 facade publishes each.
    let mut original_samples = Vec::with_capacity(SAMPLES);
    for sample in 0..(WARMUPS + SAMPLES) {
        let spi = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap());
        let bus = AsyncEventBus::from_spi(qubit_event_bus::model::ProviderId::new("bench").unwrap(), spi).unwrap();
        let topic = Topic::new("bench.async.publish").unwrap();
        let requests = (0..10_000)
            .map(|n| PublishRequest::new(topic.clone(), n).unwrap())
            .collect::<Vec<_>>();
        let started = Instant::now();
        for request in requests {
            let receipt = block_on(bus.publish(request)).unwrap();
            assert_eq!(receipt.admission_summary().unwrap().accepted, 0);
        }
        let elapsed = started.elapsed().as_nanos();
        if sample >= WARMUPS {
            original_samples.push(elapsed);
        }
        let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    }
    println!("case=original_no_subscribers raw_sample_ns={original_samples:?} messages=10000");

    for destinations in DESTINATIONS {
        run_sync(destinations, false);
        run_sync(destinations, true);
        run_async(destinations, false);
        run_async(destinations, true);
        run_notification(destinations);
    }
}
