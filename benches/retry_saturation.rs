// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures synchronous handler-pool saturation while four handlers back off.

use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::ShutdownMode;
use qubit_retry::BackoffPolicy;
use qubit_retry::RetryPolicy;

const HANDLER_SLOTS: usize = 4;
const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_BACKOFF: Duration = Duration::from_secs(1);
const PUBLISH_OFFSET: Duration = Duration::from_millis(10);

/// Runs one fresh local bus sample and returns independent-message queue wait.
fn sample() -> io::Result<Duration> {
    let registry = EventBusRegistry::with_local().map_err(io::Error::other)?;
    let positive = |value| NonZeroUsize::new(value).expect("benchmark limits are positive");
    let scheduling = DeliverySchedulingConfig::new(
        positive(HANDLER_SLOTS),
        positive(HANDLER_SLOTS * 2),
        positive(2),
        positive(HANDLER_SLOTS + 1),
    )
    .map_err(io::Error::other)?;
    let facade = EventBusFacadeConfig::new().with_delivery_scheduling(scheduling);
    let config = EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::default().provider_options())
        .with_facade_config(facade);
    let bus = registry.create(&config).map_err(io::Error::other)?;
    let result = run_sample(&bus);
    let shutdown_result = bus
        .shutdown(ShutdownMode::Immediate)
        .map_err(io::Error::other);
    match (result, shutdown_result) {
        (Ok(wait), Ok(_)) => Ok(wait),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

/// Creates four occupied handlers, then measures one independent delivery.
fn run_sample(bus: &EventBus) -> io::Result<Duration> {
    let barrier = Arc::new(Barrier::new(HANDLER_SLOTS + 1));
    let retry_options = SubscribeOptions::<u8>::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(2)
                .backoff(BackoffPolicy::fixed(RETRY_BACKOFF))
                .build()
                .map_err(io::Error::other)?,
        )
        .build();

    for index in 0..HANDLER_SLOTS {
        let topic = Topic::<u8>::new(&format!("retry-saturation.slot-{index}"))
            .map_err(io::Error::other)?;
        let request =
            SubscribeRequest::new(&format!("retry-saturation-slot-{index}"), topic.clone())
                .map_err(io::Error::other)?
                .with_options(retry_options.clone());
        let barrier = barrier.clone();
        let first_attempt = Arc::new(AtomicBool::new(true));
        let _ = bus
            .subscribe(request, move |_| {
                if first_attempt.swap(false, Ordering::AcqRel) {
                    barrier.wait();
                    Err(DeliveryError::Handler {
                        source: Box::new(io::Error::other("intentional first-attempt failure")),
                    })
                } else {
                    Ok(())
                }
            })
            .map_err(io::Error::other)?;
    }

    let (started_tx, started_rx) = mpsc::channel();
    let independent_topic =
        Topic::<u8>::new("retry-saturation.independent").map_err(io::Error::other)?;
    let independent_request =
        SubscribeRequest::new("retry-saturation-independent", independent_topic.clone())
            .map_err(io::Error::other)?;
    let _ = bus
        .subscribe(independent_request, move |_| {
            started_tx
                .send(Instant::now())
                .map_err(|error| DeliveryError::Handler {
                    source: Box::new(io::Error::other(error.to_string())),
                })
        })
        .map_err(io::Error::other)?;

    for index in 0..HANDLER_SLOTS {
        let topic = Topic::<u8>::new(&format!("retry-saturation.slot-{index}"))
            .map_err(io::Error::other)?;
        let _ = bus
            .publish(PublishRequest::new(topic, index as u8).map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
    }

    barrier.wait();
    thread::sleep(PUBLISH_OFFSET);
    let publish_started = Instant::now();
    let _ = bus
        .publish(PublishRequest::new(independent_topic, 0).map_err(io::Error::other)?)
        .map_err(io::Error::other)?;
    let handler_started = started_rx
        .recv_timeout(SAMPLE_TIMEOUT)
        .map_err(io::Error::other)?;
    Ok(handler_started.duration_since(publish_started))
}

/// Runs warmups and prints seven independent-message wait measurements.
fn main() -> io::Result<()> {
    for _ in 0..WARMUPS {
        sample()?;
    }

    let mut waits = Vec::with_capacity(SAMPLES);
    println!("iteration,independent_handler_wait_ms");
    for iteration in 1..=SAMPLES {
        let wait = sample()?;
        println!("{iteration},{}", wait.as_secs_f64() * 1_000.0);
        waits.push(wait);
    }

    waits.sort_unstable();
    println!(
        "median_ms,{}",
        waits[waits.len() / 2].as_secs_f64() * 1_000.0
    );
    Ok(())
}
