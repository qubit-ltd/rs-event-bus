// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures synchronous handler-pool saturation while four handlers back off.

use std::collections::HashSet;
use std::hint::black_box;
use std::io;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
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
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_retry::BackoffPolicy;
use qubit_retry::RetryPolicy;

const HANDLER_SLOTS: usize = 4;
const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const SAMPLE_TIMEOUT: Duration = Duration::from_secs(5);
const RETRY_BACKOFF: Duration = Duration::from_secs(1);
const PUBLISH_OFFSET: Duration = Duration::from_millis(10);

/// Shared release state and wakeup signal for callbacks parked during a sample.
type Gate = Arc<(Mutex<bool>, Condvar)>;

/// Releases all callbacks waiting at the sample gate when measurement exits.
struct ReleaseGateOnDrop(Gate);

impl ReleaseGateOnDrop {
    /// Opens the sample gate and wakes every callback waiting on it.
    fn release(&self) {
        let (released, condition) = &*self.0;
        *released.lock().unwrap_or_else(PoisonError::into_inner) = true;
        condition.notify_all();
    }
}

impl Drop for ReleaseGateOnDrop {
    fn drop(&mut self) {
        self.release();
    }
}

/// Blocks a callback until measurement releases the shared sample gate.
fn wait_for_gate(gate: &Gate) {
    let (released, condition) = &**gate;
    let mut released = released.lock().unwrap_or_else(PoisonError::into_inner);
    while !*released {
        released = condition.wait(released).unwrap_or_else(PoisonError::into_inner);
    }
}

/// Runs one fresh local bus sample and returns independent-message queue wait.
fn retry_saturation_sample(available_handler_slots: usize) -> io::Result<Duration> {
    let registry = EventBusRegistry::with_local().map_err(io::Error::other)?;
    let positive = |value| NonZeroUsize::new(value).expect("benchmark limits are positive");
    let scheduling = DeliverySchedulingConfig::new(
        positive(available_handler_slots),
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
    let result = measure_independent_delivery_wait(&bus);
    let shutdown_result = bus
        .request_shutdown(ShutdownMode::Immediate)
        .map_err(io::Error::other)
        .and_then(|ticket| ticket.wait(Some(SAMPLE_TIMEOUT)).map_err(io::Error::other))
        .and_then(|report| {
            if report.outcome == ShutdownOutcome::Complete {
                Ok(report)
            } else {
                Err(io::Error::other(format!(
                    "sample cleanup completed with unexpected outcome {:?}",
                    report.outcome
                )))
            }
        });
    match (result, shutdown_result) {
        (Ok(wait), Ok(_)) => Ok(wait),
        (Err(measurement), Ok(_)) => Err(measurement),
        (Ok(_), Err(cleanup)) => Err(io::Error::new(
            cleanup.kind(),
            format!("sample cleanup failed: {cleanup}"),
        )),
        (Err(measurement), Err(cleanup)) => Err(io::Error::new(
            measurement.kind(),
            format!("{measurement}; sample cleanup also failed: {cleanup}"),
        )),
    }
}

/// Occupies four handler positions, then measures one independent delivery.
fn measure_independent_delivery_wait(bus: &EventBus) -> io::Result<Duration> {
    let (entry_tx, entry_rx) = mpsc::channel();
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    // Install the release guard before any publish can start a callback.
    let release_gate = ReleaseGateOnDrop(gate.clone());
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
        let topic = Topic::<u8>::new(&format!("retry-saturation.slot-{index}")).map_err(io::Error::other)?;
        let request = SubscribeRequest::new(&format!("retry-saturation-slot-{index}"), topic.clone())
            .map_err(io::Error::other)?
            .with_options(retry_options.clone());
        let gate = gate.clone();
        let entry_tx = entry_tx.clone();
        let first_attempt = Arc::new(AtomicBool::new(true));
        let _ = bus
            .subscribe(request, move |_| {
                if first_attempt.swap(false, Ordering::AcqRel) {
                    if entry_tx.send(index).is_err() {
                        return Err(DeliveryError::Handler {
                            source: Box::new(io::Error::other("benchmark entry receiver was dropped")),
                        });
                    }
                    wait_for_gate(&gate);
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
    let independent_topic = Topic::<u8>::new("retry-saturation.independent").map_err(io::Error::other)?;
    let independent_request =
        SubscribeRequest::new("retry-saturation-independent", independent_topic.clone()).map_err(io::Error::other)?;
    let _ = bus
        .subscribe(independent_request, move |_| {
            started_tx.send(Instant::now()).map_err(|error| DeliveryError::Handler {
                source: Box::new(io::Error::other(error.to_string())),
            })
        })
        .map_err(io::Error::other)?;

    for index in 0..HANDLER_SLOTS {
        let topic = Topic::<u8>::new(&format!("retry-saturation.slot-{index}")).map_err(io::Error::other)?;
        let _ = bus
            .publish(PublishRequest::new(topic, index as u8).map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
    }

    let deadline = Instant::now() + SAMPLE_TIMEOUT;
    let mut entered = HashSet::with_capacity(HANDLER_SLOTS);
    while entered.len() < HANDLER_SLOTS {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(entry_timeout(entered.len()));
        }
        match entry_rx.recv_timeout(remaining) {
            Ok(index) => {
                entered.insert(index);
            }
            Err(mpsc::RecvTimeoutError::Timeout) => return Err(entry_timeout(entered.len())),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(io::Error::other(format!(
                    "handler entry channel disconnected: expected {HANDLER_SLOTS}, observed {}",
                    entered.len()
                )));
            }
        }
    }

    release_gate.release();
    thread::sleep(PUBLISH_OFFSET);
    let publish_started = Instant::now();
    let _receipt = bus
        .publish(PublishRequest::new(independent_topic, 0).map_err(io::Error::other)?)
        .map_err(io::Error::other)?;
    let handler_started = started_rx.recv_timeout(SAMPLE_TIMEOUT).map_err(|error| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("independent handler did not start: {error}"),
        )
    })?;
    drop(release_gate);
    Ok(handler_started.duration_since(publish_started))
}

/// Builds a timeout error that reports how many handlers entered the sample.
fn entry_timeout(observed: usize) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!("handler entry timed out: expected {HANDLER_SLOTS}, observed {observed}"),
    )
}

/// Runs warmups and prints seven independent-message wait measurements.
fn main() -> io::Result<()> {
    if std::env::args().any(|argument| argument == "--probe-entry-timeout") {
        return retry_saturation_sample(HANDLER_SLOTS - 1).map(|_| ());
    }

    for _ in 0..WARMUPS {
        black_box(retry_saturation_sample(HANDLER_SLOTS)?);
    }

    let mut waits = Vec::with_capacity(SAMPLES);
    println!("iteration,independent_handler_wait_ms");
    for iteration in 1..=SAMPLES {
        let wait = black_box(retry_saturation_sample(HANDLER_SLOTS)?);
        println!("{iteration},{}", wait.as_secs_f64() * 1_000.0);
        waits.push(wait);
    }

    waits.sort_unstable();
    println!("median_ms,{}", waits[waits.len() / 2].as_secs_f64() * 1_000.0);
    Ok(())
}
