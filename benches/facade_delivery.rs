// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures caller-driven asynchronous facade publication on the local SPI.

use std::future::Future;
use std::hint::black_box;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::yield_now;
use std::time::Instant;

use qubit_event_bus::facade::AsyncEventBus;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const ITERATIONS: usize = 10_000;

/// Drives a benchmark future by polling and yielding while it is pending.
///
/// # Type Parameters
/// - `F`: Future to poll.
///
/// # Parameters
/// - `future`: Operation driven to completion.
///
/// # Returns
/// The future's output.
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

/// Measures one run of local asynchronous publication operations.
///
/// # Returns
/// Average elapsed nanoseconds per publication in this sample.
fn sample() -> u128 {
    let spi = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap());
    let bus = AsyncEventBus::from_spi(ProviderId::new("bench").unwrap(), spi).unwrap();
    let topic = Topic::new("bench.async.publish").unwrap();
    let requests = (0..ITERATIONS)
        .map(|value| PublishRequest::new(topic.clone(), value).unwrap())
        .collect::<Vec<_>>();
    let mut successes = 0;
    let start = Instant::now();
    for request in requests {
        successes += usize::from(black_box(block_on(bus.publish(black_box(request)))).is_ok());
    }
    let elapsed = start.elapsed().as_nanos();
    assert_eq!(successes, ITERATIONS, "publication sample failed");
    let shutdown_report = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    assert_eq!(shutdown_report.outcome, ShutdownOutcome::Complete);
    assert_eq!(shutdown_report.known_abandoned_deliveries, 0);
    assert!(shutdown_report.provider_may_have_abandoned_deliveries);
    elapsed / ITERATIONS as u128
}

/// Prints raw sample means and their median and nearest-rank sample-mean p95.
/// These are publication batches, not a per-delivery latency distribution.
fn main() {
    for _ in 0..WARMUPS {
        black_box(sample());
    }
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        samples.push(sample());
    }
    println!(
        "async local publish raw_sample_mean_ns={samples:?} iterations={ITERATIONS} boundary=publish_no_subscribers"
    );
    samples.sort_unstable();
    println!(
        "async local publish: median={} ns/op p95={} ns/op ({} operations/sample)",
        samples[SAMPLES / 2],
        samples[(SAMPLES * 95).div_ceil(100) - 1],
        ITERATIONS
    );
}
