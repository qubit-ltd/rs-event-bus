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
use std::time::Instant;

use qubit_event_bus::facade::AsyncEventBus;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const ITERATIONS: usize = 10_000;

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn sample() -> u128 {
    let spi = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap());
    let bus = AsyncEventBus::from_spi(ProviderId::new("bench").unwrap(), spi).unwrap();
    let topic = Topic::new("bench.async.publish").unwrap();
    let start = Instant::now();
    for value in 0..ITERATIONS {
        block_on(bus.publish(black_box(PublishRequest::new(topic.clone(), value).unwrap()))).unwrap();
    }
    let elapsed = start.elapsed().as_nanos();
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    elapsed / ITERATIONS as u128
}

fn main() {
    for _ in 0..WARMUPS {
        black_box(sample());
    }
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        samples.push(sample());
    }
    samples.sort_unstable();
    println!(
        "async local publish: median={} ns/op p95={} ns/op ({} operations/sample)",
        samples[SAMPLES / 2],
        samples[(SAMPLES * 95).div_ceil(100) - 1],
        ITERATIONS
    );
}
