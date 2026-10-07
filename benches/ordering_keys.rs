// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures ordering through the current public asynchronous facade.
//!
//! Setup/publication and the blocked first wave are outside timing. Samples
//! report nanoseconds per handled delivery, not legacy registry handoff cost.
//! The retired registry-only workload has no current throughput counterpart.

// Additional public-facade workloads have separate end-to-settlement timing.
mod ordering_scenarios;

use std::env;
use std::future::Future;
use std::future::poll_fn;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread;
use std::time::Instant;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const KEYS: [usize; 4] = [4, 64, 1024, 4096];

/// One independently prepared sample and its handoff observations.
#[must_use = "benchmark samples must be reported or inspected"]
#[derive(Debug)]
struct Sample {
    /// End-to-end elapsed nanoseconds divided by completed operations.
    ns_per_operation: u128,
    /// Handlers observed concurrently in the blocked first wave.
    observed: usize,
    /// Calling-thread CPU nanoseconds over the complete measured interval.
    cpu_ns: u128,
    /// Process CPU nanoseconds, including any process-owned timer thread.
    process_cpu_ns: u128,
    /// Wall-clock nanoseconds over the complete measured interval.
    wall_ns: u128,
}

/// Native Linux clock result used only by this standalone measurement target.
#[repr(C)]
#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
struct CpuTime {
    /// Whole elapsed CPU seconds.
    seconds: i64,
    /// Fractional elapsed CPU nanoseconds.
    nanoseconds: i64,
}

#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
unsafe extern "C" {
    /// Reads the native CPU `clock` into the writable C timespec pointer.
    /// Returns zero on success or minus one when that clock cannot be read.
    fn clock_gettime(clock: i32, time: *mut CpuTime) -> i32;
}

/// Reads Linux CPU time: `clock` is 3 for thread CPU or 2 for process CPU.
/// This benchmark currently requires 64-bit Linux; failed clock reads panic.
#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
fn cpu_nanoseconds(clock: i32) -> u128 {
    let mut time = CpuTime {
        seconds: 0,
        nanoseconds: 0,
    };
    // SAFETY: Linux clock IDs 2 and 3 are process and thread CPU clocks. The C
    // layout matches this 64-bit Linux target and the pointer is valid for one
    // write.
    assert_eq!(
        unsafe { clock_gettime(clock, &mut time) },
        0,
        "CPU clock is unavailable"
    );
    time.seconds as u128 * 1_000_000_000 + time.nanoseconds as u128
}

/// Reports unsupported measurement hosts without affecting target compilation.
/// `clock` is unused; running this benchmark outside 64-bit Linux panics.
#[cfg(not(all(target_os = "linux", target_pointer_width = "64")))]
fn cpu_nanoseconds(_clock: i32) -> u128 {
    panic!("CPU-validated ordering measurements require 64-bit Linux");
}

/// Drives a future to completion without an external executor.
///
/// `future` is repeatedly polled; pending work yields the calling thread.
/// Returns its output. Benchmark fixtures must not need an independent driver.
fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::yield_now(),
        }
    }
}

/// Builds the fixed keyed publication input for one ordering workload.
fn build_requests(
    topic: &Topic<usize>,
    active_keys: usize,
    workload: &str,
    operations: usize,
) -> Vec<PublishRequest<usize>> {
    (0..operations)
        .map(|index| {
            let key = match workload {
                "same-key" => 0,
                "churn" => index,
                _ => index % active_keys,
            };
            PublishRequest::builder()
                .topic(topic.clone())
                .payload(index)
                .ordering_key(format!("key-{key}"))
                .build()
                .unwrap()
        })
        .collect()
}

/// Polls the gated subscription until the expected first-wave size is reached.
fn observe_first_wave<F: Future>(
    runner: &mut Pin<Box<F>>,
    context: &mut Context<'_>,
    peak: &AtomicUsize,
    expected: usize,
    operations: usize,
) -> usize {
    // The shared scheduler bounds work per poll; fill the blocked first wave
    // with explicit executor turns before starting measurement.
    for _ in 0..operations * 4 {
        assert!(runner.as_mut().poll(context).is_pending());
        if peak.load(Ordering::Relaxed) == expected {
            break;
        }
    }
    let observed = peak.load(Ordering::Relaxed);
    assert_eq!(observed, expected);
    observed
}

/// Times delivery completion after releasing the prepared handler wave.
fn timed_drain<F: Future>(
    runner: &mut Pin<Box<F>>,
    context: &mut Context<'_>,
    released: &AtomicBool,
    completed: &AtomicUsize,
    operations: usize,
) -> (u128, u128, u128, bool) {
    let process_cpu_start = cpu_nanoseconds(2);
    let cpu_start = cpu_nanoseconds(3);
    let start = Instant::now();
    released.store(true, Ordering::Relaxed);
    let mut runner_stopped = false;
    // Complete the fixed operation count; CI/job timeouts bound a hung run
    // without making elapsed time a benchmark correctness threshold.
    while completed.load(Ordering::Relaxed) != operations {
        if black_box(runner.as_mut().poll(context)).is_ready() {
            runner_stopped = true;
            break;
        }
    }
    let elapsed = start.elapsed().as_nanos();
    let cpu_ns = cpu_nanoseconds(3) - cpu_start;
    let process_cpu_ns = cpu_nanoseconds(2) - process_cpu_start;
    (elapsed, cpu_ns, process_cpu_ns, runner_stopped)
}

/// Measures a real public facade, with setup and publication outside timing.
///
/// The first wave is held at the handler until timing begins, proving the
/// observed admission cardinality. Returns ns/delivery and observed peak
/// handlers. `active_keys` is the input key cardinality, not a promise that
/// default admission can keep all these lanes simultaneously active. Same-key
/// input has one distinct key; churn has one distinct key per operation.
fn facade_sample(active_keys: usize, limit: usize, workload: &str, operations: usize) -> Sample {
    let spi = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new().queue_capacity(operations)).unwrap());
    let config = EventBusFacadeConfig::new().with_delivery_scheduling(
        DeliverySchedulingConfig::new(
            NonZeroUsize::new(limit).unwrap(),
            NonZeroUsize::new(limit).unwrap(),
            NonZeroUsize::new(limit).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap(),
    );
    let bus = AsyncEventBus::with_config(ProviderId::new("ordering-bench").unwrap(), spi, config).unwrap();
    let topic = Topic::<usize>::new("bench.ordering.facade").unwrap();
    let options = SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("ordering-bench", topic.clone())
                .unwrap()
                .with_options(options),
        ),
    )
    .unwrap();
    let requests = build_requests(&topic, active_keys, workload, operations);
    let initial_queue = if workload == "same-key" {
        operations.min(64)
    } else {
        operations
    };
    let mut requests = requests.into_iter();
    for request in requests.by_ref().take(initial_queue) {
        let _ = black_box(block_on(bus.publish(request)).unwrap());
    }
    let released = Arc::new(AtomicBool::new(false));
    let completed = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let handler_release = released.clone();
    let handler_completed = completed.clone();
    let handler_active = active.clone();
    let handler_peak = peak.clone();
    let mut runner = Box::pin(subscription.run(move |delivery| {
        black_box(delivery);
        let current = handler_active.fetch_add(1, Ordering::Relaxed) + 1;
        handler_peak.fetch_max(current, Ordering::Relaxed);
        let released = handler_release.clone();
        let completed = handler_completed.clone();
        let active = handler_active.clone();
        poll_fn(move |_| {
            if released.load(Ordering::Relaxed) {
                active.fetch_sub(1, Ordering::Relaxed);
                completed.fetch_add(1, Ordering::Relaxed);
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        })
    }));
    let mut context = Context::from_waker(Waker::noop());
    let expected_first_wave = match workload {
        "same-key" => 1,
        "churn" => limit.min(operations),
        _ => limit.min(active_keys).min(operations),
    };
    let first_wave = observe_first_wave(&mut runner, &mut context, &peak, expected_first_wave, operations);
    // Complete publication outside timing without polling the gated runner.
    // The fixed same-key setup backlog matches the previous facade workload.
    for request in requests {
        let _ = black_box(block_on(bus.publish(request)).unwrap());
    }
    let (elapsed, cpu_ns, process_cpu_ns, runner_stopped) =
        timed_drain(&mut runner, &mut context, &released, &completed, operations);
    drop(runner);
    let shutdown_report = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    assert_eq!(shutdown_report.outcome, ShutdownOutcome::Complete);
    assert_eq!(shutdown_report.known_abandoned_deliveries, 0);
    assert!(shutdown_report.provider_may_have_abandoned_deliveries);
    assert!(!runner_stopped, "runner stopped before completing the sample");
    assert_eq!(
        black_box(completed.load(Ordering::Relaxed)),
        operations,
        "sample did not complete all deliveries"
    );
    Sample {
        ns_per_operation: elapsed / operations as u128,
        observed: first_wave,
        cpu_ns,
        process_cpu_ns,
        wall_ns: elapsed,
    }
}

/// Prints seven raw valid samples, median, range and median absolute deviation.
/// `label` describes the measured boundary; `run` creates one fresh fixture.
fn measure(label: &str, mut run: impl FnMut() -> Sample) {
    if let Ok(filter) = env::var("ORDERING_FILTER")
        && label != filter
    {
        return;
    }
    for _ in 0..WARMUPS {
        let _ = black_box(run());
    }
    let mut samples = Vec::with_capacity(SAMPLES);
    let mut discarded = 0;
    for _ in 0..256 {
        let sample = run();
        let applicable_cpu = sample.process_cpu_ns;
        if applicable_cpu * 100 >= sample.wall_ns * 98 {
            samples.push(sample);
            if samples.len() == SAMPLES {
                break;
            }
        } else {
            println!("rejected {label} sample={sample:?} reason=timed_cpu_wall_below_98_percent");
            discarded += 1;
        }
    }
    assert_eq!(
        samples.len(),
        SAMPLES,
        "background scheduling prevented seven valid samples for {label}"
    );
    let observations = samples.iter().map(|sample| sample.observed).collect::<Vec<_>>();
    let thread_cpu = samples.iter().map(|sample| sample.cpu_ns).collect::<Vec<_>>();
    let mut times = samples.iter().map(|sample| sample.ns_per_operation).collect::<Vec<_>>();
    times.sort_unstable();
    let median = times[SAMPLES / 2];
    let mut deviations = times.iter().map(|time| time.abs_diff(median)).collect::<Vec<_>>();
    deviations.sort_unstable();
    println!(
        concat!(
            "{label} samples={samples:?} observed={observations:?} ",
            "thread_cpu_ns={thread_cpu:?} discarded={discarded} median_ns={median} ",
            "min_ns={} max_ns={} mad_ns={}"
        ),
        times[0],
        times[SAMPLES - 1],
        deviations[SAMPLES / 2],
        label = label,
        samples = samples,
        observations = observations,
        thread_cpu = thread_cpu,
        discarded = discarded,
        median = median,
    );
}

/// Runs the full matrix, or one long profile workload selected by environment.
/// Profile mode drives the public facade and records no wall-clock gate.
fn main() {
    if env::var_os("ORDERING_SCENARIOS").is_some() {
        ordering_scenarios::run(env::var_os("ORDERING_SMOKE").is_some());
        return;
    }
    if env::var_os("ORDERING_SMOKE").is_some() {
        for workload in ["balanced", "same-key", "churn"] {
            for window in [4, 64] {
                let _ = black_box(facade_sample(8, window, workload, 2048));
            }
        }
        println!("ordering workload invariants passed; smoke timings are discarded");
        return;
    }
    if env::var_os("ORDERING_PROFILE").is_some() {
        let operations = env::var("ORDERING_PROFILE_OPERATIONS")
            .map(|value| value.parse().expect("profile operations must be an integer"))
            .unwrap_or(65_536);
        let _ = black_box(facade_sample(4096, 64, "balanced", operations));
        return;
    }
    println!(
        concat!(
            "ordering_keys warmups={WARMUPS} samples={SAMPLES} boundary=public_facade ",
            "unit=ns_per_handled_delivery registry_workload=retired"
        ),
        WARMUPS = WARMUPS,
        SAMPLES = SAMPLES
    );
    for active_keys in KEYS {
        for limit in [4, active_keys.max(64)] {
            for workload in ["balanced", "same-key", "churn"] {
                let distinct_keys = match workload {
                    "same-key" => 1,
                    "churn" => active_keys.max(1024) * 2,
                    _ => active_keys,
                };
                measure(
                    &format!(
                        concat!(
                            "facade case_keys={active_keys} distinct_input_keys={distinct_keys} ",
                            "running_limit={limit} owned_limit={limit} workload={workload}"
                        ),
                        active_keys = active_keys,
                        distinct_keys = distinct_keys,
                        limit = limit,
                        workload = workload
                    ),
                    || facade_sample(active_keys, limit, workload, active_keys.max(1024) * 2),
                );
            }
        }
    }
}
