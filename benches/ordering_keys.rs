// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures actual ordering code and separately observes facade admission.
//!
//! Registry stress deliberately retains one guard per key outside the ticket
//! window. Its window is not a claim about facade admission: the public facade
//! workload reports the number of handlers it actually admits.

#[path = "../src/pipeline/ordering_lane/internal"]
mod ordering {
    mod async_lane;
    mod async_lane_state;
    #[allow(dead_code)] // The benchmark exercises handoff rather than inspecting the value.
    mod async_ordering_guard;
    mod async_ordering_lanes;
    mod async_ordering_turn;
    mod ordering_lane_key;

    pub(super) use async_ordering_guard::AsyncOrderingGuard;
    pub(super) use async_ordering_lanes::AsyncOrderingLanes;
    pub(super) use ordering_lane_key::OrderingLaneKey;
}

use std::future::Future;
use std::hint::black_box;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Instant;

use ordering::AsyncOrderingGuard;
use ordering::AsyncOrderingLanes;
use ordering::OrderingLaneKey;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::DeliveryAdmissionConfig;
use qubit_event_bus::EventBusFacadeConfig;
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
use qubit_id::Id;

const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const OPERATIONS: usize = 32_768;
const KEYS: [usize; 4] = [4, 64, 1024, 4096];

/// One independently prepared sample and its handoff observations.
#[derive(Debug)]
struct Sample {
    /// End-to-end elapsed nanoseconds divided by completed operations.
    ns_per_operation: u128,
    /// Pending registrations for registry stress, or facade first-wave
    /// handlers.
    observed: usize,
    /// Median measured queue-to-handoff latency, zero for facade-only samples.
    wait_median_ns: u128,
    /// 95th percentile measured queue-to-handoff latency.
    wait_p95_ns: u128,
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
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

/// Acquires a ready turn, panicking if the prepared lane is unexpectedly busy.
/// Returns the guard that keeps the real registry entry alive.
fn acquire<T>(future: impl Future<Output = Option<AsyncOrderingGuard<T>>>) -> AsyncOrderingGuard<T> {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut context) {
        Poll::Ready(Some(guard)) => guard,
        _ => panic!("prepared lane must be ready"),
    }
}

/// Generates lane identities before measurement for one fixed subscription.
/// `count` distinct identities are returned; no message construction is timed.
fn keys(count: usize) -> Vec<OrderingLaneKey> {
    let subscription = Id::new(1);
    (0..count)
        .map(|index| OrderingLaneKey::new("bench.ordering", Some(&format!("key-{index}")), subscription))
        .collect()
}

/// Measures enqueue, same-key pending registration, handoff and guard release.
///
/// `active_keys` guards stay live; `window` bounds newly queued tickets.
/// `workload` is balanced, same-key, or churn. Churn uses fresh prebuilt keys
/// and replaces guards after release. Returns ns/op and pending-poll count.
/// All keys, vectors, initial lanes and values are prepared outside timing.
fn registry_sample(active_keys: usize, window: usize, workload: &str, operations: usize) -> Sample {
    let lanes = AsyncOrderingLanes::new();
    let initial = keys(active_keys);
    let mut guards = initial
        .iter()
        .map(|key| Some(acquire(lanes.enqueue(key.clone(), 0_usize))))
        .collect::<Vec<_>>();
    let mut inputs = (0..operations)
        .map(|index| {
            let slot = if workload == "same-key" { 0 } else { index % active_keys };
            (slot, Some(initial[slot].clone()), index)
        })
        .collect::<Vec<_>>();
    let mut churn_keys = keys(operations + active_keys).into_iter().map(Some).collect::<Vec<_>>();
    let mut turns = Vec::with_capacity(window);
    let mut waits = Vec::with_capacity(operations);
    let mut context = Context::from_waker(Waker::noop());
    let mut pending = 0;
    let process_cpu_start = cpu_nanoseconds(2);
    let cpu_start = cpu_nanoseconds(3);
    let start = Instant::now();
    let batch_size = if workload == "same-key" {
        window
    } else {
        window.min(active_keys)
    };
    for batch in inputs.chunks_mut(batch_size) {
        if workload == "churn" {
            for (slot, _, value) in batch {
                let queued = Instant::now();
                drop(guards[*slot].take());
                guards[*slot] = Some(acquire(
                    lanes.enqueue(churn_keys[*value].take().unwrap(), black_box(*value)),
                ));
                waits.push(queued.elapsed().as_nanos());
            }
        } else {
            for (slot, key, value) in batch {
                let queued = Instant::now();
                let mut turn = Box::pin(lanes.enqueue(key.take().unwrap(), black_box(*value)));
                assert!(turn.as_mut().poll(&mut context).is_pending());
                pending += 1;
                turns.push((*slot, turn, queued));
            }
            for (slot, turn, queued) in turns.drain(..) {
                drop(guards[slot].take());
                guards[slot] = Some(acquire(turn));
                waits.push(queued.elapsed().as_nanos());
            }
        }
    }
    let elapsed = start.elapsed().as_nanos();
    let cpu_ns = cpu_nanoseconds(3) - cpu_start;
    let process_cpu_ns = cpu_nanoseconds(2) - process_cpu_start;
    black_box(&guards);
    assert_eq!(guards.iter().filter(|guard| guard.is_some()).count(), active_keys);
    waits.sort_unstable();
    Sample {
        ns_per_operation: elapsed / operations as u128,
        observed: pending,
        wait_median_ns: waits[operations / 2],
        wait_p95_ns: waits[(operations * 95).div_ceil(100) - 1],
        cpu_ns,
        process_cpu_ns,
        wall_ns: elapsed,
    }
}

/// Measures a real public facade, with setup and publication outside timing.
///
/// The first wave is held at the handler until timing begins, proving the
/// observed admission cardinality. Returns ns/delivery and observed peak
/// handlers. `active_keys` is the input key cardinality, not a promise that
/// default admission can keep all these lanes simultaneously active. Same-key
/// input has one distinct key; churn has one distinct key per operation.
fn facade_sample(active_keys: usize, limit: usize, workload: &str) -> Sample {
    let operations = active_keys.max(1024) * 2;
    let spi = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new().queue_capacity(operations)).unwrap());
    let config = EventBusFacadeConfig::new().with_delivery_admission(DeliveryAdmissionConfig::new(limit).unwrap());
    let bus = AsyncEventBus::with_config(ProviderId::new("ordering-bench").unwrap(), spi, config).unwrap();
    let topic = Topic::<usize>::new("bench.ordering.facade").unwrap();
    let options = SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let mut subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("ordering-bench", topic.clone())
                .unwrap()
                .with_options(options),
        ),
    )
    .unwrap();
    let requests = (0..operations)
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
        .collect::<Vec<_>>();
    let initial_queue = if workload == "same-key" {
        operations.min(64)
    } else {
        operations
    };
    let mut requests = requests.into_iter();
    for request in requests.by_ref().take(initial_queue) {
        black_box(block_on(bus.publish(request)).unwrap());
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
        std::future::poll_fn(move |_| {
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
    assert!(runner.as_mut().poll(&mut context).is_pending());
    let first_wave = peak.load(Ordering::Relaxed);
    assert_eq!(
        first_wave,
        match workload {
            "same-key" => 1,
            "churn" => limit.min(operations),
            _ => limit.min(active_keys),
        }
    );
    // Complete publication outside timing without polling the gated runner.
    // A fixed 64-ticket same-key backlog exercises serialization without an
    // unmeasured cubic setup cost from repeatedly polling thousands of waiters.
    for request in requests {
        black_box(block_on(bus.publish(request)).unwrap());
    }
    let process_cpu_start = cpu_nanoseconds(2);
    let cpu_start = cpu_nanoseconds(3);
    let start = Instant::now();
    released.store(true, Ordering::Relaxed);
    while completed.load(Ordering::Relaxed) != operations {
        assert!(runner.as_mut().poll(&mut context).is_pending());
    }
    let elapsed = start.elapsed().as_nanos();
    let cpu_ns = cpu_nanoseconds(3) - cpu_start;
    let process_cpu_ns = cpu_nanoseconds(2) - process_cpu_start;
    drop(runner);
    let shutdown_report = block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    assert_eq!(shutdown_report.outcome, ShutdownOutcome::Complete);
    assert_eq!(shutdown_report.known_abandoned_deliveries, 0);
    assert!(shutdown_report.provider_may_have_abandoned_deliveries);
    black_box(completed.load(Ordering::Relaxed));
    Sample {
        ns_per_operation: elapsed / operations as u128,
        observed: first_wave,
        wait_median_ns: 0,
        wait_p95_ns: 0,
        cpu_ns,
        process_cpu_ns,
        wall_ns: elapsed,
    }
}

/// Prints seven raw valid samples, median, range and median absolute deviation.
/// `label` describes the measured boundary; `run` creates one fresh fixture.
fn measure(label: &str, mut run: impl FnMut() -> Sample) {
    if let Ok(filter) = std::env::var("ORDERING_FILTER")
        && label != filter
    {
        return;
    }
    for _ in 0..WARMUPS {
        black_box(run());
    }
    let mut samples = Vec::with_capacity(SAMPLES);
    let mut discarded = 0;
    for _ in 0..256 {
        let sample = run();
        let applicable_cpu = if label.starts_with("registry ") {
            sample.cpu_ns
        } else {
            sample.process_cpu_ns
        };
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
    let waiting = samples
        .iter()
        .map(|sample| (sample.wait_median_ns, sample.wait_p95_ns))
        .collect::<Vec<_>>();
    let mut times = samples.iter().map(|sample| sample.ns_per_operation).collect::<Vec<_>>();
    times.sort_unstable();
    let median = times[SAMPLES / 2];
    let mut deviations = times.iter().map(|time| time.abs_diff(median)).collect::<Vec<_>>();
    deviations.sort_unstable();
    println!(
        "{label} samples={samples:?} observed={observations:?} wait_median_p95={waiting:?} discarded={discarded} median_ns={median} min_ns={} max_ns={} mad_ns={}",
        times[0],
        times[SAMPLES - 1],
        deviations[SAMPLES / 2]
    );
}

/// Runs the full matrix, or one long profile workload selected by environment.
/// Profile mode uses the same production source and records no wall-clock gate.
fn main() {
    if std::env::var_os("ORDERING_SMOKE").is_some() {
        for workload in ["balanced", "same-key", "churn"] {
            for window in [4, 64] {
                black_box(registry_sample(4, window, workload, 128));
                black_box(facade_sample(8, window, workload));
            }
        }
        println!("ordering workload invariants passed; smoke timings are discarded");
        return;
    }
    if std::env::var_os("ORDERING_PROFILE").is_some() {
        let operations = std::env::var("ORDERING_PROFILE_OPERATIONS")
            .map(|value| value.parse().expect("profile operations must be an integer"))
            .unwrap_or(65_536);
        black_box(registry_sample(4096, 64, "balanced", operations));
        return;
    }
    println!("ordering_keys warmups={WARMUPS} samples={SAMPLES} operations={OPERATIONS} registry source=production");
    for active_keys in KEYS {
        for window in [4, 64] {
            for workload in ["balanced", "same-key", "churn"] {
                measure(
                    &format!("registry live_keys={active_keys} ticket_window={window} workload={workload}"),
                    || registry_sample(active_keys, window, workload, OPERATIONS),
                );
            }
        }
        for limit in [4, active_keys.max(64)] {
            for workload in ["balanced", "same-key", "churn"] {
                let distinct_keys = match workload {
                    "same-key" => 1,
                    "churn" => active_keys.max(1024) * 2,
                    _ => active_keys,
                };
                measure(
                    &format!(
                        "facade case_keys={active_keys} distinct_input_keys={distinct_keys} max_in_flight={limit} workload={workload}"
                    ),
                    || facade_sample(active_keys, limit, workload),
                );
            }
        }
    }
}
