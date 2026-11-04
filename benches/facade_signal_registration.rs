// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures facade signal registration using the production implementation.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;
use std::thread::Thread;
use std::time::Instant;

use qubit_event_bus::facade::AsyncEventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

mod internal {
    pub(crate) mod panic_payload {
        use std::any::Any;
        use std::panic::AssertUnwindSafe;
        use std::panic::catch_unwind;

        pub(crate) fn dispose_panic_payload(payload: Box<dyn Any + Send>) {
            if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
                std::mem::forget(secondary);
            }
        }
    }
}

#[path = "facade_signal_registration_support/support.rs"]
mod facade;

#[derive(Default)]
struct CallbackCounts {
    clones: AtomicUsize,
    drops: AtomicUsize,
    wakes: AtomicUsize,
}

unsafe fn clone_waker(data: *const ()) -> RawWaker {
    // SAFETY: Each RawWaker owns exactly one Arc reference to CallbackCounts.
    let counts = unsafe { Arc::<CallbackCounts>::from_raw(data.cast()) };
    counts.clones.fetch_add(1, Ordering::Relaxed);
    let cloned = Arc::clone(&counts);
    let _ = Arc::into_raw(counts);
    RawWaker::new(Arc::into_raw(cloned).cast(), &WAKER_VTABLE)
}

unsafe fn wake_waker(data: *const ()) {
    // SAFETY: wake consumes the RawWaker's one owned Arc reference.
    let counts = unsafe { Arc::<CallbackCounts>::from_raw(data.cast()) };
    counts.wakes.fetch_add(1, Ordering::Relaxed);
}

unsafe fn wake_waker_by_ref(data: *const ()) {
    // SAFETY: The caller retains the RawWaker's owned Arc reference.
    let counts = unsafe { &*data.cast::<CallbackCounts>() };
    counts.wakes.fetch_add(1, Ordering::Relaxed);
}

unsafe fn drop_waker(data: *const ()) {
    // SAFETY: Drop consumes the RawWaker's one owned Arc reference.
    let counts = unsafe { Arc::<CallbackCounts>::from_raw(data.cast()) };
    counts.drops.fetch_add(1, Ordering::Relaxed);
}

static WAKER_VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake_waker, wake_waker_by_ref, drop_waker);

fn make_waker(counts: &Arc<CallbackCounts>) -> Waker {
    let pointer = Arc::into_raw(Arc::clone(counts)).cast();
    // SAFETY: The data pointer owns one Arc and the static vtable preserves it.
    unsafe { Waker::from_raw(RawWaker::new(pointer, &WAKER_VTABLE)) }
}

struct RunnerWaker {
    counts: Arc<CallbackCounts>,
    thread: Thread,
}

unsafe fn clone_runner_waker(data: *const ()) -> RawWaker {
    // SAFETY: Each RawWaker owns exactly one Arc reference to RunnerWaker.
    let runner = unsafe { Arc::<RunnerWaker>::from_raw(data.cast()) };
    runner.counts.clones.fetch_add(1, Ordering::Relaxed);
    let cloned = Arc::clone(&runner);
    let _ = Arc::into_raw(runner);
    RawWaker::new(Arc::into_raw(cloned).cast(), &RUNNER_WAKER_VTABLE)
}

unsafe fn wake_runner(data: *const ()) {
    // SAFETY: wake consumes the RawWaker's one owned Arc reference.
    let runner = unsafe { Arc::<RunnerWaker>::from_raw(data.cast()) };
    runner.counts.wakes.fetch_add(1, Ordering::Relaxed);
    runner.thread.unpark();
}

unsafe fn wake_runner_by_ref(data: *const ()) {
    // SAFETY: wake_by_ref borrows the live Arc allocation.
    let runner = unsafe { &*data.cast::<RunnerWaker>() };
    runner.counts.wakes.fetch_add(1, Ordering::Relaxed);
    runner.thread.unpark();
}

unsafe fn drop_runner_waker(data: *const ()) {
    // SAFETY: drop consumes the RawWaker's one owned Arc reference.
    let runner = unsafe { Arc::<RunnerWaker>::from_raw(data.cast()) };
    runner.counts.drops.fetch_add(1, Ordering::Relaxed);
}

static RUNNER_WAKER_VTABLE: RawWakerVTable =
    RawWakerVTable::new(clone_runner_waker, wake_runner, wake_runner_by_ref, drop_runner_waker);

fn make_runner_waker(counts: &Arc<CallbackCounts>) -> Waker {
    let pointer = Arc::into_raw(Arc::new(RunnerWaker {
        counts: Arc::clone(counts),
        thread: std::thread::current(),
    }))
    .cast();
    // SAFETY: The data pointer owns one Arc and the static vtable preserves it.
    unsafe { Waker::from_raw(RawWaker::new(pointer, &RUNNER_WAKER_VTABLE)) }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

const END_TO_END_ITERATIONS: usize = 250;

fn sample_end_to_end() -> (u128, usize, usize, usize) {
    let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::new())).expect("local facade builds");
    let topic = Topic::<usize>::new("bench.facade.signal").expect("benchmark topic is valid");
    let subscription = block_on(
        bus.subscribe(SubscribeRequest::new("facade-signal-bench", topic.clone()).expect("subscriber ID is valid")),
    )
    .expect("subscription is created");
    let handled = Arc::new(AtomicUsize::new(0));
    let handled_by_runner = Arc::clone(&handled);
    let mut runner = Box::pin(subscription.run(move |delivery| {
        let handled = Arc::clone(&handled_by_runner);
        async move {
            delivery.acknowledgement().ack().expect("delivery acknowledges");
            handled.fetch_add(1, Ordering::Release);
            Ok(())
        }
    }));
    let callback_counts = Arc::new(CallbackCounts::default());
    let waker = make_runner_waker(&callback_counts);
    let mut context = Context::from_waker(&waker);
    assert!(
        runner.as_mut().poll(&mut context).is_pending(),
        "runner waits for delivery"
    );

    let started = Instant::now();
    for index in 0..END_TO_END_ITERATIONS {
        for _ in 0..4 {
            assert!(
                runner.as_mut().poll(&mut context).is_pending(),
                "idle runner remains pending"
            );
        }
        let request = PublishRequest::new(topic.clone(), index).expect("publish request is valid");
        let _receipt = block_on(bus.publish(black_box(request))).expect("local publish succeeds");
        let expected = index + 1;
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while handled.load(Ordering::Acquire) < expected && Instant::now() < deadline {
            let _ = runner.as_mut().poll(&mut context);
            std::thread::yield_now();
        }
        assert_eq!(
            expected,
            handled.load(Ordering::Acquire),
            "runner handled each publication"
        );
        assert!(
            runner.as_mut().poll(&mut context).is_pending(),
            "settlement returns to receive wait"
        );
    }
    let elapsed = started.elapsed().as_nanos() / END_TO_END_ITERATIONS as u128;
    drop(runner);
    let _shutdown_report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("bus shuts down");
    (
        elapsed,
        callback_counts.clones.load(Ordering::Relaxed),
        callback_counts.drops.load(Ordering::Relaxed),
        callback_counts.wakes.load(Ordering::Relaxed),
    )
}

fn main() {
    let scenario = std::env::args().nth(1).unwrap_or_else(|| "both".into());
    if scenario == "end-to-end" || scenario == "both" {
        let mut samples = Vec::with_capacity(7);
        let mut callbacks = (0, 0, 0);
        for _ in 0..2 {
            black_box(sample_end_to_end());
        }
        for _ in 0..7 {
            let (elapsed, clones, drops, wakes) = sample_end_to_end();
            samples.push(elapsed);
            callbacks = (clones, drops, wakes);
        }
        println!(
            "scenario=end_to_end raw_sample_mean_ns={samples:?} iterations={END_TO_END_ITERATIONS} idle_polls_per_publish=4 callbacks_clone_drop_wake={callbacks:?}"
        );
    }
    for alternating in [false, true] {
        let name = if alternating { "alternating" } else { "same" };
        if scenario != "both" && scenario != name {
            continue;
        }
        let mut samples = Vec::with_capacity(7);
        let mut callbacks = (0, 0, 0);
        for _ in 0..2 {
            black_box(facade::benchmark::sample(alternating));
        }
        for _ in 0..7 {
            let (elapsed, clones, drops, wakes) = facade::benchmark::sample(alternating);
            samples.push(elapsed);
            callbacks = (clones, drops, wakes);
        }
        println!(
            "scenario={name} raw_sample_mean_ns={samples:?} iterations={} callbacks_clone_drop_wake={callbacks:?}",
            facade::benchmark::ITERATIONS
        );
    }
}
