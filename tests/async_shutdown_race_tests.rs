// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for asynchronous shutdown leader races.

#[path = "support/fake_spi.rs"]
#[allow(dead_code)]
mod fake_spi;
#[path = "support/manual_async.rs"]
#[allow(dead_code)]
mod manual_async;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;

use fake_spi::FakeAsyncEventBusSpi;
use manual_async::block_on;
use qubit_clock::MonotonicClock;
use qubit_clock::MonotonicInstant;
use qubit_clock::StdMonotonicClock;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

const WATCHDOG: Duration = Duration::from_secs(5);

#[derive(Default)]
struct GateState {
    entered: bool,
    released: bool,
}

#[derive(Default)]
struct Gate {
    state: Mutex<GateState>,
    changed: Condvar,
}

impl Gate {
    fn pause(&self) {
        let mut state = self.state.lock().expect("gate lock");
        state.entered = true;
        self.changed.notify_all();
        while !state.released {
            state = self.changed.wait(state).expect("gate wait");
        }
    }

    fn wait_until_entered(&self) -> bool {
        let state = self.state.lock().expect("gate lock");
        let (state, _) = self
            .changed
            .wait_timeout_while(state, WATCHDOG, |state| !state.entered)
            .expect("gate watchdog wait");
        state.entered
    }

    fn release(&self) {
        self.state.lock().expect("gate lock").released = true;
        self.changed.notify_all();
    }
}

/// Always releases a blocked timer registration, including assertion unwinds.
struct GateRelease(Arc<Gate>);

impl Drop for GateRelease {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct ControlledTimer {
    clock: StdMonotonicClock,
    gate: Arc<Gate>,
    pause_next: AtomicBool,
}

impl Timer for ControlledTimer {
    fn clock(&self) -> &dyn MonotonicClock {
        &self.clock
    }

    fn at(&self, deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        if self.pause_next.swap(false, Ordering::AcqRel) {
            self.gate.pause();
        }
        self.clock.new_timer().at(deadline)
    }
}

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    future.poll(&mut Context::from_waker(&waker))
}

fn setup(timer: Arc<dyn Timer>) -> (AsyncEventBus, Arc<FakeAsyncEventBusSpi>) {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("shutdown-race").expect("provider id"),
        spi.clone(),
        EventBusFacadeConfig::default(),
        timer,
    )
    .expect("event bus");
    (bus, spi)
}

#[test]
fn test_closed_bus_is_not_reopened_by_late_shutdown_leader() {
    let gate = Arc::new(Gate::default());
    let timer = Arc::new(ControlledTimer {
        clock: StdMonotonicClock::new(),
        gate: gate.clone(),
        pause_next: AtomicBool::new(true),
    });
    let (bus, spi) = setup(timer);
    let _release_on_unwind = GateRelease(gate.clone());
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let graceful_bus = bus.clone();
    let graceful_thread = thread::spawn(move || {
        result_tx
            .send(block_on(graceful_bus.shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(30),
            })))
            .expect("send graceful result");
    });

    assert!(
        gate.wait_until_entered(),
        "graceful timer registration watchdog expired"
    );
    let immediate_report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("immediate shutdown");
    assert_eq!(
        1,
        spi.operation_log().iter().filter(|&&call| call == "shutdown").count()
    );

    gate.release();
    let graceful_report = result_rx
        .recv_timeout(WATCHDOG)
        .expect("graceful shutdown watchdog expired")
        .expect("graceful result");
    graceful_thread.join().expect("graceful thread");
    assert_eq!(ShutdownOutcome::Complete, immediate_report.outcome);
    assert_eq!(immediate_report, graceful_report);

    let cached_report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("cached shutdown report");
    assert_eq!(immediate_report, cached_report);
    assert_eq!(
        1,
        spi.operation_log().iter().filter(|&&call| call == "shutdown").count()
    );
}

#[test]
fn test_cancelled_shutdown_leader_can_be_taken_over() {
    let (bus, spi) = setup(Arc::new(StdMonotonicClock::new().new_timer()));
    spi.pause_shutdown();
    let mut first = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(
        poll_once(first.as_mut()).is_pending(),
        "provider shutdown should remain pending"
    );
    drop(first);
    assert_eq!(
        1,
        spi.operation_log().iter().filter(|&&call| call == "shutdown").count()
    );

    spi.release_shutdown();
    let report = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("replacement leader completes shutdown");
    assert_eq!(ShutdownOutcome::Complete, report.outcome);
    assert_eq!(
        2,
        spi.operation_log().iter().filter(|&&call| call == "shutdown").count()
    );
}

#[test]
fn test_graceful_shutdown_can_be_upgraded_while_provider_shutdown_is_pending() {
    let (bus, spi) = setup(Arc::new(StdMonotonicClock::new().new_timer()));
    spi.pause_shutdown();
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let graceful_bus = bus.clone();
    let graceful_thread = thread::spawn(move || {
        result_tx
            .send(block_on(graceful_bus.shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(30),
            })))
            .expect("send graceful result");
    });

    let shutdown_started = std::time::Instant::now();
    while !spi.operation_log().contains(&"shutdown") {
        assert!(
            shutdown_started.elapsed() < WATCHDOG,
            "provider shutdown watchdog expired"
        );
        thread::yield_now();
    }

    let mut immediate = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(
        poll_once(immediate.as_mut()).is_pending(),
        "immediate upgrade waits for active leader"
    );
    spi.release_shutdown();
    let immediate_report = block_on(immediate).expect("immediate upgrade completes");
    let graceful_report = result_rx
        .recv_timeout(WATCHDOG)
        .expect("graceful shutdown watchdog expired")
        .expect("graceful result");
    graceful_thread.join().expect("graceful thread");

    assert_eq!(ShutdownOutcome::Complete, immediate_report.outcome);
    assert_eq!(immediate_report, graceful_report);
}
