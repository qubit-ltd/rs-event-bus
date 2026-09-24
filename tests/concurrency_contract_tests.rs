// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Small loom models for lifecycle contracts that must survive racing callers.
//!
//! These models validate the required state transitions independently of the
//! production executor. They intentionally do not claim to instrument private
//! facade internals or prove their implementation correct by themselves.

#![cfg(loom)]

use loom::model;
use loom::sync::Arc;
use loom::sync::Condvar;
use loom::sync::Mutex;
use loom::sync::atomic::AtomicBool;
use loom::sync::atomic::AtomicUsize;
use loom::sync::atomic::Ordering;
use loom::thread;

#[test]
fn admission_permit_is_released_exactly_once_under_competing_cleanup() {
    model(|| {
        let active = Arc::new(AtomicUsize::new(1));
        let released = Arc::new(AtomicBool::new(false));
        let release = |active: &AtomicUsize, released: &AtomicBool| {
            if !released.swap(true, Ordering::AcqRel) {
                active.fetch_sub(1, Ordering::AcqRel);
            }
        };

        let first_active = active.clone();
        let first_released = released.clone();
        let first = thread::spawn(move || release(&first_active, &first_released));
        let second_active = active.clone();
        let second_released = released.clone();
        let second = thread::spawn(move || release(&second_active, &second_released));
        first.join().unwrap();
        second.join().unwrap();

        assert_eq!(0, active.load(Ordering::Acquire));
        assert!(released.load(Ordering::Acquire));
    });
}

#[test]
fn cancelling_an_ordering_lane_advances_the_next_waiter() {
    model(|| {
        let lane = Arc::new((Mutex::new((true, false)), Condvar::new()));
        let next_lane = lane.clone();
        let next = thread::spawn(move || {
            let (lock, ready) = &*next_lane;
            let mut state = lock.lock().unwrap();
            while state.0 {
                state = ready.wait(state).unwrap();
            }
            state.1 = true;
        });

        let cancel_lane = lane.clone();
        let cancel = thread::spawn(move || {
            let (lock, ready) = &*cancel_lane;
            let mut state = lock.lock().unwrap();
            state.0 = false;
            ready.notify_all();
        });

        cancel.join().unwrap();
        next.join().unwrap();
        assert!(lane.0.lock().unwrap().1);
    });
}

#[test]
fn subscription_cancel_racing_receive_never_starts_after_cancel_wins() {
    model(|| {
        #[derive(Default)]
        struct State {
            cancelled: bool,
            handler_starts: usize,
            handler_starts_when_cancelled: usize,
        }

        let state = Arc::new(Mutex::new(State::default()));
        let receive_state = state.clone();
        let receive = thread::spawn(move || {
            let mut state = receive_state.lock().unwrap();
            if !state.cancelled {
                state.handler_starts += 1;
            }
        });
        let cancel_state = state.clone();
        let cancel = thread::spawn(move || {
            let mut state = cancel_state.lock().unwrap();
            state.cancelled = true;
            state.handler_starts_when_cancelled = state.handler_starts;
        });

        receive.join().unwrap();
        cancel.join().unwrap();
        let state = state.lock().unwrap();
        assert!(state.cancelled);
        assert_eq!(state.handler_starts_when_cancelled, state.handler_starts);
    });
}

#[test]
fn graceful_shutdown_and_publish_have_one_admission_linearization_point() {
    model(|| {
        #[derive(Default)]
        struct State {
            closed: bool,
            admitted: usize,
        }

        let state = Arc::new(Mutex::new(State::default()));
        let publish_state = state.clone();
        let publish = thread::spawn(move || {
            let mut state = publish_state.lock().unwrap();
            if !state.closed {
                state.admitted += 1;
            }
        });
        let shutdown_state = state.clone();
        let shutdown = thread::spawn(move || {
            shutdown_state.lock().unwrap().closed = true;
        });

        publish.join().unwrap();
        shutdown.join().unwrap();
        let state = state.lock().unwrap();
        assert!(state.closed);
        assert!(state.admitted <= 1);
    });
}
