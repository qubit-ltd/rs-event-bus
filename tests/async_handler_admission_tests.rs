// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Actual handler admission after middleware, filtering, and local retry.

mod support;

use std::future::Future;
use std::future::poll_fn;
use std::io::Error;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::ReceiveError;
use qubit_event_bus::model::AsyncSubscriberNext;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionStopReason;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::SpiFuture;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryPolicy;
use support::fake_spi::FakeAsyncEventBusSpi;
use support::fake_spi::inbound_message;
use support::manual_async::block_on;
use support::manual_async::poll_once;

/// Drives a finite number of turns without a background executor or wall clock.
fn poll_pending<F: Future>(mut future: Pin<&mut F>) {
    for _ in 0..16 {
        assert!(poll_once(future.as_mut()).is_pending());
    }
}

/// The published cause used while middleware retains a granted delivery.
#[derive(Clone, Copy)]
enum Stop {
    Immediate,
    Graceful,
    Terminal,
    RetryCancellation,
    Pause,
}

/// Exercises first and retry-attempt gates with identical observable counters.
fn middleware_gate(stop: Stop, retry: bool) {
    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let clock = ManualMonotonicClock::new();
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("handler-gate").expect("provider"),
        fake.clone(),
        EventBusFacadeConfig::default(),
        clock.new_timer(),
    )
    .expect("bus");
    let entered = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(AtomicBool::new(false));
    let failures = Arc::new(AtomicUsize::new(0));
    let cancellation = RetryCancellationToken::new();
    let gate_attempt = if retry { 2 } else { 1 };
    let mut options = SubscribeOptions::builder()
        .async_interceptor({
            let entered = entered.clone();
            let release = release.clone();
            move |delivery: Delivery<u32>, next: AsyncSubscriberNext<u32>| {
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    let attempt = entered.fetch_add(1, Ordering::SeqCst) + 1;
                    if attempt == gate_attempt {
                        poll_fn(|_| {
                            if release.load(Ordering::SeqCst) {
                                Poll::Ready(())
                            } else {
                                Poll::Pending
                            }
                        })
                        .await;
                    }
                    next(delivery).await
                }) as SpiFuture<'static, Result<(), DeliveryError>>
            }
        })
        .error_handler({
            let failures = failures.clone();
            move |_, _| {
                failures.fetch_add(1, Ordering::SeqCst);
                FailureDirective::Retry
            }
        })
        .retry_cancellation_token(cancellation.clone());
    if retry {
        options = options.retry_policy(RetryPolicy::builder().max_attempts(2).build().expect("retry policy"));
    }
    let request = SubscribeRequest::new("handler-gate", Topic::<u32>::new("orders.created").expect("topic"))
        .expect("request")
        .with_options(options.build());
    let mut sub = block_on(bus.subscribe(request)).expect("subscribe");
    fake.enqueue(inbound_message(Some(SettlementToken::new(sub.id(), "token"))));
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = calls.clone();
    let mut run = Box::pin(sub.run(move |_| {
        let call = observed.fetch_add(1, Ordering::SeqCst);
        async move {
            if retry && call == 0 {
                Err(DeliveryError::Handler {
                    source: Box::new(Error::other("retry once")),
                })
            } else {
                Ok(())
            }
        }
    }));
    poll_pending(run.as_mut());
    assert_eq!(entered.load(Ordering::SeqCst), gate_attempt);
    let prior = usize::from(retry);
    assert_eq!(calls.load(Ordering::SeqCst), prior);
    let mut shutdown = None;
    match stop {
        Stop::Immediate | Stop::Graceful => {
            let mode = if matches!(stop, Stop::Immediate) {
                ShutdownMode::Immediate
            } else {
                ShutdownMode::Graceful {
                    timeout: Duration::from_secs(1),
                }
            };
            shutdown = Some(Box::pin(bus.shutdown(mode)));
            assert!(poll_once(shutdown.as_mut().unwrap().as_mut()).is_pending());
        }
        Stop::Terminal => {
            fake.fail_next_receive();
            poll_pending(run.as_mut());
        }
        Stop::RetryCancellation => cancellation.cancel(),
        Stop::Pause => {}
    }
    drop(run);
    let retained = sub.terminal_failure();
    if matches!(stop, Stop::Terminal) {
        assert!(matches!(
            retained.as_deref(),
            Some(SubscriptionStopReason::Provider { .. })
        ));
    }
    release.store(true, Ordering::SeqCst);
    let mut run_result = Poll::Pending;
    let replacements = Arc::new(AtomicUsize::new(0));
    if let Some(shutdown) = shutdown.as_mut() {
        let mut completed = false;
        for _ in 0..64 {
            if poll_once(shutdown.as_mut()).is_ready() {
                completed = true;
                break;
            }
        }
        assert!(completed, "shutdown must finish after the gated handler resumes");
    } else {
        let replaced = replacements.clone();
        let mut resumed = Box::pin(sub.run(move |_| {
            replaced.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        }));
        for _ in 0..64 {
            run_result = poll_once(resumed.as_mut());
            if run_result.is_ready() {
                break;
            }
        }
        drop(resumed);
    }
    let admitted = matches!(stop, Stop::Graceful | Stop::Pause);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        prior + usize::from(admitted),
        "actual factory must be gated on every attempt"
    );
    assert_eq!(
        replacements.load(Ordering::SeqCst),
        0,
        "pause retains original middleware and handler"
    );
    assert_eq!(
        failures.load(Ordering::SeqCst),
        prior,
        "denial must not become a subscriber failure or DLQ request"
    );
    assert_eq!(
        fake.settlement_count(),
        usize::from(admitted),
        "denial must not fabricate an Accept or Reject"
    );
    if let Some(retained) = retained {
        let Poll::Ready(Err(ReceiveError::Stopped(reason))) = run_result else {
            panic!("original terminal source must survive cleanup");
        };
        assert!(Arc::ptr_eq(&retained, &reason));
    }
    block_on(sub.close()).expect("cleanup receiver");
    let snapshot = sub.delivery_metrics().metrics;
    assert_eq!(
        snapshot.reserved_receives + snapshot.queued + snapshot.running_handlers + snapshot.settling,
        0
    );
    assert_eq!(snapshot.handler_duration_count, (prior + usize::from(admitted)) as u64);
}

#[test]
fn test_immediate_stop_after_middleware_blocks_actual_handler() {
    middleware_gate(Stop::Immediate, false);
}
#[test]
fn test_terminal_stop_after_middleware_preserves_original_source() {
    middleware_gate(Stop::Terminal, false);
}
#[test]
fn test_graceful_stop_after_middleware_drains_actual_handler() {
    middleware_gate(Stop::Graceful, false);
}
#[test]
fn test_retry_cancellation_after_middleware_blocks_actual_handler() {
    middleware_gate(Stop::RetryCancellation, false);
}
#[test]
fn test_pause_after_middleware_preserves_original_handler() {
    middleware_gate(Stop::Pause, false);
}
#[test]
fn test_immediate_stop_gates_each_local_retry_attempt() {
    middleware_gate(Stop::Immediate, true);
}
#[test]
fn test_retry_cancellation_gates_local_retry_attempt() {
    middleware_gate(Stop::RetryCancellation, true);
}
#[test]
fn test_graceful_stop_drains_local_retry_attempt() {
    middleware_gate(Stop::Graceful, true);
}

/// A synchronous filter can publish stop before it returns to handler
/// admission.
#[test]
fn test_filter_stop_cannot_authorize_handler_or_settlement() {
    for accepted in [true, false] {
        let fake = Arc::new(FakeAsyncEventBusSpi::new());
        let bus =
            AsyncEventBus::from_spi(ProviderId::new("filter-gate").expect("provider"), fake.clone()).expect("bus");
        let filter_bus = bus.clone();
        let options = SubscribeOptions::builder()
            .filter(move |_| {
                let mut shutdown = Box::pin(filter_bus.shutdown(ShutdownMode::Immediate));
                let _ = poll_once(shutdown.as_mut());
                accepted
            })
            .build();
        let request = SubscribeRequest::new("filter-gate", Topic::<u32>::new("orders.created").expect("topic"))
            .expect("request")
            .with_options(options);
        let sub = block_on(bus.subscribe(request)).expect("subscribe");
        fake.enqueue(inbound_message(Some(SettlementToken::new(sub.id(), "token"))));
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        block_on(sub.run(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        }))
        .expect("stopped runner");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(fake.settlement_count(), 0);
    }
}
