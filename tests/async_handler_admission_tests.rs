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
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::LifecycleError;
use qubit_event_bus::ReceiveError;
use qubit_event_bus::ShutdownError;
use qubit_event_bus::WaitOutcome;
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

type ReceivedWaitFuture = Pin<Box<dyn Future<Output = Result<WaitOutcome, LifecycleError>> + Send>>;
type ObservedWait = Arc<Mutex<Option<Poll<Result<WaitOutcome, LifecycleError>>>>>;

struct WaitDropProbe {
    bus: AsyncEventBus,
    topic: Topic<u32>,
    observed: ObservedWait,
}

// Dropping this waker reenters the wait operation; Waker::noop() cannot test
// that.
#[allow(clippy::manual_noop_waker)]
impl Wake for WaitDropProbe {
    fn wake(self: Arc<Self>) {}
}

impl Drop for WaitDropProbe {
    fn drop(&mut self) {
        let mut nested = Box::pin(self.bus.wait_for_received_deliveries(&self.topic, None));
        let result = nested.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        *self.observed.lock().expect("drop probe observation") = Some(result);
    }
}

struct WaitDropTimer {
    clock: ManualMonotonicClock,
    bus: Arc<Mutex<Option<AsyncEventBus>>>,
    topic: Topic<u32>,
    observed: ObservedWait,
}

struct WaitDropTimerFuture {
    inner: TimerFuture,
    bus: Arc<Mutex<Option<AsyncEventBus>>>,
    topic: Topic<u32>,
    observed: ObservedWait,
}

impl Timer for WaitDropTimer {
    fn clock(&self) -> &dyn MonotonicClock {
        &self.clock
    }

    fn at(&self, deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        Ok(Box::pin(WaitDropTimerFuture {
            inner: self.clock.new_timer().at(deadline)?,
            bus: self.bus.clone(),
            topic: self.topic.clone(),
            observed: self.observed.clone(),
        }))
    }
}

impl Future for WaitDropTimerFuture {
    type Output = Result<(), TimeError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().inner.as_mut().poll(cx)
    }
}

impl Drop for WaitDropTimerFuture {
    fn drop(&mut self) {
        let Some(bus) = self.bus.lock().expect("timer bus slot").clone() else {
            return;
        };
        let mut nested = Box::pin(bus.wait_for_received_deliveries(&self.topic, None));
        let result = nested.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        *self.observed.lock().expect("timer drop observation") = Some(result);
    }
}

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
        });
    if retry {
        options = options
            .retry_policy(RetryPolicy::builder().max_attempts(2).build().expect("retry policy"))
            .retry_cancellation_token(cancellation.clone());
    }
    let topic = Topic::<u32>::new("orders.created").expect("topic");
    let request = SubscribeRequest::new("handler-gate", topic)
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
    assert!(matches!(block_on(sub.close()), Ok(()) | Err(LifecycleError::Closed)));
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
    const CASE: &str = "filter-stop-admission";
    if std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() != Ok(CASE) {
        support::isolated_process::run_case("test_filter_stop_cannot_authorize_handler_or_settlement", CASE);
        return;
    }
    for accepted in [true, false] {
        let fake = Arc::new(FakeAsyncEventBusSpi::new());
        let bus =
            AsyncEventBus::from_spi(ProviderId::new("filter-gate").expect("provider"), fake.clone()).expect("bus");
        let (entered_filter, filter_entered) = std::sync::mpsc::channel();
        let (continue_filter, filter_continue) = std::sync::mpsc::channel();
        let filter_continue = Arc::new(std::sync::Mutex::new(filter_continue));
        let filter_continue_in_closure = filter_continue.clone();
        let options = SubscribeOptions::builder()
            .filter(move |_| {
                entered_filter.send(()).expect("test driver is waiting");
                filter_continue_in_closure
                    .lock()
                    .expect("filter continuation lock")
                    .recv_timeout(Duration::from_secs(3))
                    .expect("test driver releases filter within the handshake deadline");
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
        let runner = std::thread::spawn(move || {
            block_on(sub.run(move |_| {
                observed.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }))
        });
        filter_entered
            .recv_timeout(Duration::from_secs(3))
            .expect("filter enters before the handshake deadline");
        let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
        assert!(poll_once(shutdown.as_mut()).is_pending());
        continue_filter.send(()).expect("filter continues");
        let _ = block_on(shutdown).expect("external shutdown");
        runner.join().expect("runner thread").expect("stopped runner");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(fake.settlement_count(), 0);
    }
}

struct PanickingDropPayload(Arc<AtomicUsize>);

impl Drop for PanickingDropPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("filter panic payload destructor panicked");
    }
}

fn filter_panic_payload_stop(cancel_retry: bool) {
    let case = if cancel_retry {
        "filter-payload-cancel"
    } else {
        "filter-payload-shutdown"
    };
    let test = if cancel_retry {
        "test_filter_panic_payload_drop_isolated_on_retry_cancellation"
    } else {
        "test_filter_panic_payload_drop_isolated_on_shutdown"
    };
    if std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() != Ok(case) {
        support::isolated_process::run_case(test, case);
        return;
    }

    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("filter-payload").expect("provider"), fake.clone()).expect("bus");
    let (entered_filter, filter_entered) = std::sync::mpsc::channel();
    let (continue_filter, filter_continue) = std::sync::mpsc::channel();
    let filter_continue = Arc::new(std::sync::Mutex::new(filter_continue));
    let cancelled = Arc::new(AtomicUsize::new(0));
    let payload_drops = Arc::new(AtomicUsize::new(0));
    let handlers = Arc::new(AtomicUsize::new(0));
    let failures = Arc::new(AtomicUsize::new(0));
    let cancellation = RetryCancellationToken::new();
    let mut builder = SubscribeOptions::builder().filter({
        let filter_continue = filter_continue.clone();
        let cancellation = cancellation.clone();
        let cancelled = cancelled.clone();
        let payload_drops = payload_drops.clone();
        move |_| {
            entered_filter.send(()).expect("test driver waits for filter");
            filter_continue
                .lock()
                .expect("filter continuation lock")
                .recv_timeout(Duration::from_secs(3))
                .expect("test driver releases filter");
            if cancel_retry {
                cancellation.cancel();
                cancelled.fetch_add(1, Ordering::SeqCst);
            }
            std::panic::panic_any(PanickingDropPayload(payload_drops.clone()));
        }
    });
    if cancel_retry {
        builder = builder
            .retry_policy(RetryPolicy::builder().max_attempts(2).build().expect("retry policy"))
            .retry_cancellation_token(cancellation.clone());
    }
    builder = builder.error_handler({
        let failures = failures.clone();
        move |_, _| {
            failures.fetch_add(1, Ordering::SeqCst);
            FailureDirective::Retry
        }
    });
    let request = SubscribeRequest::new("filter-payload", Topic::<u32>::new("orders.created").expect("topic"))
        .expect("request")
        .with_options(builder.build());
    let sub = block_on(bus.subscribe(request)).expect("subscribe");
    fake.enqueue(inbound_message(Some(SettlementToken::new(sub.id(), "token"))));
    let observed = handlers.clone();
    let runner = std::thread::spawn(move || {
        block_on(sub.run(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        }))
    });
    filter_entered
        .recv_timeout(Duration::from_secs(3))
        .expect("filter entered");
    if !cancel_retry {
        let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
        assert!(poll_once(shutdown.as_mut()).is_pending());
        continue_filter.send(()).expect("release filter");
        let _ = block_on(shutdown).expect("external shutdown");
    } else {
        continue_filter.send(()).expect("release filter");
        for _ in 0..300 {
            if payload_drops.load(Ordering::SeqCst) == 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(payload_drops.load(Ordering::SeqCst), 1);
        let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("application shutdown");
    }
    runner.join().expect("runner thread").expect("stopped runner");
    assert_eq!(payload_drops.load(Ordering::SeqCst), 1);
    assert_eq!(cancelled.load(Ordering::SeqCst), usize::from(cancel_retry));
    assert_eq!(handlers.load(Ordering::SeqCst), 0);
    assert_eq!(failures.load(Ordering::SeqCst), 0);
    assert_eq!(fake.settlement_count(), 0);
}

#[test]
fn test_filter_panic_payload_drop_isolated_on_shutdown() {
    filter_panic_payload_stop(false);
}

#[test]
fn test_filter_panic_payload_drop_isolated_on_retry_cancellation() {
    filter_panic_payload_stop(true);
}

#[test]
fn test_filter_reentrant_shutdown_is_rejected_in_isolated_process() {
    const CASE: &str = "filter-reentrant-shutdown";
    if std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() != Ok(CASE) {
        support::isolated_process::run_case("test_filter_reentrant_shutdown_is_rejected_in_isolated_process", CASE);
        return;
    }

    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("filter-shutdown").expect("provider"), fake.clone()).expect("bus");
    let filter_bus = bus.clone();
    let filter_fake = fake.clone();
    let (entered_filter, filter_entered) = std::sync::mpsc::channel();
    let (continue_filter, filter_continue) = std::sync::mpsc::channel();
    let filter_continue = Arc::new(std::sync::Mutex::new(filter_continue));
    let filter_continue_in_closure = filter_continue.clone();
    let options = SubscribeOptions::builder()
        .filter(move |_| {
            assert!(matches!(
                block_on(filter_bus.shutdown(ShutdownMode::Immediate)),
                Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
                    operation: "shutdown"
                }))
            ));
            assert_eq!(filter_fake.shutdown_transition_count(), 0);
            entered_filter.send(()).expect("test driver is waiting");
            filter_continue_in_closure
                .lock()
                .expect("filter continuation lock")
                .recv()
                .expect("test driver releases filter");
            false
        })
        .build();
    let topic = Topic::<u32>::new("filter.shutdown").expect("topic");
    let request = SubscribeRequest::new("filter-shutdown", topic)
        .expect("request")
        .with_options(options);
    let sub = block_on(bus.subscribe(request)).expect("subscribe");
    fake.enqueue(inbound_message(None));
    let runner = std::thread::spawn(move || block_on(sub.run(|_| async { Ok(()) })));
    filter_entered.recv().expect("filter entered");
    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(poll_once(shutdown.as_mut()).is_pending());
    continue_filter.send(()).expect("filter continues");
    let _ = block_on(shutdown).expect("external shutdown");
    runner.join().expect("runner thread").expect("runner");
}

#[test]
fn test_filter_reentrant_received_wait_is_rejected_in_isolated_process() {
    const CASE: &str = "filter-reentrant-received-wait";
    if std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() != Ok(CASE) {
        support::isolated_process::run_case(
            "test_filter_reentrant_received_wait_is_rejected_in_isolated_process",
            CASE,
        );
        return;
    }

    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("filter-wait").expect("provider"), fake.clone()).expect("bus");
    let topic = Topic::<u32>::new("filter.wait").expect("topic");
    let filter_bus = bus.clone();
    let filter_topic = topic.clone();
    let filter_fake = fake.clone();
    let (entered_filter, filter_entered) = std::sync::mpsc::channel();
    let (continue_filter, filter_continue) = std::sync::mpsc::channel();
    let filter_continue = Arc::new(std::sync::Mutex::new(filter_continue));
    let filter_continue_in_closure = filter_continue.clone();
    let options = SubscribeOptions::builder()
        .filter(move |_| {
            assert!(matches!(
                block_on(filter_bus.wait_for_received_deliveries(&filter_topic, None)),
                Err(LifecycleError::WouldDeadlock {
                    operation: "wait_for_received_deliveries"
                })
            ));
            assert_eq!(filter_fake.shutdown_transition_count(), 0);
            entered_filter.send(()).expect("test driver is waiting");
            filter_continue_in_closure
                .lock()
                .expect("filter continuation lock")
                .recv()
                .expect("test driver releases filter");
            false
        })
        .build();
    let request = SubscribeRequest::new("filter-wait", topic)
        .expect("request")
        .with_options(options);
    let sub = block_on(bus.subscribe(request)).expect("subscribe");
    fake.enqueue(inbound_message(None));
    let runner = std::thread::spawn(move || block_on(sub.run(|_| async { Ok(()) })));
    filter_entered.recv().expect("filter entered");
    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(poll_once(shutdown.as_mut()).is_pending());
    continue_filter.send(()).expect("filter continues");
    let _ = block_on(shutdown).expect("external shutdown");
    runner.join().expect("runner thread").expect("runner");
}

#[test]
fn test_filter_poll_context_is_limited_to_its_own_bus() {
    const CASE: &str = "filter-own-bus-context";
    if std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() != Ok(CASE) {
        support::isolated_process::run_case("test_filter_poll_context_is_limited_to_its_own_bus", CASE);
        return;
    }
    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let other_fake = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("filter-owner").expect("provider"), fake.clone()).expect("bus");
    let other_bus = AsyncEventBus::from_spi(ProviderId::new("filter-other").expect("provider"), other_fake.clone())
        .expect("other bus");
    let filter_bus = other_bus.clone();
    let other_topic = Topic::<u32>::new("filter.owner").expect("other topic");
    let (entered_filter, filter_entered) = std::sync::mpsc::channel();
    let (continue_filter, filter_continue) = std::sync::mpsc::channel();
    let filter_continue = Arc::new(std::sync::Mutex::new(filter_continue));
    let filter_continue_in_closure = filter_continue.clone();
    let options = SubscribeOptions::builder()
        .filter(move |_| {
            assert!(matches!(
                block_on(filter_bus.wait_for_received_deliveries(&other_topic, Some(Duration::ZERO))),
                Ok(WaitOutcome::Idle)
            ));
            entered_filter.send(()).expect("test driver is waiting");
            filter_continue_in_closure
                .lock()
                .expect("filter continuation lock")
                .recv_timeout(Duration::from_secs(3))
                .expect("test driver releases filter within the handshake deadline");
            false
        })
        .build();
    let request = SubscribeRequest::new("filter-owner", Topic::<u32>::new("filter.owner").expect("topic"))
        .expect("request")
        .with_options(options);
    let sub = block_on(bus.subscribe(request)).expect("subscribe");
    fake.enqueue(inbound_message(None));
    let runner = std::thread::spawn(move || block_on(sub.run(|_| async { Ok(()) })));
    filter_entered
        .recv_timeout(Duration::from_secs(3))
        .expect("filter enters before the handshake deadline");
    let mut shutdown = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(poll_once(shutdown.as_mut()).is_pending());
    continue_filter.send(()).expect("filter continues");
    let _ = block_on(shutdown).expect("owner shutdown");
    runner.join().expect("runner thread").expect("runner");
    assert_eq!(other_fake.shutdown_transition_count(), 0);
    assert_eq!(fake.shutdown_transition_count(), 1);
}

#[test]
fn test_pending_runner_poll_does_not_leave_bus_context_active() {
    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("pending-poll").expect("provider"), fake.clone()).expect("bus");
    let topic = Topic::<u32>::new("pending.poll").expect("topic");
    let sub = block_on(bus.subscribe(SubscribeRequest::new("pending-poll", topic.clone()).expect("request")))
        .expect("subscribe");
    let mut run = Box::pin(sub.run(|_| async { Ok(()) }));
    assert!(poll_once(run.as_mut()).is_pending());
    assert!(block_on(bus.wait_for_received_deliveries(&topic, Some(Duration::ZERO))).is_ok());
    drop(run);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
}

#[test]
fn test_dropping_pending_received_wait_releases_last_waker_during_active_delivery() {
    const CASE: &str = "received-wait-cancel-callbacks";
    if std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() != Ok(CASE) {
        support::isolated_process::run_case_with_timeout(
            "test_dropping_pending_received_wait_releases_last_waker_during_active_delivery",
            CASE,
            Duration::from_secs(10),
        );
        return;
    }

    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let topic = Topic::<u32>::new("received.drop").expect("topic");
    let bus_slot = Arc::new(Mutex::new(None));
    let timer_observed = Arc::new(Mutex::new(None));
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("received-drop").expect("provider"),
        fake.clone(),
        EventBusFacadeConfig::default(),
        Arc::new(WaitDropTimer {
            clock: ManualMonotonicClock::new(),
            bus: bus_slot.clone(),
            topic: topic.clone(),
            observed: timer_observed.clone(),
        }),
    )
    .expect("bus");
    *bus_slot.lock().expect("timer bus slot") = Some(bus.clone());
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let resume_rx = Arc::new(Mutex::new(resume_rx));
    let sub = block_on(bus.subscribe(SubscribeRequest::new("received-drop", topic.clone()).expect("request")))
        .expect("subscribe");
    fake.enqueue(inbound_message(None));
    let handler_resume = resume_rx.clone();
    let runner = std::thread::spawn(move || {
        block_on(sub.run(move |_| {
            let entered_tx = entered_tx.clone();
            let handler_resume = handler_resume.clone();
            async move {
                entered_tx.send(()).expect("signal active handler");
                handler_resume
                    .lock()
                    .expect("handler resume receiver")
                    .recv_timeout(Duration::from_secs(3))
                    .expect("test releases active handler");
                Ok(())
            }
        }))
    });
    entered_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("handler has an accepted delivery");

    let observed = Arc::new(Mutex::new(None));
    let waker = Waker::from(Arc::new(WaitDropProbe {
        bus: bus.clone(),
        topic: topic.clone(),
        observed: observed.clone(),
    }));
    let mut observer = Box::pin(bus.wait_for_received_deliveries(&topic, None));
    assert!(observer.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    drop(waker);
    drop(observer);

    assert!(
        matches!(
            *observed.lock().expect("drop probe observation"),
            Some(Poll::Ready(Err(LifecycleError::WouldDeadlock {
                operation: "wait_for_received_deliveries"
            })))
        ),
        "last registered waker reentry is rejected while the wait is being dropped"
    );

    let mut timed_observer = Box::pin(bus.wait_for_received_deliveries(&topic, Some(Duration::from_secs(1))));
    assert!(
        timed_observer
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(timed_observer);
    assert!(
        matches!(
            *timer_observed.lock().expect("timer drop observation"),
            Some(Poll::Ready(Err(LifecycleError::WouldDeadlock {
                operation: "wait_for_received_deliveries"
            })))
        ),
        "timer future drop reentry is rejected while the wait is being dropped"
    );

    resume_tx.send(()).expect("resume handler");
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
    runner.join().expect("runner thread").expect("runner");
    *bus_slot.lock().expect("timer bus slot") = None;
}

#[test]
fn test_pending_received_wait_repolled_from_handler_is_rejected_then_observes_idle() {
    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("received-repoll").expect("provider"), fake.clone()).expect("bus");
    let topic = Topic::<u32>::new("received.repoll").expect("topic");
    let future_slot = Arc::new(Mutex::new(None::<ReceivedWaitFuture>));
    let callback_slot = future_slot.clone();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let resume_rx = Arc::new(Mutex::new(resume_rx));
    let (rejected_tx, rejected_rx) = std::sync::mpsc::channel();
    let sub = block_on(bus.subscribe(SubscribeRequest::new("received-repoll", topic.clone()).expect("request")))
        .expect("subscribe");
    fake.enqueue(inbound_message(None));
    let callback_resume = resume_rx.clone();
    let runner = std::thread::spawn(move || {
        block_on(sub.run(move |_| {
            let callback_slot = callback_slot.clone();
            let callback_resume = callback_resume.clone();
            let entered_tx = entered_tx.clone();
            let rejected_tx = rejected_tx.clone();
            async move {
                entered_tx.send(()).expect("signal active handler");
                callback_resume
                    .lock()
                    .expect("resume receiver lock")
                    .recv()
                    .expect("external observer permits nested poll");
                let mut future = callback_slot
                    .lock()
                    .expect("future slot")
                    .take()
                    .expect("pending future");
                let rejected = matches!(
                    future.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                    Poll::Ready(Err(LifecycleError::WouldDeadlock {
                        operation: "wait_for_received_deliveries"
                    }))
                );
                rejected_tx.send(rejected).expect("send nested poll result");
                Ok(())
            }
        }))
    });

    entered_rx
        .recv_timeout(Duration::from_secs(3))
        .expect("handler has an accepted delivery");
    let wait_bus = bus.clone();
    let wait_topic = topic.clone();
    let mut future: ReceivedWaitFuture =
        Box::pin(async move { wait_bus.wait_for_received_deliveries(&wait_topic, None).await });
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    *future_slot.lock().expect("future slot") = Some(future);
    resume_tx.send(()).expect("resume handler");
    assert!(
        rejected_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("nested wait completes")
    );
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
    runner.join().expect("runner thread").expect("runner");
    assert_eq!(
        block_on(bus.wait_for_received_deliveries(&topic, Some(Duration::from_secs(3)))).expect("idle wait"),
        WaitOutcome::Idle
    );
}

#[test]
fn test_filter_panic_does_not_leave_bus_context_active() {
    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("panic-poll").expect("provider"), fake.clone()).expect("bus");
    let topic = Topic::<u32>::new("panic.poll").expect("topic");
    let options = SubscribeOptions::builder().filter(|_| panic!("filter panic")).build();
    let request = SubscribeRequest::new("panic-poll", topic.clone())
        .expect("request")
        .with_options(options);
    let sub = block_on(bus.subscribe(request)).expect("subscribe");
    fake.enqueue(inbound_message(None));
    let mut run = Box::pin(sub.run(|_| async { Ok(()) }));
    assert!(poll_once(run.as_mut()).is_pending());
    drop(run);
    assert!(block_on(bus.wait_for_received_deliveries(&topic, Some(Duration::ZERO))).is_ok());
    assert_eq!(fake.shutdown_transition_count(), 0);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("shutdown");
}
