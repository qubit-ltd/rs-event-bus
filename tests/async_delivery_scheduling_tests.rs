// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Caller-driven scheduling and bounded settlement contracts.
mod support;

use std::collections::VecDeque;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
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
use qubit_event_bus::AsyncSubscription;
use qubit_event_bus::CodecError;
use qubit_event_bus::Diagnostic;
use qubit_event_bus::ReceiveError;
use qubit_event_bus::SpiError;
use qubit_event_bus::SubscribeError;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::facade::DeliverySchedulingConfig;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::SettlementRetryConfig;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::SettlementTermination;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionStopReason;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::InboundMessage;
use qubit_event_bus::spi::OrderingKey;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;
use support::fake_spi::FakeAsyncEventBusSpi;
use support::fake_spi::inbound_message;
use support::fake_spi::native_no_settlement_capabilities;
use support::manual_async::block_on;
use support::manual_async::poll_once;

/// Permanent provider evidence stops immediately and preserves the structured
/// cause.
#[test]
fn test_permanent_settlement_stops_after_one_attempt() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("fake").expect("static test provider ID must be valid"),
        spi.clone(),
    )
    .expect("test provider capabilities must be valid");
    let topic = Topic::<u32>::new("orders.created").expect("static test topic must be valid");
    let mut sub = block_on(
        bus.subscribe(SubscribeRequest::new("permanent", topic).expect("test subscription request must be valid")),
    )
    .expect("test subscription must be accepted");
    spi.fail_all_settles();
    spi.enqueue(inbound_message(Some(SettlementToken::new(sub.id(), "token"))));
    let mut run = Box::pin(sub.run(|_| async { Ok(()) }));
    let mut result = Poll::Pending;
    for _ in 0..16 {
        result = poll_once(run.as_mut());
        if result.is_ready() {
            break;
        }
    }
    assert!(
        matches!(result, Poll::Ready(Err(ReceiveError::Stopped(ref reason))) if matches!(reason.as_ref(), SubscriptionStopReason::Settlement { attempts: 1, termination: SettlementTermination::PermanentError, .. })),
        "permanent settlement must converge in one poll: {result:?}"
    );
    assert_eq!(spi.operation_log().iter().filter(|op| **op == "settle").count(), 1);
}

/// Shared observations and controls for scripted settlement/concurrency tests.
#[derive(Default)]
struct Probe {
    attempts: Mutex<Vec<(usize, DeliveryDisposition)>>,
    failures: Mutex<VecDeque<Option<bool>>>,
    reject_failures: AtomicUsize,
    pause: AtomicBool,
    gate: AtomicBool,
    receives: AtomicUsize,
    closes: AtomicUsize,
}
/// SPI adapter that instruments a fake provider while preserving its behavior.
struct ProbeBus {
    fake: Arc<FakeAsyncEventBusSpi>,
    probe: Arc<Probe>,
}
impl AsyncEventBusSpi for ProbeBus {
    fn capabilities(&self) -> EventBusCapabilities {
        self.fake.capabilities()
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.fake.publish(message)
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move {
            Ok(Box::new(ProbeReceiver {
                receiver: self.fake.subscribe(request).await?,
                probe: self.probe.clone(),
                active: Arc::new(AtomicBool::new(false)),
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.fake.shutdown(mode)
    }
}
/// Marks a receiver operation active and asserts that SPI operations stay
/// exclusive.
struct OperationGuard(Arc<AtomicBool>);
impl OperationGuard {
    fn new(flag: Arc<AtomicBool>) -> Self {
        assert!(
            !flag.swap(true, Ordering::SeqCst),
            "receiver operations must be exclusive"
        );
        Self(flag)
    }
}
impl Drop for OperationGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}
/// Receiver adapter that records settlement attempts and enforces operation
/// exclusivity.
struct ProbeReceiver {
    receiver: Box<dyn AsyncEventSubscriptionSpi>,
    probe: Arc<Probe>,
    active: Arc<AtomicBool>,
}
impl AsyncEventSubscriptionSpi for ProbeReceiver {
    fn receive<'a>(&'a mut self, timeout: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        Box::pin(async move {
            let _operation = OperationGuard::new(self.active.clone());
            let outcome = self.receiver.receive(timeout).await?;
            if matches!(outcome, ReceiveOutcome::Message(_)) {
                self.probe.receives.fetch_add(1, Ordering::SeqCst);
            }
            Ok(outcome)
        })
    }
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        let identity = token
            .downcast_ref::<String>()
            .map_or(token as *const SettlementToken as usize, |token| {
                token as *const String as usize
            });
        Box::pin(async move {
            let _operation = OperationGuard::new(self.active.clone());
            self.probe
                .attempts
                .lock()
                .expect("settlement attempt log mutex must not be poisoned")
                .push((identity, disposition));
            if self.probe.pause.swap(false, Ordering::SeqCst) {
                std::future::poll_fn(|_| {
                    if self.probe.gate.load(Ordering::SeqCst) {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                })
                .await;
            }
            let reject_failure = disposition == DeliveryDisposition::Reject
                && self
                    .probe
                    .reject_failures
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| remaining.checked_sub(1))
                    .is_ok();
            let failure = if reject_failure {
                Some(Some(true))
            } else {
                self.probe
                    .failures
                    .lock()
                    .expect("injected failure queue mutex must not be poisoned")
                    .pop_front()
            };
            if let Some(retryable) = failure {
                return Err(SpiError::Operation {
                    provider_id: "probe".into(),
                    operation: "settle",
                    resource: None,
                    kind: "injected",
                    retryable,
                    source: Box::new(std::io::Error::other("probe failure")),
                });
            }
            Ok(())
        })
    }
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(async move {
            let _operation = OperationGuard::new(self.active.clone());
            self.probe.closes.fetch_add(1, Ordering::SeqCst);
            self.receiver.close().await
        })
    }
}

/// Builds explicit independent capacity and finite retry limits.
fn config(running: usize, owned: usize, per_sub: usize, subscriptions: usize, attempts: u32) -> EventBusFacadeConfig {
    EventBusFacadeConfig::new()
        .with_delivery_scheduling(
            DeliverySchedulingConfig::new(
                NonZeroUsize::new(running).expect("running handler capacity must be nonzero"),
                NonZeroUsize::new(owned).expect("owned delivery capacity must be nonzero"),
                NonZeroUsize::new(per_sub).expect("per-subscription capacity must be nonzero"),
                NonZeroUsize::new(subscriptions).expect("subscription capacity must be nonzero"),
            )
            .expect("delivery scheduling configuration must be valid"),
        )
        .with_settlement_retry(
            SettlementRetryConfig::new(
                NonZeroU32::new(attempts).expect("settlement attempt log mutex must not be poisoned"),
                Duration::from_secs(1),
                Duration::from_millis(10),
                Duration::from_millis(100),
            )
            .expect("settlement retry configuration must be valid"),
        )
}
/// Creates a real facade with the controllable SPI and its own manual time
/// domain.
fn setup(
    config: EventBusFacadeConfig,
) -> (
    AsyncEventBus,
    Arc<FakeAsyncEventBusSpi>,
    Arc<Probe>,
    ManualMonotonicClock,
) {
    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let probe = Arc::new(Probe::default());
    let clock = ManualMonotonicClock::new();
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("probe").expect("static test provider ID must be valid"),
        Arc::new(ProbeBus {
            fake: fake.clone(),
            probe: probe.clone(),
        }),
        config,
        clock.new_timer(),
    )
    .expect("test provider capabilities must be valid");
    (bus, fake, probe, clock)
}
/// Registers an ordered u32 subscription through the public facade.
fn subscribe(bus: &AsyncEventBus, name: &str) -> AsyncSubscription<u32> {
    let request = SubscribeRequest::new(
        name,
        Topic::<u32>::new("test.topic").expect("static test topic must be valid"),
    )
    .expect("test subscription request must be valid")
    .with_options(
        SubscribeOptions::builder()
            .ordering_policy(OrderingPolicy::PerKey)
            .build(),
    );
    block_on(bus.subscribe(request)).expect("test subscription must be accepted")
}
/// Places a uniquely identified message and token in the fake receiver.
fn enqueue(fake: &FakeAsyncEventBusSpi, id: Id, value: u32, key: &str) {
    fake.enqueue(InboundMessage::new(
        TopicAddress::new("test.topic").expect("static SPI topic address must be valid"),
        EventId::new(format!("event-{value}")).expect("generated test event ID must be valid"),
        std::time::SystemTime::UNIX_EPOCH,
        Headers::new(),
        Some(OrderingKey::new(key).expect("static ordering key must be valid")),
        TransportPayload::Native(Arc::new(value)),
        Some(SettlementToken::new(id, format!("token-{value}"))),
        Default::default(),
    ));
}
/// Drives bounded executor turns while asserting that the receiver remains
/// active.
fn poll_pending<F: std::future::Future>(mut future: std::pin::Pin<&mut F>) {
    for _ in 0..16 {
        assert!(poll_once(future.as_mut()).is_pending(), "runner stays live");
    }
}

/// Missing retryability evidence must not trigger an automatic settlement
/// retry.
#[test]
fn test_unknown_retryability_stops() {
    let (bus, fake, probe, _) = setup(config(1, 4, 4, 2, 5));
    probe
        .failures
        .lock()
        .expect("injected failure queue mutex must not be poisoned")
        .push_back(None);
    let mut sub = subscribe(&bus, "unknown");
    enqueue(&fake, sub.id(), 1, "A");
    let mut run = Box::pin(sub.run(|_| async { Ok(()) }));
    let mut result = Poll::Pending;
    for _ in 0..16 {
        result = poll_once(run.as_mut());
        if result.is_ready() {
            break;
        }
    }
    assert!(
        matches!(result, Poll::Ready(Err(ReceiveError::Stopped(ref reason))) if matches!(reason.as_ref(), SubscriptionStopReason::Settlement { attempts: 1, termination: SettlementTermination::RetryabilityUnknown, .. })),
        "{result:?}"
    );
    assert_eq!(
        probe
            .attempts
            .lock()
            .expect("settlement attempt log mutex must not be poisoned")
            .len(),
        1
    );
}

/// Paused backoff retains token, counters, lane ownership, and the original
/// handler result.
#[test]
fn test_drop_run_during_settlement_backoff_resumes_same_token() {
    let (bus, fake, probe, clock) = setup(config(1, 4, 4, 2, 3));
    probe
        .failures
        .lock()
        .expect("injected failure queue mutex must not be poisoned")
        .push_back(Some(true));
    let mut sub = subscribe(&bus, "resume-backoff");
    enqueue(&fake, sub.id(), 1, "A");
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let mut run = Box::pin(sub.run(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
        async { Ok(()) }
    }));
    poll_pending(run.as_mut());
    drop(run);
    assert_eq!(
        probe
            .attempts
            .lock()
            .expect("settlement attempt log mutex must not be poisoned")
            .len(),
        1
    );
    assert_eq!(probe.closes.load(Ordering::SeqCst), 0);
    let paused = sub.delivery_metrics().metrics;
    assert_eq!(
        (
            paused.reserved_receives,
            paused.queued,
            paused.running_handlers,
            paused.settling
        ),
        (0, 0, 0, 1)
    );
    assert_eq!(
        (paused.settlement_attempts, paused.settlement_retries, paused.completed),
        (1, 0, 0)
    );
    clock
        .advance(Duration::from_millis(10))
        .expect("manual clock advance must remain in its clock domain");
    let mut resumed = Box::pin(sub.run(|_| async { panic!("completed handler must not restart") }));
    poll_pending(resumed.as_mut());
    let attempts = probe
        .attempts
        .lock()
        .expect("settlement attempt log mutex must not be poisoned");
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0], attempts[1]);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(attempts);
    drop(resumed);
    let completed = sub.delivery_metrics().metrics;
    assert_eq!(
        (
            completed.settlement_attempts,
            completed.settlement_retries,
            completed.completed
        ),
        (2, 1, 1)
    );
    assert_eq!(
        (completed.handler_duration_count, completed.settlement_duration_count),
        (1, 1)
    );
    assert_eq!(completed.settlement_duration_total_nanos, 10_000_000);
    block_on(sub.close()).expect("subscription close must succeed");
    assert_eq!(sub.delivery_metrics().metrics, completed);
}

/// Cancelled in-flight SPI work consumes one attempt and waits before
/// idempotent retry.
#[test]
fn test_drop_inflight_attempt_counts_budget_and_backoff_without_stopping_on_pause() {
    let (bus, fake, probe, clock) = setup(config(1, 4, 4, 2, 2));
    probe.pause.store(true, Ordering::SeqCst);
    let mut sub = subscribe(&bus, "resume-attempt");
    enqueue(&fake, sub.id(), 1, "A");
    let mut run = Box::pin(sub.run(|_| async { Ok(()) }));
    poll_pending(run.as_mut());
    drop(run);
    assert!(sub.terminal_failure().is_none());
    assert_eq!(probe.closes.load(Ordering::SeqCst), 0);
    let mut resumed = Box::pin(sub.run(|_| async { panic!("handler cannot rerun") }));
    poll_pending(resumed.as_mut());
    assert_eq!(
        probe
            .attempts
            .lock()
            .expect("settlement attempt log mutex must not be poisoned")
            .len(),
        1
    );
    clock
        .advance(Duration::from_millis(10))
        .expect("manual clock advance must remain in its clock domain");
    poll_pending(resumed.as_mut());
    let attempts = probe
        .attempts
        .lock()
        .expect("settlement attempt log mutex must not be poisoned");
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0], attempts[1]);
}

/// Dropping the final admitted attempt pauses first and reports exhaustion only
/// on resume.
#[test]
fn test_cancelled_last_attempt_only_stops_on_resume() {
    let (bus, fake, probe, _) = setup(config(1, 4, 4, 2, 1));
    probe.pause.store(true, Ordering::SeqCst);
    let mut sub = subscribe(&bus, "last-attempt");
    enqueue(&fake, sub.id(), 1, "A");
    let mut run = Box::pin(sub.run(|_| async { Ok(()) }));
    poll_pending(run.as_mut());
    drop(run);
    assert!(sub.terminal_failure().is_none());
    let result = block_on(sub.run(|_| async { panic!("handler cannot rerun") }));
    assert!(
        matches!(result, Err(ReceiveError::Stopped(ref reason)) if matches!(reason.as_ref(), SubscriptionStopReason::Settlement { attempts: 1, termination: SettlementTermination::AttemptsExhausted, error, .. } if error.kind() == "settlement_attempt_cancelled"))
    );
    assert_eq!(
        probe
            .attempts
            .lock()
            .expect("settlement attempt log mutex must not be poisoned")
            .len(),
        1
    );
}

/// Constructing a run future alone cannot receive messages or invoke
/// user/provider work.
#[test]
fn test_unpolled_run_has_no_handler_or_settlement_side_effects() {
    let (bus, fake, probe, _) = setup(config(1, 4, 4, 2, 1));
    let mut sub = subscribe(&bus, "unpolled");
    enqueue(&fake, sub.id(), 1, "A");
    drop(sub.run(|_| async { panic!("unpolled handler") }));
    assert_eq!(probe.receives.load(Ordering::SeqCst), 0);
    assert!(
        probe
            .attempts
            .lock()
            .expect("settlement attempt log mutex must not be poisoned")
            .is_empty()
    );
}

/// An eligible cold key progresses beside a blocked hot key without exceeding
/// owned capacity.
#[test]
fn test_hot_backlog_does_not_take_handler_slots_and_owned_is_bounded() {
    let (bus, fake, probe, _) = setup(config(2, 4, 4, 2, 3));
    let mut sub = subscribe(&bus, "hot");
    for (value, key) in [(1, "A"), (2, "A"), (3, "B"), (4, "A"), (5, "C")] {
        enqueue(&fake, sub.id(), value, key);
    }
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_handler = seen.clone();
    let mut run = Box::pin(sub.run(move |delivery| {
        seen_handler
            .lock()
            .expect("observed delivery log mutex must not be poisoned")
            .push(*delivery.payload());
        async {
            std::future::pending::<()>().await;
            Ok(())
        }
    }));
    poll_pending(run.as_mut());
    assert_eq!(
        *seen.lock().expect("observed delivery log mutex must not be poisoned"),
        [1, 3]
    );
    assert_eq!(probe.receives.load(Ordering::SeqCst), 4);
}

/// An unadvanced settlement timer does not stall an already-running handler.
#[test]
fn test_backoff_keeps_polling_another_started_handler() {
    let (bus, fake, probe, _) = setup(config(2, 4, 4, 2, 3));
    let mut sub = subscribe(&bus, "backoff-progress");
    enqueue(&fake, sub.id(), 1, "A");
    enqueue(&fake, sub.id(), 2, "B");
    let gate = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicUsize::new(0));
    let handler_gate = gate.clone();
    let handler_done = done.clone();
    let mut run = Box::pin(sub.run(move |delivery| {
        let gate = handler_gate.clone();
        let done = handler_done.clone();
        let value = *delivery.payload();
        async move {
            std::future::poll_fn(|_| {
                if gate.load(Ordering::SeqCst) || value == 2 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            done.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }));
    probe
        .failures
        .lock()
        .expect("injected failure queue mutex must not be poisoned")
        .push_back(Some(true));
    poll_pending(run.as_mut());
    assert_eq!(done.load(Ordering::SeqCst), 1);
    gate.store(true, Ordering::SeqCst);
    poll_pending(run.as_mut());
    assert_eq!(
        done.load(Ordering::SeqCst),
        2,
        "handler completes while B settlement timer remains unadvanced"
    );
}

/// An exclusive pending settle continues polling existing handler futures.
#[test]
fn test_inflight_settlement_keeps_polling_other_started_handler() {
    let (bus, fake, probe, _) = setup(config(2, 4, 4, 2, 3));
    let mut sub = subscribe(&bus, "inflight-progress");
    enqueue(&fake, sub.id(), 1, "A");
    enqueue(&fake, sub.id(), 2, "B");
    let gate = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicUsize::new(0));
    let handler_gate = gate.clone();
    let handler_done = done.clone();
    let mut run = Box::pin(sub.run(move |delivery| {
        let gate = handler_gate.clone();
        let done = handler_done.clone();
        let value = *delivery.payload();
        async move {
            std::future::poll_fn(|_| {
                if gate.load(Ordering::SeqCst) || value == 2 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            done.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }));
    probe.pause.store(true, Ordering::SeqCst);
    poll_pending(run.as_mut());
    gate.store(true, Ordering::SeqCst);
    poll_pending(run.as_mut());
    assert_eq!(done.load(Ordering::SeqCst), 2);
    assert_eq!(probe.receives.load(Ordering::SeqCst), 2);
}

/// Pause retains the subscription registration cap and close releases it.
#[test]
fn test_paused_session_keeps_registration_until_close() {
    let (bus, _, _, _) = setup(config(1, 2, 2, 1, 2));
    let mut sub = subscribe(&bus, "one");
    let mut run = Box::pin(sub.run(|_| async { Ok(()) }));
    poll_pending(run.as_mut());
    drop(run);
    let request = || {
        SubscribeRequest::new(
            "two",
            Topic::<u32>::new("test.topic").expect("static test topic must be valid"),
        )
        .expect("test subscription request must be valid")
    };
    assert!(matches!(
        block_on(bus.subscribe(request())),
        Err(SubscribeError::ResourceLimit { limit: 1, .. })
    ));
    block_on(sub.close()).expect("subscription close must succeed");
    assert!(block_on(bus.subscribe(request())).is_ok());
}

/// A released execution slot wakes the next subscription and respects its RR
/// turn.
#[test]
fn test_ready_subscriptions_rotate_and_scheduler_notifies_other_session() {
    /// Test waker that counts scheduler notifications.
    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (bus, _, _, _) = setup(config(1, 8, 4, 2, 3));
    let mut first = subscribe(&bus, "first");
    let mut second = subscribe(&bus, "second");
    for value in [1u32, 2] {
        let _ = block_on(
            bus.publish(
                PublishRequest::new(
                    Topic::new("test.topic").expect("static test topic must be valid"),
                    value,
                )
                .expect("test publish request must be valid"),
            ),
        )
        .expect("test publish must be accepted");
    }
    let gate = Arc::new(AtomicBool::new(false));
    let first_gate = gate.clone();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let first_seen = seen.clone();
    let second_seen = seen.clone();
    let mut first_run = Box::pin(first.run(move |delivery| {
        let gate = first_gate.clone();
        first_seen
            .lock()
            .expect("observed delivery log mutex must not be poisoned")
            .push(("first", *delivery.payload()));
        async move {
            std::future::poll_fn(|_| {
                if gate.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            Ok(())
        }
    }));
    let mut second_run = Box::pin(second.run(move |delivery| {
        second_seen
            .lock()
            .expect("observed delivery log mutex must not be poisoned")
            .push(("second", *delivery.payload()));
        async {
            std::future::pending::<()>().await;
            Ok(())
        }
    }));
    poll_pending(first_run.as_mut());
    let wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let mut context = Context::from_waker(&waker);
    for _ in 0..16 {
        assert!(std::future::Future::poll(second_run.as_mut(), &mut context).is_pending());
    }
    wake.0.store(0, Ordering::SeqCst);
    gate.store(true, Ordering::SeqCst);
    poll_pending(first_run.as_mut());
    assert!(
        wake.0.load(Ordering::SeqCst) > 0,
        "releasing H wakes the other eligible session"
    );
    poll_pending(second_run.as_mut());
    assert_eq!(
        *seen.lock().expect("observed delivery log mutex must not be poisoned"),
        [("first", 1), ("second", 1)],
        "second subscription receives next H grant before first's queued successor"
    );
}

/// Reaching the total deadline cannot admit another provider call.
#[test]
fn test_backoff_deadline_stops_without_extra_attempt() {
    let (bus, fake, probe, clock) = setup(config(1, 4, 4, 2, 5));
    probe
        .failures
        .lock()
        .expect("injected failure queue mutex must not be poisoned")
        .push_back(Some(true));
    let mut sub = subscribe(&bus, "deadline");
    enqueue(&fake, sub.id(), 1, "A");
    let mut run = Box::pin(sub.run(|_| async { Ok(()) }));
    poll_pending(run.as_mut());
    clock
        .advance(Duration::from_secs(1))
        .expect("manual clock must advance to the settlement deadline");
    let result = poll_once(run.as_mut());
    assert!(
        matches!(result, Poll::Ready(Err(ReceiveError::Stopped(ref reason))) if matches!(reason.as_ref(), SubscriptionStopReason::Settlement { attempts: 1, termination: SettlementTermination::DeadlineExceeded, .. })),
        "{result:?}"
    );
    assert_eq!(
        probe
            .attempts
            .lock()
            .expect("settlement attempt log mutex must not be poisoned")
            .len(),
        1
    );
}

/// A permanent settlement failure affects only its owning subscription.
#[test]
fn test_terminal_subscription_does_not_stop_other_session() {
    let (bus, _, probe, _) = setup(config(1, 4, 2, 2, 3));
    let mut first = subscribe(&bus, "terminal");
    let mut second = subscribe(&bus, "healthy");
    let _ = block_on(
        bus.publish(
            PublishRequest::new(Topic::new("test.topic").expect("static test topic must be valid"), 1u32)
                .expect("test publish request must be valid"),
        ),
    )
    .expect("test publish must be accepted");
    probe
        .failures
        .lock()
        .expect("injected failure queue mutex must not be poisoned")
        .push_back(Some(false));
    assert!(matches!(
        block_on(first.run(|_| async { Ok(()) })),
        Err(ReceiveError::Stopped(_))
    ));
    let called = Arc::new(AtomicUsize::new(0));
    let seen = called.clone();
    let mut healthy = Box::pin(second.run(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
        async { Ok(()) }
    }));
    poll_pending(healthy.as_mut());
    assert_eq!(called.load(Ordering::SeqCst), 1);
}

/// Decode rejection uses settlement ownership directly and consumes no handler
/// slot.
#[test]
fn test_decode_rejection_runs_while_handler_capacity_is_full() {
    /// Codec double that rejects malformed UTF-8 payloads.
    struct Utf8Codec(ContentType);
    impl EventCodec<String> for Utf8Codec {
        fn content_type(&self) -> &ContentType {
            &self.0
        }
        fn schema_id(&self) -> Option<&SchemaId> {
            None
        }
        fn encode(&self, value: &String) -> Result<Arc<[u8]>, CodecError> {
            Ok(Arc::from(value.as_bytes()))
        }
        fn decode(&self, value: &EncodedPayload) -> Result<String, CodecError> {
            String::from_utf8(value.bytes().to_vec()).map_err(|source| CodecError::Decode {
                source: Box::new(source),
            })
        }
    }
    let (bus, fake, probe, _) = setup(config(1, 4, 4, 2, 3));
    let topic = Topic::new("test.topic")
        .expect("static test topic must be valid")
        .with_codec(Utf8Codec(
            ContentType::new("text/plain").expect("static codec content type must be valid"),
        ));
    let mut sub = block_on(
        bus.subscribe(SubscribeRequest::new("decode", topic).expect("test subscription request must be valid")),
    )
    .expect("test subscription must be accepted");
    for (id, payload) in [
        ("valid", TransportPayload::Native(Arc::new("valid".to_owned()))),
        (
            "invalid",
            TransportPayload::Encoded(EncodedPayload::new(
                Arc::from([0xffu8]),
                ContentType::new("text/plain").expect("static codec content type must be valid"),
                None,
            )),
        ),
    ] {
        fake.enqueue(InboundMessage::new(
            TopicAddress::new("test.topic").expect("static SPI topic address must be valid"),
            EventId::new(id).expect("generated test event ID must be valid"),
            std::time::SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            payload,
            Some(SettlementToken::new(sub.id(), id.to_owned())),
            Default::default(),
        ));
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = calls.clone();
    let mut run = Box::pin(sub.run(move |_| {
        seen.fetch_add(1, Ordering::SeqCst);
        async {
            std::future::pending::<()>().await;
            Ok(())
        }
    }));
    poll_pending(run.as_mut());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let attempts = probe
        .attempts
        .lock()
        .expect("settlement attempt log mutex must not be poisoned");
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].1, DeliveryDisposition::Reject);
    assert_eq!(bus.delivery_metrics().running_handlers, 1);
    assert_eq!(bus.delivery_metrics().handler_duration_count, 0);
}

/// A cached terminal cause cannot skip paused handler and receiver cleanup.
#[test]
fn test_resume_after_terminal_failure_finishes_owned_handlers_and_closes() {
    let (bus, fake, probe, _) = setup(config(2, 4, 4, 2, 3));
    let mut sub = subscribe(&bus, "terminal-resume");
    enqueue(&fake, sub.id(), 1, "A");
    enqueue(&fake, sub.id(), 2, "B");
    let gate = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicUsize::new(0));
    let handler_gate = gate.clone();
    let handler_done = done.clone();
    probe
        .failures
        .lock()
        .expect("injected failure queue mutex must not be poisoned")
        .push_back(Some(false));
    let mut run = Box::pin(sub.run(move |delivery| {
        let gate = handler_gate.clone();
        let done = handler_done.clone();
        let value = *delivery.payload();
        async move {
            std::future::poll_fn(|_| {
                if gate.load(Ordering::SeqCst) || value == 2 {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            done.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }));
    poll_pending(run.as_mut());
    drop(run);
    assert!(sub.terminal_failure().is_some());
    gate.store(true, Ordering::SeqCst);
    assert!(matches!(
        block_on(sub.run(|_| async { panic!("resume must keep existing handler") })),
        Err(ReceiveError::Stopped(_))
    ));
    assert_eq!(
        done.load(Ordering::SeqCst),
        2,
        "terminal cancellation must preserve cleanup work for resume"
    );
    assert_eq!(probe.closes.load(Ordering::SeqCst), 1);
}

/// Timer failures at either boundary stop with their original source and no
/// extra SPI call.
#[test]
fn test_settlement_timer_registration_and_poll_failures_stop_without_extra_attempt() {
    /// Timer double that fails either at registration or when its future is
    /// polled.
    struct FailingTimer {
        clock: ManualMonotonicClock,
        fail_on_poll: bool,
    }
    impl Timer for FailingTimer {
        fn clock(&self) -> &dyn MonotonicClock {
            &self.clock
        }
        fn at(&self, _: MonotonicInstant) -> Result<TimerFuture, TimeError> {
            if self.fail_on_poll {
                Ok(Box::pin(async { Err(TimeError::InstantOverflow) }))
            } else {
                Err(TimeError::InstantOverflow)
            }
        }
    }
    for fail_on_poll in [false, true] {
        let fake = Arc::new(FakeAsyncEventBusSpi::new());
        let probe = Arc::new(Probe::default());
        probe
            .failures
            .lock()
            .expect("injected failure queue mutex must not be poisoned")
            .push_back(Some(true));
        let timer = Arc::new(FailingTimer {
            clock: ManualMonotonicClock::new(),
            fail_on_poll,
        });
        let bus = AsyncEventBus::with_config_and_timer(
            ProviderId::new("probe").expect("static test provider ID must be valid"),
            Arc::new(ProbeBus {
                fake: fake.clone(),
                probe: probe.clone(),
            }),
            config(1, 4, 4, 2, 3),
            timer,
        )
        .expect("test provider capabilities must be valid");
        let mut sub = subscribe(&bus, "clock-error");
        enqueue(&fake, sub.id(), 1, "A");
        let result = block_on(sub.run(|_| async { Ok(()) }));
        assert!(
            matches!(result, Err(ReceiveError::Stopped(ref reason)) if matches!(reason.as_ref(), SubscriptionStopReason::Settlement { attempts: 1, termination: SettlementTermination::InfrastructureFailure, error, .. } if std::error::Error::source(error.as_ref()).is_some_and(|source| source.is::<TimeError>())))
        );
        assert_eq!(
            probe
                .attempts
                .lock()
                .expect("settlement attempt log mutex must not be poisoned")
                .len(),
            1
        );
        assert_eq!(sub.delivery_metrics().metrics.settlement_terminal_failures, 1);
    }
}

/// Settlement remains the first cause while an independent close failure
/// survives for shutdown.
#[test]
fn test_close_failure_does_not_overwrite_first_settlement_cause() {
    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("fake").expect("static test provider ID must be valid"),
        fake.clone(),
    )
    .expect("test provider capabilities must be valid");
    let mut sub = subscribe(&bus, "first-cause");
    enqueue(&fake, sub.id(), 1, "A");
    fake.fail_all_settles();
    fake.panic_on_close_call();
    let result = block_on(sub.run(|_| async { Ok(()) }));
    let Err(ReceiveError::Stopped(reason)) = result else {
        panic!("settlement remains the primary error");
    };
    assert!(matches!(
        reason.as_ref(),
        SubscriptionStopReason::Settlement {
            termination: SettlementTermination::PermanentError,
            ..
        }
    ));
    assert!(Arc::ptr_eq(
        &reason,
        &sub.terminal_failure()
            .expect("terminal settlement reason must be retained")
    ));
    assert!(
        block_on(bus.shutdown(ShutdownMode::Immediate)).is_err(),
        "close failure is retained independently for shutdown"
    );
}

/// A misbehaving clock cannot recursively re-emit diagnostics through snapshot
/// observers.
#[test]
fn test_metrics_clock_failure_reentrant_observer_is_not_repeated() {
    /// Timer double that switches to a foreign clock to trigger snapshot
    /// errors.
    struct SwitchingTimer {
        normal: ManualMonotonicClock,
        foreign: ManualMonotonicClock,
        switched: AtomicBool,
    }
    impl Timer for SwitchingTimer {
        fn clock(&self) -> &dyn MonotonicClock {
            if self.switched.load(Ordering::SeqCst) {
                &self.foreign
            } else {
                &self.normal
            }
        }
        fn at(&self, deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
            self.clock().new_timer().at(deadline)
        }
    }
    for through_handle in [false, true] {
        let fake = Arc::new(FakeAsyncEventBusSpi::new());
        let timer = Arc::new(SwitchingTimer {
            normal: ManualMonotonicClock::new(),
            foreign: ManualMonotonicClock::new(),
            switched: AtomicBool::new(false),
        });
        let bus = AsyncEventBus::with_config_and_timer(
            ProviderId::new("clock-reentry").expect("static test provider ID must be valid"),
            fake.clone(),
            config(1, 4, 4, 2, 3),
            timer.clone(),
        )
        .expect("test provider capabilities must be valid");
        let mut sub = subscribe(&bus, "clock-reentry");
        enqueue(&fake, sub.id(), 1, "A");
        let mut run = Box::pin(sub.run(|_| async {
            std::future::pending::<()>().await;
            Ok(())
        }));
        poll_pending(run.as_mut());
        drop(run);
        let sub = Arc::new(sub);
        let weak_sub = Arc::downgrade(&sub);
        let callback_bus = bus.clone();
        let callbacks = Arc::new(AtomicUsize::new(0));
        let observed = callbacks.clone();
        let nested_snapshots = Arc::new(Mutex::new(Vec::new()));
        let captured_snapshots = nested_snapshots.clone();
        let _observer = bus.observe_diagnostics(move |diagnostic| {
            if matches!(diagnostic, Diagnostic::InternalFailure { origin, .. } if origin.as_ref() == "delivery_metrics_clock") {
                let count = observed.fetch_add(1, Ordering::SeqCst);
                // Bound recursion deliberately: failure is an assertion, never a stack overflow.
                if count < 3 {
                    let nested = if through_handle { weak_sub.upgrade().expect("subscription handle must remain alive during callback").delivery_metrics().metrics } else { callback_bus.delivery_metrics() };
                    captured_snapshots.lock().expect("reentrant snapshot log mutex must not be poisoned").push(nested);
                }
            }
        });
        timer.switched.store(true, Ordering::SeqCst);
        let snapshot = if through_handle {
            sub.delivery_metrics().metrics
        } else {
            bus.delivery_metrics()
        };
        assert_eq!(
            callbacks.load(Ordering::SeqCst),
            1,
            "first cause must be stored before invoking the observer"
        );
        let nested = nested_snapshots
            .lock()
            .expect("reentrant snapshot log mutex must not be poisoned");
        assert_eq!(nested.len(), 1, "observer completed its reentrant snapshot call");
        assert_eq!(nested[0].running_handlers, 1);
        assert_eq!(nested[0].oldest_owned_age, None);
        assert_eq!(snapshot.running_handlers, 1);
        assert!(sub.terminal_failure().is_some());
    }
}

/// Decode rejection retains the predecessor's ordered lane without consuming H.
#[test]
fn test_decode_rejection_waits_for_same_key_predecessor_and_blocks_successor() {
    /// Codec double that rejects every payload during decoding.
    struct RejectCodec(ContentType);
    impl EventCodec<u32> for RejectCodec {
        fn content_type(&self) -> &ContentType {
            &self.0
        }
        fn schema_id(&self) -> Option<&SchemaId> {
            None
        }
        fn encode(&self, _: &u32) -> Result<Arc<[u8]>, CodecError> {
            Ok(Arc::from([1u8]))
        }
        fn decode(&self, _: &EncodedPayload) -> Result<u32, CodecError> {
            Err(CodecError::Decode {
                source: Box::new(std::io::Error::other("malformed payload")),
            })
        }
    }
    let (bus, fake, probe, clock) = setup(config(1, 6, 6, 2, 3));
    let topic = Topic::new("test.topic")
        .expect("static test topic must be valid")
        .with_codec(RejectCodec(
            ContentType::new("application/test").expect("static codec content type must be valid"),
        ));
    let request = SubscribeRequest::new("ordered-decode", topic)
        .expect("test subscription request must be valid")
        .with_options(
            SubscribeOptions::builder()
                .ordering_policy(OrderingPolicy::PerKey)
                .build(),
        );
    let mut sub = block_on(bus.subscribe(request)).expect("test subscription must be accepted");
    for (id, payload) in [
        ("first", TransportPayload::Native(Arc::new(1u32))),
        (
            "malformed",
            TransportPayload::Encoded(EncodedPayload::new(
                Arc::from([0xffu8]),
                ContentType::new("application/test").expect("static codec content type must be valid"),
                None,
            )),
        ),
        ("third", TransportPayload::Native(Arc::new(3u32))),
    ] {
        fake.enqueue(InboundMessage::new(
            TopicAddress::new("test.topic").expect("static SPI topic address must be valid"),
            EventId::new(id).expect("static test event ID must be valid"),
            std::time::SystemTime::UNIX_EPOCH,
            Headers::new(),
            None,
            payload,
            Some(SettlementToken::new(sub.id(), id.to_owned())),
            Default::default(),
        ));
    }
    enqueue(&fake, sub.id(), 4, "other");
    probe.reject_failures.store(1, Ordering::SeqCst);
    let gate = Arc::new(AtomicBool::new(false));
    let handler_gate = gate.clone();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let mut run = Box::pin(sub.run(move |delivery| {
        let gate = handler_gate.clone();
        observed
            .lock()
            .expect("handler call log mutex must not be poisoned")
            .push(*delivery.payload());
        async move {
            std::future::poll_fn(|_| {
                if gate.load(Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
            Ok(())
        }
    }));
    poll_pending(run.as_mut());
    assert_eq!(*calls.lock().expect("handler call log mutex must not be poisoned"), [1]);
    assert!(
        probe
            .attempts
            .lock()
            .expect("settlement attempt log mutex must not be poisoned")
            .is_empty(),
        "malformed same-key message cannot settle ahead of its running predecessor"
    );
    let pending = bus.delivery_metrics();
    assert_eq!((pending.running_handlers, pending.queued, pending.settling), (1, 3, 0));
    gate.store(true, Ordering::SeqCst);
    poll_pending(run.as_mut());
    assert_eq!(
        *calls.lock().expect("handler call log mutex must not be poisoned"),
        [1, 4],
        "another key runs while Reject retains its lane during backoff"
    );
    let backing_off = bus.delivery_metrics();
    assert_eq!(
        (backing_off.running_handlers, backing_off.queued, backing_off.settling),
        (0, 1, 1)
    );
    drop(run);
    let paused = sub.delivery_metrics().metrics;
    assert_eq!(paused.queued + paused.running_handlers + paused.settling, 2);
    assert_eq!(paused.reserved_receives, 0);
    clock
        .advance(Duration::from_millis(10))
        .expect("manual retry clock advance must remain in its clock domain");
    let resumed_calls = calls.clone();
    let mut resumed = Box::pin(sub.run(move |delivery| {
        resumed_calls
            .lock()
            .expect("handler call log mutex must not be poisoned")
            .push(*delivery.payload());
        async { Ok(()) }
    }));
    poll_pending(resumed.as_mut());
    assert_eq!(
        *calls.lock().expect("handler call log mutex must not be poisoned"),
        [1, 4, 3],
        "resume neither reconstructs the first handler nor invokes a malformed handler"
    );
    let attempts = probe
        .attempts
        .lock()
        .expect("settlement attempt log mutex must not be poisoned");
    assert_eq!(
        attempts.iter().map(|(_, disposition)| *disposition).collect::<Vec<_>>(),
        [
            DeliveryDisposition::Accept,
            DeliveryDisposition::Reject,
            DeliveryDisposition::Accept,
            DeliveryDisposition::Reject,
            DeliveryDisposition::Accept
        ]
    );
    let rejects = attempts
        .iter()
        .filter(|(_, disposition)| *disposition == DeliveryDisposition::Reject)
        .collect::<Vec<_>>();
    assert_eq!(
        rejects[0], rejects[1],
        "retry preserves the token address and immutable Reject action"
    );
}

/// A tokenless successful handler completes, while an un-settleable rejection
/// is abandoned.
#[test]
fn test_tokenless_metrics_distinguish_success_from_unresolved_rejection() {
    /// Codec double that rejects every payload during decoding.
    struct RejectCodec(ContentType);
    impl EventCodec<u32> for RejectCodec {
        fn content_type(&self) -> &ContentType {
            &self.0
        }
        fn schema_id(&self) -> Option<&SchemaId> {
            None
        }
        fn encode(&self, _: &u32) -> Result<Arc<[u8]>, CodecError> {
            Ok(Arc::from([1u8]))
        }
        fn decode(&self, _: &EncodedPayload) -> Result<u32, CodecError> {
            Err(CodecError::Decode {
                source: Box::new(std::io::Error::other("malformed payload")),
            })
        }
    }

    let spi = Arc::new(FakeAsyncEventBusSpi::with_capabilities(
        native_no_settlement_capabilities(),
    ));
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("fake").expect("static test provider ID must be valid"),
        spi.clone(),
    )
    .expect("test provider capabilities must be valid");
    let diagnostics = Arc::new(Mutex::new(Vec::new()));
    let observed = diagnostics.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        observed
            .lock()
            .expect("diagnostic log mutex must not be poisoned")
            .push(diagnostic.clone())
    });
    let successful_topic = Topic::<u32>::new("successful").expect("static test topic must be valid");
    let mut successful = block_on(bus.subscribe(
        SubscribeRequest::new("successful", successful_topic).expect("test subscription request must be valid"),
    ))
    .expect("test subscription must be accepted");
    spi.enqueue(InboundMessage::new(
        TopicAddress::new("successful").expect("static SPI topic address must be valid"),
        EventId::new("success").expect("static test event ID must be valid"),
        std::time::SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        TransportPayload::Native(Arc::new(1_u32)),
        None,
        Default::default(),
    ));
    let mut success_run = Box::pin(successful.run(|_| async { Ok(()) }));
    poll_pending(success_run.as_mut());
    drop(success_run);
    let success_metrics = successful.delivery_metrics().metrics;
    assert_eq!((success_metrics.completed, success_metrics.abandoned_ephemeral), (1, 0));

    let reject_spi = Arc::new(FakeAsyncEventBusSpi::with_capabilities(
        native_no_settlement_capabilities(),
    ));
    let reject_bus = AsyncEventBus::from_spi(
        ProviderId::new("fake").expect("static test provider ID must be valid"),
        reject_spi.clone(),
    )
    .expect("test provider capabilities must be valid");
    let reject_observed = diagnostics.clone();
    let _reject_observer = reject_bus.observe_diagnostics(move |diagnostic| {
        reject_observed
            .lock()
            .expect("diagnostic log mutex must not be poisoned")
            .push(diagnostic.clone())
    });
    let rejected_topic = Topic::new("rejected")
        .expect("static test topic must be valid")
        .with_codec(RejectCodec(
            ContentType::new("application/test").expect("static codec content type must be valid"),
        ));
    let mut rejected = block_on(reject_bus.subscribe(
        SubscribeRequest::new("rejected", rejected_topic).expect("test subscription request must be valid"),
    ))
    .expect("test subscription must be accepted");
    reject_spi.enqueue(InboundMessage::new(
        TopicAddress::new("rejected").expect("static SPI topic address must be valid"),
        EventId::new("malformed").expect("static test event ID must be valid"),
        std::time::SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        TransportPayload::Encoded(EncodedPayload::new(
            Arc::from([0xff_u8]),
            ContentType::new("application/test").expect("static codec content type must be valid"),
            None,
        )),
        None,
        Default::default(),
    ));
    let mut reject_run =
        Box::pin(rejected.run(|_| async { panic!("decode-rejected message cannot reach the handler") }));
    poll_pending(reject_run.as_mut());
    drop(reject_run);
    let reject_metrics = rejected.delivery_metrics().metrics;
    assert_eq!(
        (reject_metrics.completed, reject_metrics.abandoned_ephemeral),
        (0, 1),
        "unresolved tokenless Reject is not a successful completion; snapshot={reject_metrics:?}"
    );
    assert_eq!(
        reject_metrics.settlement_attempts, 0,
        "missing tokens never enter provider settlement"
    );
    assert_eq!(
        reject_metrics.settlement_terminal_failures, 0,
        "unavailable settlement is not a terminal retry-policy failure"
    );
    assert_eq!(
        reject_spi.settlement_count(),
        0,
        "tokenless messages never call SPI settlement"
    );
    assert!(diagnostics.lock().expect("diagnostic log mutex must not be poisoned").iter().any(|diagnostic| matches!(diagnostic,
        Diagnostic::SettlementUnavailable { event_id, requested: DeliveryDisposition::Reject, .. } if event_id.as_str() == "malformed"
    )), "unavailable tokenless Reject must be observable");
}
