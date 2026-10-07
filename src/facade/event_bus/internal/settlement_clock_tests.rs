// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private clock-wake contract: public clocks do not expose owner notification.

use std::error::Error;
use std::io::Error as IoError;
use std::num::NonZeroU32;
use std::num::NonZeroUsize;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_clock::ClockDomain;
use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_retry::RetryPolicy;

use crate::DeliveryError;
use crate::Diagnostic;
use crate::error::SpiError;
use crate::facade::DeliverySchedulingConfig;
use crate::facade::EventBus;
use crate::facade::EventBusFacadeConfig;
use crate::facade::SettlementRetryConfig;
use crate::model::Delivery;
use crate::model::ProviderId;
use crate::model::PublishAcknowledgement;
use crate::model::PublishRequest;
use crate::model::SettlementTermination;
use crate::model::SubscribeOptions;
use crate::model::SubscribeRequest;
use crate::model::SubscriptionStopReason;
use crate::model::Topic;
use crate::spi::DeliveryDisposition;
use crate::spi::EventBusCapabilities;
use crate::spi::EventBusSpi;
use crate::spi::EventSubscriptionSpi;
use crate::spi::InboundMessage;
use crate::spi::OutboundMessage;
use crate::spi::ReceiveOutcome;
use crate::spi::SettlementToken;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::SpiSubscriptionRequest;

/// Samples immutable instants before notifying the test, avoiding clock-advance
/// races.
struct SamplingClock {
    manual: Arc<ManualMonotonicClock>,
    sampled: mpsc::Sender<(usize, Duration)>,
    attempts: Arc<AtomicUsize>,
}
impl MonotonicClock for SamplingClock {
    fn domain(&self) -> ClockDomain {
        self.manual.domain()
    }
    fn now(&self) -> MonotonicInstant {
        let now = self.manual.now();
        let _ = self
            .sampled
            .send((self.attempts.load(Ordering::SeqCst), now.elapsed_since_origin()));
        now
    }
    fn new_timer(&self) -> Arc<dyn Timer> {
        self.manual.new_timer()
    }
}
/// SPI proxy that counts and fails each settlement attempt.
struct FailingSpi {
    inner: Arc<dyn EventBusSpi>,
    attempts: Arc<AtomicUsize>,
}
/// Receiver proxy that injects a retryable settlement error.
struct FailingReceiver {
    inner: Box<dyn EventSubscriptionSpi>,
    attempts: Arc<AtomicUsize>,
}
impl EventBusSpi for FailingSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.inner.publish(message)
    }
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Ok(Box::new(FailingReceiver {
            inner: self.inner.subscribe(request)?,
            attempts: self.attempts.clone(),
        }))
    }
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.inner.shutdown(mode)
    }
}
impl EventSubscriptionSpi for FailingReceiver {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        self.inner.receive(timeout)
    }
    fn settle(&mut self, _: &SettlementToken, _: DeliveryDisposition) -> Result<(), SpiError> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        Err(SpiError::Operation {
            provider_id: "clock-test".into(),
            operation: "settle",
            resource: None,
            kind: "transient",
            retryable: Some(true),
            source: Box::new(IoError::other("clock failure")),
        })
    }
    fn close(&mut self) -> Result<(), SpiError> {
        self.inner.close()
    }
}
/// Waits until the owner samples the requested logical instant after an
/// explicit wake.
fn sampled_at(receiver: &mpsc::Receiver<(usize, Duration)>, expected_attempts: usize, expected: Duration) {
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut last = None;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "expected clock sample ({expected_attempts}, {expected:?}); last={last:?}"
        );
        let sample = receiver.recv_timeout(remaining).unwrap_or_else(|error| {
            panic!("expected clock sample ({expected_attempts}, {expected:?}); last={last:?}; receive={error}")
        });
        if sample == (expected_attempts, expected) {
            break;
        }
        last = Some(sample);
    }
}
#[test]
fn test_sync_settlement_clock_wake_respects_backoff_and_total_deadline() {
    let local = EventBus::local(Default::default()).expect("local provider");
    let attempts = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(FailingSpi {
        inner: local.inner.spi.clone(),
        attempts: attempts.clone(),
    });
    let manual = Arc::new(ManualMonotonicClock::new());
    let (sampled_tx, sampled_rx) = mpsc::channel();
    let clock = Arc::new(SamplingClock {
        manual: manual.clone(),
        sampled: sampled_tx,
        attempts: attempts.clone(),
    });
    let config = EventBusFacadeConfig::new().with_settlement_retry(
        SettlementRetryConfig::new(
            NonZeroU32::new(3).expect("positive attempts"),
            Duration::from_millis(20),
            Duration::from_millis(10),
            Duration::from_millis(10),
        )
        .expect("valid budget"),
    );
    let bus = EventBus::with_config_and_clock(ProviderId::new("clock-test").expect("provider"), spi, config, clock)
        .expect("bus");
    let (failed_tx, failed_rx) = mpsc::channel();
    let (stopped_tx, stopped_rx) = mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| match diagnostic {
        Diagnostic::SettlementFailed { attempt, .. } => {
            let _ = failed_tx.send(*attempt);
        }
        Diagnostic::SettlementStopped { termination, .. } => {
            let _ = stopped_tx.send(*termination);
        }
        _ => {}
    });
    let topic = Topic::<u32>::new("clock.retry").expect("topic");
    let subscription = bus
        .subscribe(SubscribeRequest::new("clock", topic.clone()).expect("request"), |_| {})
        .expect("subscribe");
    let _ = bus
        .publish(PublishRequest::new(topic, 7).expect("publication"))
        .expect("publish");
    assert_eq!(
        failed_rx.recv_timeout(Duration::from_secs(2)).expect("first attempt"),
        1
    );
    sampled_at(&sampled_rx, 1, Duration::ZERO);
    manual.advance(Duration::from_millis(9)).expect("advance");
    bus.inner.scheduler.notify(subscription.id());
    sampled_at(&sampled_rx, 1, Duration::from_millis(9));
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "wake before due time cannot retry");
    manual.advance(Duration::from_millis(1)).expect("advance to retry");
    bus.inner.scheduler.notify(subscription.id());
    assert_eq!(
        failed_rx.recv_timeout(Duration::from_secs(2)).expect("second attempt"),
        2
    );
    // Observe the post-error sample before advancing the deadline.
    sampled_at(&sampled_rx, 2, Duration::from_millis(10));
    manual.advance(Duration::from_millis(10)).expect("advance to deadline");
    bus.inner.scheduler.notify(subscription.id());
    assert_eq!(
        stopped_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("deadline stops owner"),
        SettlementTermination::DeadlineExceeded
    );
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        2,
        "no provider call at the total deadline"
    );
    assert!(matches!(
        subscription.terminal_failure().as_deref(),
        Some(SubscriptionStopReason::Settlement {
            attempts: 2,
            termination: SettlementTermination::DeadlineExceeded,
            ..
        })
    ));
    subscription.cancel().expect("cleanup");
}

/// Changes domains only after the provider was called, exercising checked
/// settlement elapsed time.
struct BrokenClock {
    initial: ManualMonotonicClock,
    foreign: ManualMonotonicClock,
    attempts: Arc<AtomicUsize>,
}
impl MonotonicClock for BrokenClock {
    fn domain(&self) -> ClockDomain {
        self.initial.domain()
    }
    fn now(&self) -> MonotonicInstant {
        if self.attempts.load(Ordering::SeqCst) == 0 {
            self.initial.now()
        } else {
            self.foreign.now()
        }
    }
    fn new_timer(&self) -> Arc<dyn Timer> {
        self.initial.new_timer()
    }
}
#[test]
fn test_sync_settlement_clock_domain_failure_keeps_structured_source_and_attempt_context() {
    let local = EventBus::local(Default::default()).expect("local provider");
    let attempts = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(FailingSpi {
        inner: local.inner.spi.clone(),
        attempts: attempts.clone(),
    });
    let clock = Arc::new(BrokenClock {
        initial: ManualMonotonicClock::new(),
        foreign: ManualMonotonicClock::new(),
        attempts: attempts.clone(),
    });
    let bus = EventBus::with_config_and_clock(
        ProviderId::new("clock-test").expect("provider"),
        spi,
        EventBusFacadeConfig::new(),
        clock,
    )
    .expect("bus");
    let (stopped_tx, stopped_rx) = mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::SettlementStopped { error, .. } = diagnostic {
            let _ = stopped_tx.send(error.clone());
        }
    });
    let topic = Topic::<u32>::new("clock.domain").expect("topic");
    let subscription = bus
        .subscribe(SubscribeRequest::new("clock", topic.clone()).expect("request"), |_| {})
        .expect("subscribe");
    let _ = bus
        .publish(PublishRequest::new(topic, 7).expect("publication"))
        .expect("publish");
    let stopped_error = stopped_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("clock failure diagnosed");
    let reason = subscription.terminal_failure().expect("terminal reason");
    let SubscriptionStopReason::Settlement {
        attempts: count,
        termination,
        error,
        disposition,
        ..
    } = reason.as_ref()
    else {
        panic!("settlement context required")
    };
    assert_eq!(*count, 1);
    assert_eq!(*termination, SettlementTermination::InfrastructureFailure);
    assert_eq!(*disposition, DeliveryDisposition::Accept);
    assert!(Arc::ptr_eq(error, &stopped_error));
    assert!(
        Error::source(error.as_ref())
            .expect("structured time error")
            .downcast_ref::<TimeError>()
            .is_some()
    );
    subscription.cancel().expect("cleanup");
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

/// Closed handles retain counters without accumulating controls in the real bus
/// registry.
#[test]
fn test_sync_thousand_closed_handles_leave_active_registry_at_baseline() {
    let local = EventBus::local(Default::default()).expect("local provider");
    let one = NonZeroUsize::new(1).expect("positive limit");
    let config = EventBusFacadeConfig::new()
        .with_delivery_scheduling(DeliverySchedulingConfig::new(one, one, one, one).expect("one live subscription"));
    let bus = EventBus::with_config(
        ProviderId::new("registry-test").expect("provider"),
        local.inner.spi.clone(),
        config,
    )
    .expect("bus");
    let topic = Topic::<u32>::new("registry.churn").expect("topic");
    let mut closed = Vec::with_capacity(1000);
    for index in 0..1000 {
        let subscription = bus
            .subscribe(SubscribeRequest::new("churn", topic.clone()).expect("request"), |_| {})
            .expect("one slot is reusable");
        if index == 999 {
            let _ = bus
                .publish(PublishRequest::new(topic.clone(), 1).expect("publication"))
                .expect("publish");
            // Wait for received processing before closing this final handle.
            let deadline = Instant::now() + Duration::from_secs(2);
            while subscription.delivery_metrics().metrics.completed == 0 && Instant::now() < deadline {
                thread::yield_now();
            }
            assert_eq!(subscription.delivery_metrics().metrics.completed, 1);
        }
        subscription.cancel().expect("close and join");
        assert!(bus.inner.subscriptions.lock().expect("registry lock").is_empty());
        let gauges = bus.inner.scheduler.snapshot_gauges(None);
        assert_eq!(
            gauges.reserved_receives + gauges.queued + gauges.running_handlers + gauges.settling,
            0
        );
        closed.push(subscription);
    }
    assert_eq!(bus.delivery_metrics().completed, 1);
    assert_eq!(
        closed
            .last()
            .expect("last closed handle")
            .delivery_metrics()
            .metrics
            .completed,
        1
    );
    drop(bus);
    assert_eq!(
        closed
            .last()
            .expect("retained handle")
            .delivery_metrics()
            .metrics
            .completed,
        1
    );
}

/// Switches domains on command while a handler gate keeps one old-domain lease
/// alive.
struct SwitchingClock {
    initial: ManualMonotonicClock,
    foreign: ManualMonotonicClock,
    switched: AtomicBool,
}
impl MonotonicClock for SwitchingClock {
    fn domain(&self) -> ClockDomain {
        self.initial.domain()
    }
    fn now(&self) -> MonotonicInstant {
        if self.switched.load(Ordering::SeqCst) {
            self.foreign.now()
        } else {
            self.initial.now()
        }
    }
    fn new_timer(&self) -> Arc<dyn Timer> {
        self.initial.new_timer()
    }
}
#[test]
fn test_sync_metrics_clock_failure_observer_can_reenter_snapshot_once() {
    struct ReleaseGate(Arc<(Mutex<bool>, Condvar)>);
    impl Drop for ReleaseGate {
        fn drop(&mut self) {
            let (released, changed) = &*self.0;
            *released.lock().expect("gate") = true;
            changed.notify_all();
        }
    }
    let local = EventBus::local(Default::default()).expect("local provider");
    let clock = Arc::new(SwitchingClock {
        initial: ManualMonotonicClock::new(),
        foreign: ManualMonotonicClock::new(),
        switched: AtomicBool::new(false),
    });
    let one = NonZeroUsize::new(1).expect("positive limit");
    let config = EventBusFacadeConfig::new()
        .with_delivery_scheduling(DeliverySchedulingConfig::new(one, one, one, one).expect("one owned lease"));
    let bus = EventBus::with_config_and_clock(
        ProviderId::new("snapshot-test").expect("provider"),
        local.inner.spi.clone(),
        config,
        clock.clone(),
    )
    .expect("bus");
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let callback_gate = gate.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let topic = Topic::<u32>::new("snapshot.failure").expect("topic");
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("snapshot", topic.clone()).expect("request"),
            move |delivery: Delivery<u32>| {
                if *delivery.payload() == 1 {
                    let _ = entered_tx.send(());
                    let (released, changed) = &*callback_gate;
                    let mut released = released.lock().expect("gate");
                    while !*released {
                        released = changed.wait(released).expect("gate wait");
                    }
                }
            },
        )
        .expect("subscribe");
    let release = ReleaseGate(gate);
    let _ = bus
        .publish(PublishRequest::new(topic.clone(), 0).expect("first request"))
        .expect("first publish");
    let deadline = Instant::now() + Duration::from_secs(2);
    while bus.delivery_metrics().completed == 0 && Instant::now() < deadline {
        thread::yield_now();
    }
    assert_eq!(bus.delivery_metrics().completed, 1);
    let _ = bus
        .publish(PublishRequest::new(topic, 1).expect("second request"))
        .expect("second publish");
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("second handler retains its old-domain lease");
    let nested_bus = bus.clone();
    let callbacks = Arc::new(AtomicUsize::new(0));
    let callback_count = callbacks.clone();
    let (nested_tx, nested_rx) = mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::InternalFailure { origin, .. } if origin.as_ref() == "metrics_clock") {
            // The bound makes a broken reentry gate fail without stack overflow.
            if callback_count.fetch_add(1, Ordering::SeqCst) < 2 {
                let _ = nested_tx.send(nested_bus.delivery_metrics());
            }
        }
    });
    clock.switched.store(true, Ordering::SeqCst);
    let outer = bus.delivery_metrics();
    let nested = nested_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("observer reenters without locking or recursive diagnostics");
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    for snapshot in [outer, nested] {
        assert_eq!(snapshot.running_handlers, 1);
        assert_eq!(snapshot.completed, 1);
        assert_eq!(snapshot.oldest_owned_age, None);
    }
    let reason = subscription
        .terminal_failure()
        .expect("clock error is published before the observer");
    let SubscriptionStopReason::Provider { error } = reason.as_ref() else {
        panic!("snapshot clock provider boundary reason")
    };
    assert_eq!(error.operation(), "metrics_clock");
    assert!(
        Error::source(error.as_ref())
            .expect("clock source")
            .downcast_ref::<TimeError>()
            .is_some()
    );
    drop(release);
    subscription.cancel().expect("cleanup");
    assert!(Arc::ptr_eq(
        &reason,
        &subscription.terminal_failure().expect("first cause retained")
    ));
}

/// Returns real provider success before the test clock changes its domain.
struct SuccessfulSettlementSpi {
    inner: Arc<dyn EventBusSpi>,
    successes: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
}
/// Receiver proxy that counts accepted settlements and close operations.
struct SuccessfulSettlementReceiver {
    inner: Box<dyn EventSubscriptionSpi>,
    successes: Arc<AtomicUsize>,
    closes: Arc<AtomicUsize>,
}
impl EventBusSpi for SuccessfulSettlementSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.inner.publish(message)
    }
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Ok(Box::new(SuccessfulSettlementReceiver {
            inner: self.inner.subscribe(request)?,
            successes: self.successes.clone(),
            closes: self.closes.clone(),
        }))
    }
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.inner.shutdown(mode)
    }
}
impl EventSubscriptionSpi for SuccessfulSettlementReceiver {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        self.inner.receive(timeout)
    }
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        self.inner.settle(token, disposition)?;
        self.successes.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn close(&mut self) -> Result<(), SpiError> {
        self.closes.fetch_add(1, Ordering::SeqCst);
        self.inner.close()
    }
}
#[test]
fn test_sync_successful_settlement_clock_failure_is_terminal_without_completed_or_abandoned() {
    let local = EventBus::local(Default::default()).expect("local provider");
    let successes = Arc::new(AtomicUsize::new(0));
    let spi = Arc::new(SuccessfulSettlementSpi {
        inner: local.inner.spi.clone(),
        successes: successes.clone(),
        closes: Arc::new(AtomicUsize::new(0)),
    });
    let clock = Arc::new(BrokenClock {
        initial: ManualMonotonicClock::new(),
        foreign: ManualMonotonicClock::new(),
        attempts: successes.clone(),
    });
    let bus = EventBus::with_config_and_clock(
        ProviderId::new("success-clock").expect("provider"),
        spi,
        EventBusFacadeConfig::new(),
        clock,
    )
    .expect("bus");
    let (stopped_tx, stopped_rx) = mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::SettlementStopped { error, .. } = diagnostic {
            let _ = stopped_tx.send(error.clone());
        }
    });
    let topic = Topic::<u32>::new("clock.success").expect("topic");
    let subscription = bus
        .subscribe(SubscribeRequest::new("clock", topic.clone()).expect("request"), |_| {})
        .expect("subscribe");
    let _ = bus
        .publish(PublishRequest::new(topic, 7).expect("request"))
        .expect("publish");
    let stopped_error = stopped_rx.recv_timeout(Duration::from_secs(2)).expect("clock stop");
    subscription.cancel().expect("close and join");
    assert_eq!(
        successes.load(Ordering::SeqCst),
        1,
        "the real provider accepted exactly one settlement"
    );
    let reason = subscription.terminal_failure().expect("terminal reason");
    let SubscriptionStopReason::Settlement {
        attempts,
        termination,
        error,
        ..
    } = reason.as_ref()
    else {
        panic!("settlement context")
    };
    assert_eq!(*attempts, 1);
    assert_eq!(*termination, SettlementTermination::InfrastructureFailure);
    assert!(Arc::ptr_eq(error, &stopped_error));
    assert!(
        Error::source(error.as_ref())
            .expect("clock source")
            .downcast_ref::<TimeError>()
            .is_some()
    );
    let metrics = subscription.delivery_metrics().metrics;
    assert_eq!(metrics.settlement_attempts, 1);
    assert_eq!(metrics.settlement_terminal_failures, 1);
    assert_eq!(
        metrics.completed, 0,
        "fail-stop delivery is not a successfully completed lifecycle"
    );
    assert_eq!(
        metrics.abandoned_ephemeral, 0,
        "accepted SPI settlement is not unresolved provider recovery"
    );
    assert_eq!(metrics.settlement_duration_count, 0);
    assert_eq!(bus.delivery_metrics(), metrics);
}

/// Panics exactly once at the first claimed receive reservation.
struct PanicOnceClock {
    manual: ManualMonotonicClock,
    calls: AtomicUsize,
}
impl MonotonicClock for PanicOnceClock {
    fn domain(&self) -> ClockDomain {
        self.manual.domain()
    }
    fn now(&self) -> MonotonicInstant {
        assert_ne!(
            self.calls.fetch_add(1, Ordering::SeqCst),
            0,
            "injected first owner clock panic"
        );
        self.manual.now()
    }
    fn new_timer(&self) -> Arc<dyn Timer> {
        self.manual.new_timer()
    }
}

/// Panics once after a receiver has delivered a message to the handler pool.
struct PanicAfterReceiveClock {
    manual: ManualMonotonicClock,
    armed: Arc<AtomicBool>,
    panic_seen: Arc<AtomicBool>,
}
impl MonotonicClock for PanicAfterReceiveClock {
    fn domain(&self) -> ClockDomain {
        self.manual.domain()
    }
    fn now(&self) -> MonotonicInstant {
        let is_handler = thread::current()
            .name()
            .is_some_and(|name| name.starts_with("event-bus-handler-"));
        if is_handler && self.armed.swap(false, Ordering::SeqCst) {
            self.panic_seen.store(true, Ordering::SeqCst);
            panic!("injected handler-start clock panic");
        }
        self.manual.now()
    }
    fn new_timer(&self) -> Arc<dyn Timer> {
        self.manual.new_timer()
    }
}

type ReceiveGate = Arc<(Mutex<bool>, Condvar)>;
/// SPI proxy that removes a received message's settlement token.
struct TokenlessAfterReceiveSpi {
    inner: Arc<dyn EventBusSpi>,
    armed: Arc<AtomicBool>,
    settles: Arc<AtomicUsize>,
    message_seen: Arc<AtomicBool>,
    gate: ReceiveGate,
    gate_entered: mpsc::Sender<()>,
}
/// Receiver proxy that strips tokens and gates the next receive call.
struct TokenlessAfterReceiveSubscription {
    inner: Box<dyn EventSubscriptionSpi>,
    armed: Arc<AtomicBool>,
    settles: Arc<AtomicUsize>,
    message_seen: Arc<AtomicBool>,
    gate: ReceiveGate,
    gate_entered: mpsc::Sender<()>,
}
impl EventBusSpi for TokenlessAfterReceiveSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.inner.publish(message)
    }
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Ok(Box::new(TokenlessAfterReceiveSubscription {
            inner: self.inner.subscribe(request)?,
            armed: self.armed.clone(),
            settles: self.settles.clone(),
            message_seen: self.message_seen.clone(),
            gate: self.gate.clone(),
            gate_entered: self.gate_entered.clone(),
        }))
    }
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.inner.shutdown(mode)
    }
}
impl EventSubscriptionSpi for TokenlessAfterReceiveSubscription {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        if self.message_seen.swap(false, Ordering::SeqCst) {
            let (released, ready) = &*self.gate;
            self.gate_entered.send(()).expect("test owns receive gate");
            let mut released = released.lock().expect("receive gate");
            while !*released {
                released = ready.wait(released).expect("receive gate wait");
            }
        }
        match self.inner.receive(timeout)? {
            ReceiveOutcome::Message(message) => {
                let (address, event_id, timestamp, headers, ordering_key, payload, _, metadata) = message.into_parts();
                self.armed.store(true, Ordering::SeqCst);
                self.message_seen.store(true, Ordering::SeqCst);
                Ok(ReceiveOutcome::Message(InboundMessage::new(
                    address,
                    event_id,
                    timestamp,
                    headers,
                    ordering_key,
                    payload,
                    None,
                    metadata,
                )))
            }
            outcome => Ok(outcome),
        }
    }
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        self.settles.fetch_add(1, Ordering::SeqCst);
        self.inner.settle(token, disposition)
    }
    fn close(&mut self) -> Result<(), SpiError> {
        self.inner.close()
    }
}

#[test]
fn test_sync_tokenless_handler_start_clock_panic_counts_abandoned_not_completed() {
    let local = EventBus::local(Default::default()).expect("local provider");
    let armed = Arc::new(AtomicBool::new(false));
    let settles = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let (gate_entered_tx, gate_entered_rx) = mpsc::channel();
    let spi = Arc::new(TokenlessAfterReceiveSpi {
        inner: local.inner.spi.clone(),
        armed: armed.clone(),
        settles: settles.clone(),
        message_seen: Arc::new(AtomicBool::new(false)),
        gate: gate.clone(),
        gate_entered: gate_entered_tx,
    });
    let panic_seen = Arc::new(AtomicBool::new(false));
    let clock = Arc::new(PanicAfterReceiveClock {
        manual: ManualMonotonicClock::new(),
        armed,
        panic_seen: panic_seen.clone(),
    });
    let bus = EventBus::with_config_and_clock(
        ProviderId::new("tokenless-panic-clock").expect("provider"),
        spi,
        EventBusFacadeConfig::new(),
        clock,
    )
    .expect("bus");
    let topic = Topic::<u32>::new("clock.tokenless-panic").expect("topic");
    let called = Arc::new(AtomicBool::new(false));
    let handler_called = called.clone();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("panic", topic.clone()).expect("request"),
            move |_| {
                handler_called.store(true, Ordering::SeqCst);
            },
        )
        .expect("subscribe");
    let _ = bus
        .publish(PublishRequest::new(topic, 7).expect("request"))
        .expect("publish");

    struct ReleaseReceiveGate(ReceiveGate);
    impl ReleaseReceiveGate {
        fn release(&self) {
            let (released, ready) = &*self.0;
            *released.lock().expect("receive gate") = true;
            ready.notify_all();
        }
    }
    impl Drop for ReleaseReceiveGate {
        fn drop(&mut self) {
            self.release();
        }
    }
    let _release_gate = ReleaseReceiveGate(gate);
    gate_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("owner blocks at the gated next receive");
    let panic_deadline = Instant::now() + Duration::from_secs(2);
    while !panic_seen.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < panic_deadline,
            "handler did not reach its injected clock sample"
        );
        thread::yield_now();
    }
    _release_gate.release();

    let deadline = Instant::now() + Duration::from_secs(2);
    let metrics = loop {
        let metrics = subscription.delivery_metrics().metrics;
        if metrics.completed > 0 || metrics.abandoned_ephemeral > 0 {
            break metrics;
        }
        assert!(
            Instant::now() < deadline,
            "tokenless delivery did not reach a terminal metric state"
        );
        thread::yield_now();
    };
    assert_eq!(
        metrics.completed, 0,
        "a handler-start panic cannot complete an unsettled tokenless delivery: {metrics:?}"
    );
    assert_eq!(
        metrics.abandoned_ephemeral, 1,
        "the unresolved ephemeral delivery is recorded as abandoned"
    );
    assert_eq!(
        metrics.settlement_attempts, 0,
        "a tokenless failure cannot create an SPI settlement attempt"
    );
    assert_eq!(
        metrics.settlement_duration_count, 0,
        "a tokenless failure has no settlement timing sample"
    );
    assert_eq!(
        metrics.handler_duration_count, 0,
        "the user handler was fenced before its measured call"
    );
    assert!(
        !called.load(Ordering::SeqCst),
        "handler admission follows the injected clock sample"
    );
    assert_eq!(
        settles.load(Ordering::SeqCst),
        0,
        "a missing token never becomes an SPI settlement intent"
    );
    let bus_metrics = bus.delivery_metrics();
    assert_eq!(bus_metrics.completed, 0);
    assert_eq!(bus_metrics.abandoned_ephemeral, 1);
    subscription.cancel().expect("owner lease and receiver cleanup");
    let final_metrics = subscription.delivery_metrics().metrics;
    assert_eq!(
        final_metrics.reserved_receives
            + final_metrics.queued
            + final_metrics.running_handlers
            + final_metrics.settling,
        0,
        "owner returns all scheduler credit"
    );
    assert_eq!(final_metrics.completed, 0);
    assert_eq!(
        final_metrics.abandoned_ephemeral, 1,
        "one delivery receives one terminal recovery outcome"
    );
    assert!(
        bus.inner
            .subscriptions
            .lock()
            .expect("subscription registry")
            .is_empty()
    );
    assert!(bus.inner.tracker.workers_are_idle());
    drop(bus);
    drop(local);
}

/// The external watchdog can fail without waiting for the broken owner's Drop.
#[test]
fn test_sync_claimed_receive_clock_panic_releases_lease_and_finishes_owner() {
    let (finished_tx, finished_rx) = mpsc::channel();
    thread::spawn(move || {
        let local = EventBus::local(Default::default()).expect("local provider");
        let clock = Arc::new(PanicOnceClock {
            manual: ManualMonotonicClock::new(),
            calls: AtomicUsize::new(0),
        });
        let closes = Arc::new(AtomicUsize::new(0));
        let spi = Arc::new(SuccessfulSettlementSpi {
            inner: local.inner.spi.clone(),
            successes: Arc::new(AtomicUsize::new(0)),
            closes: closes.clone(),
        });
        let bus = EventBus::with_config_and_clock(
            ProviderId::new("panic-clock").expect("provider"),
            spi,
            EventBusFacadeConfig::new(),
            clock,
        )
        .expect("bus");
        let topic = Topic::<u32>::new("clock.panic").expect("topic");
        let subscription = bus
            .subscribe(SubscribeRequest::new("panic", topic.clone()).expect("request"), |_| {})
            .expect("subscribe");
        // Wait for the failure before cancellation, ensuring the claimed lease path
        // ran.
        let deadline = Instant::now() + Duration::from_secs(2);
        while subscription.terminal_failure().is_none() && Instant::now() < deadline {
            thread::yield_now();
        }
        assert!(subscription.terminal_failure().is_some());
        subscription
            .cancel()
            .expect("panicked owner still completes cancellation");
        assert_eq!(
            closes.load(Ordering::SeqCst),
            1,
            "claimed-reservation unwind closes its receiver exactly once"
        );
        assert!(bus.inner.subscriptions.lock().expect("registry").is_empty());
        assert!(bus.inner.tracker.workers_are_idle());
        let metrics = bus.delivery_metrics();
        assert_eq!(
            metrics.reserved_receives + metrics.queued + metrics.running_handlers + metrics.settling,
            0
        );
        let (called_tx, called_rx) = mpsc::channel();
        let healthy = bus
            .subscribe(
                SubscribeRequest::new("healthy", topic.clone()).expect("request"),
                move |_| {
                    let _ = called_tx.send(());
                },
            )
            .expect("subsequent subscribe");
        let _ = bus
            .publish(PublishRequest::new(topic, 1).expect("request"))
            .expect("publish");
        called_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("healthy handler after owner panic");
        healthy.cancel().expect("healthy cleanup");
        drop(bus);
        drop(local);
        let _ = finished_tx.send(());
    });
    finished_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("claimed lease panic cleanup must finish within the watchdog");
}

/// Stops between pipeline admission and each actual handler call, including
/// retries.
#[test]
fn test_sync_actual_handler_admission_respects_stop_and_graceful_drain() {
    for mode in ["terminal", "cancel", "immediate", "graceful"] {
        for stage in ["filter", "middleware", "retry"] {
            let bus = EventBus::local(Default::default()).expect("local bus");
            let topic = Topic::<u32>::new("handler.admission").expect("topic");
            let (entered_tx, entered_rx) = mpsc::channel();
            let (release_tx, release_rx) = mpsc::channel();
            struct Release(Option<mpsc::Sender<()>>);
            impl Drop for Release {
                fn drop(&mut self) {
                    if let Some(sender) = self.0.take() {
                        let _ = sender.send(());
                    }
                }
            }
            let release = Release(Some(release_tx));
            let release_rx = Arc::new(Mutex::new(release_rx));
            let mut builder = SubscribeOptions::<u32>::builder();
            if stage == "filter" {
                builder = builder.filter(move |_| {
                    let _ = entered_tx.send(());
                    let _ = release_rx.lock().expect("gate").recv();
                    true
                });
            } else {
                let calls = AtomicUsize::new(0);
                builder = builder.interceptor(move |delivery, next| {
                    let attempt = calls.fetch_add(1, Ordering::SeqCst);
                    if stage == "middleware" || attempt == 1 {
                        let _ = entered_tx.send(());
                        let _ = release_rx.lock().expect("gate").recv();
                    }
                    next(delivery)
                });
            }
            if stage == "retry" {
                builder = builder.retry_policy(RetryPolicy::builder().max_attempts(2).build().expect("policy"));
            }
            let calls = Arc::new(AtomicUsize::new(0));
            let callback_calls = calls.clone();
            let subscription = Arc::new(
                bus.subscribe(
                    SubscribeRequest::new("admission", topic.clone())
                        .expect("request")
                        .with_options(builder.build()),
                    move |_| {
                        let call = callback_calls.fetch_add(1, Ordering::SeqCst);
                        if stage == "retry" && call == 0 {
                            Err(DeliveryError::Handler {
                                source: Box::new(IoError::other("retry first")),
                            })
                        } else {
                            Ok(())
                        }
                    },
                )
                .expect("subscription"),
            );
            let _ = bus
                .publish(PublishRequest::new(topic, 1).expect("request"))
                .expect("publish");
            entered_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("pipeline gate reached before actual handler admission");
            let control = bus
                .inner
                .subscriptions
                .lock()
                .expect("registry")
                .get(&subscription.id())
                .expect("control")
                .clone();
            let (stopped_tx, stopped_rx) = mpsc::channel();
            match mode {
                "terminal" => {
                    control.fail_receive(SubscriptionStopReason::Provider {
                        error: Arc::new(SpiError::Operation {
                            provider_id: "test".into(),
                            operation: "receive",
                            resource: None,
                            kind: "terminal",
                            retryable: Some(false),
                            source: Box::new(IoError::other("terminal")),
                        }),
                    });
                    bus.inner.scheduler.cancel_subscription(subscription.id());
                    let _ = stopped_tx.send(());
                }
                "cancel" => {
                    let subscription = subscription.clone();
                    thread::spawn(move || {
                        subscription.cancel().expect("cancel");
                        let _ = stopped_tx.send(());
                    });
                }
                _ => {
                    let bus = bus.clone();
                    thread::spawn(move || {
                        let _ = bus
                            .shutdown(if mode == "immediate" {
                                ShutdownMode::Immediate
                            } else {
                                ShutdownMode::Graceful {
                                    timeout: Duration::from_secs(3),
                                }
                            })
                            .expect("shutdown");
                        let _ = stopped_tx.send(());
                    });
                }
            }
            let deadline = Instant::now() + Duration::from_secs(2);
            while !control.is_cancelled() && Instant::now() < deadline {
                thread::yield_now();
            }
            assert!(
                control.is_cancelled(),
                "stop must be published before releasing pipeline gate"
            );
            drop(release);
            stopped_rx
                .recv_timeout(Duration::from_secs(4))
                .expect("stop completes after gate release");
            subscription.cancel().expect("join subscription");
            let expected = usize::from(stage == "retry") + usize::from(mode == "graceful");
            assert_eq!(calls.load(Ordering::SeqCst), expected, "mode={mode}, stage={stage}");
            assert_eq!(
                subscription.delivery_metrics().metrics.handler_duration_count,
                expected as u64
            );
        }
    }
}

/// Continuous wrong samples cannot reset the clock assertion's watchdog.
#[test]
fn test_sync_clock_sample_watchdog_bounds_nonmatching_stream() {
    let (sampled_tx, sampled_rx) = mpsc::channel();
    let stop = Arc::new(AtomicBool::new(false));
    let producer_stop = stop.clone();
    let producer = thread::spawn(move || {
        while !producer_stop.load(Ordering::Acquire) {
            if sampled_tx.send((0, Duration::ZERO)).is_err() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
    });
    let (done_tx, done_rx) = mpsc::channel();
    let waiter = thread::spawn(move || {
        let result = catch_unwind(|| sampled_at(&sampled_rx, 99, Duration::from_secs(99)));
        let _ = done_tx.send(result.is_err());
    });
    let result = done_rx.recv_timeout(Duration::from_secs(3));
    stop.store(true, Ordering::Release);
    producer.join().expect("producer exits");
    waiter.join().expect("waiter contains expected assertion");
    assert!(result.expect("wrong samples must not extend the overall two-second watchdog"));
}

/// Injects a receive failure after one real message was accepted by one owner.
struct ReceiveFailureSpi {
    inner: Arc<dyn EventBusSpi>,
}
/// Receiver proxy that injects a terminal error after its first message.
struct ReceiveFailureReceiver {
    inner: Box<dyn EventSubscriptionSpi>,
    fail_after_message: bool,
    received: bool,
}
impl EventBusSpi for ReceiveFailureSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.inner.capabilities()
    }
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.inner.publish(message)
    }
    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        let fail_after_message = request.subscriber_id().as_str() == "receive-failure";
        Ok(Box::new(ReceiveFailureReceiver {
            inner: self.inner.subscribe(request)?,
            fail_after_message,
            received: false,
        }))
    }
    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.inner.shutdown(mode)
    }
}
impl EventSubscriptionSpi for ReceiveFailureReceiver {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        if self.fail_after_message && self.received {
            return Err(SpiError::Operation {
                provider_id: "receive-failure".into(),
                operation: "receive",
                resource: None,
                kind: "terminal",
                retryable: Some(false),
                source: Box::new(IoError::other("receive failed after owned message")),
            });
        }
        let result = self.inner.receive(timeout)?;
        self.received |= matches!(result, ReceiveOutcome::Message(_));
        Ok(result)
    }
    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        self.inner.settle(token, disposition)
    }
    fn close(&mut self) -> Result<(), SpiError> {
        self.inner.close()
    }
}

/// A stopped owner loses its RR eligibility before a diagnostic observer
/// blocks.
#[test]
fn test_sync_receive_stop_fences_scheduler_before_observer_callback() {
    struct Release(Option<mpsc::Sender<()>>);
    impl Drop for Release {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }
    let local = EventBus::local(Default::default()).expect("local");
    let config = EventBusFacadeConfig::new().with_delivery_scheduling(
        DeliverySchedulingConfig::new(
            NonZeroUsize::new(1).expect("H"),
            NonZeroUsize::new(6).expect("D"),
            NonZeroUsize::new(6).expect("P"),
            NonZeroUsize::new(4).expect("S"),
        )
        .expect("config"),
    );
    let bus = EventBus::with_config(
        ProviderId::new("receive-failure").expect("provider"),
        Arc::new(ReceiveFailureSpi {
            inner: local.inner.spi.clone(),
        }),
        config,
    )
    .expect("bus");
    let blocker_topic = Topic::<u32>::new("fence.blocker").expect("topic");
    let bad_topic = Topic::<u32>::new("fence.bad").expect("topic");
    let healthy_topic = Topic::<u32>::new("fence.healthy").expect("topic");
    let (blocked_tx, blocked_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let blocker = bus
        .subscribe(
            SubscribeRequest::new("blocker", blocker_topic.clone()).expect("request"),
            move |_| {
                let _ = blocked_tx.send(());
                let _ = release_rx.lock().expect("gate").recv();
            },
        )
        .expect("blocker");
    let release_handler = Release(Some(release_tx));
    let _ = bus
        .publish(PublishRequest::new(blocker_topic, 1).expect("request"))
        .expect("publish");
    blocked_rx.recv_timeout(Duration::from_secs(2)).expect("H occupied");
    let bad = bus
        .subscribe(
            SubscribeRequest::new("receive-failure", bad_topic.clone()).expect("request"),
            |_| -> () {
                panic!("failed owner cannot start queued handler");
            },
        )
        .expect("bad");
    let (observed_tx, observed_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::InternalFailure { origin, .. } if origin.as_ref() == "receive") {
            let _ = observed_tx.send(());
            let _ = release_rx.lock().expect("observer gate").recv();
        }
    });
    let release_observer = Release(Some(release_tx));
    let _ = bus
        .publish(PublishRequest::new(bad_topic, 2).expect("request"))
        .expect("publish bad");
    observed_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("receive terminal observer blocked with queued work");
    let (healthy_tx, healthy_rx) = mpsc::channel();
    let healthy = bus
        .subscribe(
            SubscribeRequest::new("healthy", healthy_topic.clone()).expect("request"),
            move |_| {
                let _ = healthy_tx.send(());
            },
        )
        .expect("healthy");
    let _ = bus
        .publish(PublishRequest::new(healthy_topic, 3).expect("request"))
        .expect("publish healthy");
    let deadline = Instant::now() + Duration::from_secs(2);
    while healthy.delivery_metrics().metrics.queued == 0 && Instant::now() < deadline {
        thread::yield_now();
    }
    drop(release_handler);
    let healthy_result = healthy_rx.recv_timeout(Duration::from_secs(2));
    drop(release_observer);
    blocker.cancel().expect("cleanup");
    bad.cancel().expect("cleanup");
    healthy.cancel().expect("cleanup");
    healthy_result.expect("healthy owner must advance while terminal observer remains blocked");
}

/// Terminal attempt observers must see the first cause and admission fence
/// already published.
#[test]
fn test_sync_terminal_settlement_failed_observer_sees_published_stop() {
    let local = EventBus::local(Default::default()).expect("local");
    let attempts = Arc::new(AtomicUsize::new(0));
    let config = EventBusFacadeConfig::new().with_settlement_retry(
        SettlementRetryConfig::new(
            NonZeroU32::new(1).expect("one attempt"),
            Duration::from_secs(1),
            Duration::from_millis(1),
            Duration::from_millis(1),
        )
        .expect("budget"),
    );
    let bus = EventBus::with_config(
        ProviderId::new("terminal-observer").expect("provider"),
        Arc::new(FailingSpi {
            inner: local.inner.spi.clone(),
            attempts,
        }),
        config,
    )
    .expect("bus");
    let weak = Arc::downgrade(&bus.inner);
    let (observed_tx, observed_rx) = mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::SettlementFailed { subscription_id, .. } = diagnostic {
            let inner = weak.upgrade().expect("bus live");
            let control = inner
                .subscriptions
                .lock()
                .expect("registry")
                .get(subscription_id)
                .expect("control")
                .clone();
            let _ = observed_tx.send(control.is_cancelled() && control.terminal_failure().is_some());
        }
    });
    let topic = Topic::<u32>::new("terminal.observer").expect("topic");
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("terminal", topic.clone()).expect("request"),
            |_| {},
        )
        .expect("subscribe");
    let _ = bus
        .publish(PublishRequest::new(topic, 1).expect("request"))
        .expect("publish");
    let published = observed_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("failed callback observed");
    subscription.cancel().expect("cleanup");
    assert!(
        published,
        "terminal SettlementFailed callbacks cannot precede stop publication"
    );
}
