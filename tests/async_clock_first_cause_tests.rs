// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! First-cause publication before reentrant diagnostics on clock failures.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;
use std::time::Duration;

use qubit_clock::ManualMonotonicClock;
use qubit_clock::MonotonicClock;
use qubit_clock::MonotonicInstant;
use qubit_clock::TimeError;
use qubit_clock::Timer;
use qubit_clock::TimerFuture;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::AsyncSubscription;
use qubit_event_bus::Diagnostic;
use qubit_event_bus::EventBusFacadeConfig;
use qubit_event_bus::ReceiveError;
use qubit_event_bus::SpiError;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::SettlementTermination;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriptionStopReason;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use support::fake_spi::FakeAsyncEventBusSpi;
use support::manual_async::block_on;
use support::manual_async::poll_once;

/// Deliberately violates the stable-domain contract to exercise error
/// publication.
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

/// Creates an isolated facade and controllable clocks, with no background
/// runtime.
fn setup() -> (
    AsyncEventBus,
    Arc<FakeAsyncEventBusSpi>,
    Arc<SwitchingTimer>,
) {
    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let timer = Arc::new(SwitchingTimer {
        normal: ManualMonotonicClock::new(),
        foreign: ManualMonotonicClock::new(),
        switched: AtomicBool::new(false),
    });
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("first-cause").expect("provider"),
        fake.clone(),
        EventBusFacadeConfig::default(),
        timer.clone(),
    )
    .expect("bus");
    (bus, fake, timer)
}

/// Registers one u32 subscription and supplies a single valid token-owned
/// message.
fn subscribe(bus: &AsyncEventBus, fake: &FakeAsyncEventBusSpi) -> AsyncSubscription<u32> {
    let request = SubscribeRequest::new(
        "first-cause",
        Topic::<u32>::new("test.topic").expect("topic"),
    )
    .expect("request");
    let sub = block_on(bus.subscribe(request)).expect("subscription");
    fake.enqueue(support::fake_spi::inbound_message(Some(
        SettlementToken::new(sub.id(), "token"),
    )));
    sub
}

/// The original handler timer source must be stored before a snapshot observer
/// runs.
#[test]
fn test_handler_clock_error_is_first_cause_before_observer_snapshot() {
    let (bus, fake, timer) = setup();
    let sub = subscribe(&bus, &fake);
    let callback_bus = bus.clone();
    let callbacks = Arc::new(AtomicUsize::new(0));
    let observed = callbacks.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::InternalFailure { origin, .. } if origin.as_ref() == "handler_clock") {
            observed.fetch_add(1, Ordering::SeqCst);
            let _ = callback_bus.delivery_metrics();
        }
    });
    let result = block_on(sub.run(move |_| {
        let timer = timer.clone();
        async move {
            timer.switched.store(true, Ordering::SeqCst);
            Ok(())
        }
    }));
    let Err(ReceiveError::Stopped(reason)) = result else {
        panic!("handler clock error must stop");
    };
    let SubscriptionStopReason::Provider { error } = reason.as_ref() else {
        panic!("original handler cause must remain");
    };
    assert_eq!(error.kind(), "handler_clock_failure");
    assert_eq!(error.operation(), "handler");
    assert!(
        std::error::Error::source(error.as_ref()).is_some_and(|source| source.is::<TimeError>())
    );
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(fake.settlement_count(), 0);
}

/// A settlement clock failure must retain its source and emit exactly one
/// Stopped.
#[test]
fn test_settlement_clock_error_is_first_cause_before_observer_snapshot() {
    let (bus, fake, timer) = setup();
    let sub = subscribe(&bus, &fake);
    fake.pause_next_settle();
    let mut run = Box::pin(sub.run(|_| async { Ok(()) }));
    for _ in 0..16 {
        assert!(matches!(poll_once(run.as_mut()), Poll::Pending));
    }
    drop(run);
    assert!(sub.terminal_failure().is_none());
    let callback_bus = bus.clone();
    let clock_callbacks = Arc::new(AtomicUsize::new(0));
    let observed = clock_callbacks.clone();
    let stopped = Arc::new(Mutex::new(Vec::new()));
    let captured = stopped.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::InternalFailure { origin, .. } if origin.as_ref() == "settlement_clock") {
            observed.fetch_add(1, Ordering::SeqCst);
            let _ = callback_bus.delivery_metrics();
        }
        if let Diagnostic::SettlementStopped { error, .. } = diagnostic {
            captured.lock().expect("stopped diagnostics").push(error.clone());
        }
    });
    timer.switched.store(true, Ordering::SeqCst);
    let result = block_on(sub.run(|_| async { panic!("completed handler must not be recreated") }));
    let Err(ReceiveError::Stopped(reason)) = result else {
        panic!("settlement clock error must stop");
    };
    let SubscriptionStopReason::Settlement {
        error,
        attempts,
        termination,
        ..
    } = reason.as_ref()
    else {
        panic!("original settlement cause must remain: {reason:?}");
    };
    assert_eq!(*attempts, 1);
    assert_eq!(*termination, SettlementTermination::InfrastructureFailure);
    assert_eq!(error.kind(), "settlement_clock_failure");
    assert_eq!(error.operation(), "settle");
    assert!(
        std::error::Error::source(error.as_ref()).is_some_and(|source| source.is::<TimeError>())
    );
    assert_eq!(clock_callbacks.load(Ordering::SeqCst), 1);
    let stopped = stopped.lock().expect("stopped diagnostics");
    assert_eq!(stopped.len(), 1);
    assert!(Arc::ptr_eq(error, &stopped[0]));
}

/// Permanent provider evidence is gated before a Failed observer can inspect
/// metrics.
#[test]
fn test_permanent_settlement_is_first_cause_before_failed_observer_snapshot() {
    let (bus, fake, timer) = setup();
    let sub = subscribe(&bus, &fake);
    fake.fail_all_settles();
    let callback_bus = bus.clone();
    let diagnostics = Arc::new(Mutex::new(Vec::new()));
    let observed = diagnostics.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::SettlementFailed { error, .. } = diagnostic {
            observed
                .lock()
                .expect("diagnostics")
                .push(("failed", error.clone()));
            timer.switched.store(true, Ordering::SeqCst);
            let _ = callback_bus.delivery_metrics();
        }
        if let Diagnostic::SettlementStopped { error, .. } = diagnostic {
            observed
                .lock()
                .expect("diagnostics")
                .push(("stopped", error.clone()));
        }
    });
    let result = block_on(sub.run(|_| async { Ok(()) }));
    let Err(ReceiveError::Stopped(reason)) = result else {
        panic!("permanent provider error must stop");
    };
    let SubscriptionStopReason::Settlement {
        error, termination, ..
    } = reason.as_ref()
    else {
        panic!("original permanent cause must remain: {reason:?}");
    };
    assert_eq!(*termination, SettlementTermination::PermanentError);
    let diagnostics = diagnostics.lock().expect("diagnostics");
    assert_eq!(
        diagnostics
            .iter()
            .map(|(kind, _)| *kind)
            .collect::<Vec<_>>(),
        ["failed", "stopped"]
    );
    assert!(
        diagnostics
            .iter()
            .all(|(_, source)| Arc::ptr_eq(error, source))
    );
}

/// Switches clock domains only after a real successful provider settlement
/// response.
struct SuccessClockBus {
    fake: Arc<FakeAsyncEventBusSpi>,
    timer: Arc<SwitchingTimer>,
}

impl AsyncEventBusSpi for SuccessClockBus {
    fn capabilities(&self) -> EventBusCapabilities {
        self.fake.capabilities()
    }
    fn publish<'a>(
        &'a self,
        message: OutboundMessage,
    ) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        self.fake.publish(message)
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move {
            let receiver = self.fake.subscribe(request).await?;
            Ok(Box::new(SuccessClockReceiver {
                receiver,
                timer: self.timer.clone(),
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }
    fn shutdown<'a>(
        &'a self,
        mode: ShutdownMode,
    ) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        self.fake.shutdown(mode)
    }
}

/// Preserves the real single receiver and flips only after its settlement
/// succeeds.
struct SuccessClockReceiver {
    receiver: Box<dyn AsyncEventSubscriptionSpi>,
    timer: Arc<SwitchingTimer>,
}

impl AsyncEventSubscriptionSpi for SuccessClockReceiver {
    fn receive<'a>(
        &'a mut self,
        timeout: Duration,
    ) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        self.receiver.receive(timeout)
    }
    fn settle<'a>(
        &'a mut self,
        token: &SettlementToken,
        disposition: DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        let timer = self.timer.clone();
        let settled = self.receiver.settle(token, disposition);
        Box::pin(async move {
            settled.await?;
            timer.switched.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        self.receiver.close()
    }
}

/// A timing failure cannot turn a confirmed SPI success into an abandoned
/// message.
#[test]
fn test_successful_spi_then_clock_failure_is_not_abandoned_or_retried() {
    let fake = Arc::new(FakeAsyncEventBusSpi::new());
    let timer = Arc::new(SwitchingTimer {
        normal: ManualMonotonicClock::new(),
        foreign: ManualMonotonicClock::new(),
        switched: AtomicBool::new(false),
    });
    let provider = Arc::new(SuccessClockBus {
        fake: fake.clone(),
        timer: timer.clone(),
    });
    let bus = AsyncEventBus::with_config_and_timer(
        ProviderId::new("success-clock").expect("provider"),
        provider,
        EventBusFacadeConfig::default(),
        timer,
    )
    .expect("bus");
    let sub = subscribe(&bus, &fake);
    let result = block_on(sub.run(|_| async { Ok(()) }));
    assert!(matches!(
        result,
        Err(ReceiveError::Stopped(ref reason))
            if matches!(
                reason.as_ref(),
                SubscriptionStopReason::Settlement {
                    termination: SettlementTermination::InfrastructureFailure,
                    ..
                }
            )
    ));
    let metrics = sub.delivery_metrics().metrics;
    assert_eq!(
        (
            metrics.settlement_terminal_failures,
            metrics.completed,
            metrics.abandoned_ephemeral
        ),
        (1, 0, 0)
    );
    assert_eq!(fake.settlement_count(), 1);
    assert!(matches!(
        block_on(sub.run(|_| async { panic!("terminal handler must not restart") })),
        Err(ReceiveError::Stopped(_))
    ));
    assert_eq!(
        fake.operation_log()
            .iter()
            .filter(|operation| **operation == "settle")
            .count(),
        1
    );
}
