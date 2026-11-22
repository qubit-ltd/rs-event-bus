// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shutdown reentrancy and coordinator diagnostic panic boundaries.

use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;
use std::panic::panic_any;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::ShutdownError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;

mod support;

const LIMIT: Duration = Duration::from_secs(3);

#[derive(Clone)]
struct Reentry(Arc<Mutex<Option<EventBus>>>, mpsc::Sender<bool>, ShutdownMode);

impl Drop for Reentry {
    fn drop(&mut self) {
        let bus = self.0.lock().expect("bus slot").as_ref().expect("bus set").clone();
        let guarded = matches!(
            bus.shutdown(self.2),
            Err(ShutdownError::Lifecycle(
                qubit_event_bus::LifecycleError::WouldDeadlock { .. }
            ))
        );
        let _ = self.1.send(guarded);
    }
}

struct ProbeSpi {
    receiver_probe: Mutex<Option<Reentry>>,
    receiver_close_count: Arc<AtomicUsize>,
    panic_shutdown: bool,
    shutdown_error_once: bool,
    receiver_close_error: bool,
    payload_drop_count: Arc<AtomicUsize>,
    shutdown_count: Arc<AtomicUsize>,
}

impl EventBusSpi for ProbeSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        unreachable!("regression provider does not publish")
    }

    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Ok(Box::new(ProbeReceiver(
            self.receiver_probe.lock().expect("probe").take(),
            self.receiver_close_count.clone(),
            self.receiver_close_error,
            self.payload_drop_count.clone(),
        )))
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        let call = self.shutdown_count.fetch_add(1, Ordering::SeqCst);
        if self.panic_shutdown || (self.shutdown_error_once && call == 0) {
            return Err(SpiError::Operation {
                provider_id: "shutdown-probe".into(),
                operation: "shutdown",
                resource: None,
                kind: if self.panic_shutdown {
                    "display_panicked"
                } else {
                    "shutdown_display_panicked"
                },
                retryable: Some(false),
                source: Box::new(PanickingDisplay(self.payload_drop_count.clone())),
            });
        }
        Ok(ShutdownOutcome::Complete)
    }
}

struct ProbeReceiver(Option<Reentry>, Arc<AtomicUsize>, bool, Arc<AtomicUsize>);

impl EventSubscriptionSpi for ProbeReceiver {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        std::thread::sleep(timeout.min(Duration::from_millis(5)));
        Ok(ReceiveOutcome::TimedOut)
    }

    fn settle(&mut self, _: &SettlementToken, _: DeliveryDisposition) -> Result<(), SpiError> {
        unreachable!("regression provider has no deliveries")
    }

    fn close(&mut self) -> Result<(), SpiError> {
        self.1.fetch_add(1, Ordering::SeqCst);
        if self.2 {
            return Err(display_panic_error(
                "close_subscription",
                "receiver_close_display_panicked",
                self.3.clone(),
            ));
        }
        Ok(())
    }
}

impl Drop for ProbeReceiver {
    fn drop(&mut self) {
        drop(self.0.take());
    }
}

struct PanickingDrop(Arc<AtomicUsize>);

impl Drop for PanickingDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("injected panic payload drop panic");
    }
}

struct PanickingDisplay(Arc<AtomicUsize>);

impl Display for PanickingDisplay {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        panic_any(PanickingDrop(self.0.clone()));
    }
}

impl std::fmt::Debug for PanickingDisplay {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("PanickingDisplay")
    }
}

impl Error for PanickingDisplay {}

fn display_panic_error(operation: &'static str, kind: &'static str, payload_drop_count: Arc<AtomicUsize>) -> SpiError {
    SpiError::Operation {
        provider_id: "shutdown-probe".into(),
        operation,
        resource: None,
        kind,
        retryable: Some(false),
        source: Box::new(PanickingDisplay(payload_drop_count)),
    }
}

fn probe_bus(
    receiver_probe: Option<Reentry>,
    panic_shutdown: bool,
    receiver_close_count: Arc<AtomicUsize>,
    shutdown_error_once: bool,
    receiver_close_error: bool,
    payload_drop_count: Arc<AtomicUsize>,
    shutdown_count: Arc<AtomicUsize>,
) -> EventBus {
    EventBus::from_spi(
        ProviderId::new("shutdown-probe").expect("provider id"),
        Arc::new(ProbeSpi {
            receiver_probe: Mutex::new(receiver_probe),
            receiver_close_count,
            panic_shutdown,
            shutdown_error_once,
            receiver_close_error,
            payload_drop_count,
            shutdown_count,
        }),
    )
    .expect("bus construction")
}

fn reentry_probe(mode: ShutdownMode) -> (Reentry, mpsc::Receiver<bool>, Arc<Mutex<Option<EventBus>>>) {
    let (tx, rx) = mpsc::channel();
    let slot = Arc::new(Mutex::new(None));
    (Reentry(slot.clone(), tx, mode), rx, slot)
}

#[test]
fn test_handler_drop_shutdown_is_guarded() {
    if let Ok(case) = std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE") {
        assert_eq!(case, "handler-drop");
        let (probe, rx, slot) = reentry_probe(ShutdownMode::Immediate);
        let bus = EventBus::local(LocalEventBusConfig::default()).expect("local bus");
        *slot.lock().expect("bus slot") = Some(bus.clone());
        let _subscription = bus
            .subscribe(
                SubscribeRequest::new("drop-probe", Topic::<String>::new("drop.handler").expect("topic"))
                    .expect("subscribe request"),
                move |_| {
                    let _ = &probe;
                },
            )
            .expect("subscription");
        let ticket = bus.request_shutdown(ShutdownMode::Immediate).expect("request shutdown");
        assert_eq!(
            ticket.wait(Some(LIMIT)).expect("ticket completes").outcome,
            ShutdownOutcome::Complete
        );
        assert!(rx.try_recv().expect("drop completes before ticket completion"));
    } else {
        support::isolated_process::run_case("test_handler_drop_shutdown_is_guarded", "handler-drop");
    }
}

#[test]
fn test_receiver_drop_shutdown_is_guarded() {
    if let Ok(case) = std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE") {
        assert_eq!(case, "receiver-drop");
        let (probe, rx, slot) = reentry_probe(ShutdownMode::Immediate);
        let close_count = Arc::new(AtomicUsize::new(0));
        let bus = probe_bus(
            Some(probe),
            false,
            close_count.clone(),
            false,
            false,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
        );
        *slot.lock().expect("bus slot") = Some(bus.clone());
        let _subscription = bus
            .subscribe(
                SubscribeRequest::new("receiver-drop", Topic::<String>::new("drop.receiver").expect("topic"))
                    .expect("subscribe request"),
                |_| {},
            )
            .expect("subscription");
        let ticket = bus.request_shutdown(ShutdownMode::Immediate).expect("request shutdown");
        assert_eq!(
            ticket.wait(Some(LIMIT)).expect("ticket completes").outcome,
            ShutdownOutcome::Complete
        );
        assert!(rx.recv_timeout(LIMIT).expect("receiver dropped"));
        assert_eq!(close_count.load(Ordering::SeqCst), 1);
    } else {
        support::isolated_process::run_case("test_receiver_drop_shutdown_is_guarded", "receiver-drop");
    }
}

#[test]
fn test_drop_graceful_zero_timeout_is_guarded() {
    if let Ok(case) = std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE") {
        assert_eq!(case, "graceful-zero-drop");
        let (probe, rx, slot) = reentry_probe(ShutdownMode::Graceful {
            timeout: Duration::ZERO,
        });
        let bus = EventBus::local(LocalEventBusConfig::default()).expect("local bus");
        *slot.lock().expect("bus slot") = Some(bus.clone());
        let _subscription = bus
            .subscribe(
                SubscribeRequest::new("drop-probe", Topic::<String>::new("drop.graceful").expect("topic"))
                    .expect("subscribe request"),
                move |_| {
                    let _ = &probe;
                },
            )
            .expect("subscription");
        let ticket = bus.request_shutdown(ShutdownMode::Immediate).expect("request shutdown");
        let _ = ticket.wait(Some(LIMIT)).expect("ticket completes");
        assert!(rx.recv_timeout(LIMIT).expect("handler capture dropped"));
    } else {
        support::isolated_process::run_case("test_drop_graceful_zero_timeout_is_guarded", "graceful-zero-drop");
    }
}

#[test]
fn test_shutdown_error_display_panic_completes_ticket() {
    let bus = probe_bus(
        None,
        true,
        Arc::new(AtomicUsize::new(0)),
        false,
        false,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );
    let ticket = bus.request_shutdown(ShutdownMode::Immediate).expect("request shutdown");
    let error = ticket.wait(Some(LIMIT)).expect_err("provider error is returned");
    assert!(matches!(
        error,
        ShutdownError::Spi(SpiError::Operation {
            kind: "display_panicked",
            ..
        })
    ));
}

#[test]
fn test_shutdown_error_display_payload_drop_panic_completes_ticket() {
    let payload_drop_count = Arc::new(AtomicUsize::new(0));
    let shutdown_count = Arc::new(AtomicUsize::new(0));
    let bus = probe_bus(
        None,
        false,
        Arc::new(AtomicUsize::new(0)),
        true,
        false,
        payload_drop_count.clone(),
        shutdown_count.clone(),
    );
    let ticket = bus.request_shutdown(ShutdownMode::Immediate).expect("request shutdown");
    let error = ticket.wait(Some(LIMIT)).expect_err("provider error is returned");
    let provider_error = match &error {
        ShutdownError::Spi(error) => error,
        _ => panic!("expected provider error, got another structured variant"),
    };
    assert_eq!(provider_error.kind(), "shutdown_display_panicked");
    assert!(
        provider_error
            .source()
            .and_then(Error::source)
            .and_then(Error::source)
            .unwrap()
            .downcast_ref::<PanickingDisplay>()
            .is_some()
    );
    assert_eq!(payload_drop_count.load(Ordering::SeqCst), 1);
    let repeated = ticket
        .wait(Some(LIMIT))
        .expect_err("same generation retains provider error");
    assert!(matches!(
        repeated,
        ShutdownError::Spi(SpiError::Operation {
            kind: "shutdown_display_panicked",
            ..
        })
    ));
    let retry = bus.request_shutdown(ShutdownMode::Immediate).expect("retry shutdown");
    assert_eq!(
        retry.wait(Some(LIMIT)).expect("retry completes").outcome,
        ShutdownOutcome::Complete
    );
    assert_eq!(shutdown_count.load(Ordering::SeqCst), 2);
}

#[test]
fn test_sync_receiver_close_error_display_panic_is_retained() {
    let payload_drop_count = Arc::new(AtomicUsize::new(0));
    let shutdown_count = Arc::new(AtomicUsize::new(0));
    let close_count = Arc::new(AtomicUsize::new(0));
    let bus = probe_bus(
        None,
        false,
        close_count.clone(),
        false,
        true,
        payload_drop_count.clone(),
        shutdown_count.clone(),
    );
    let subscription = bus
        .subscribe(
            SubscribeRequest::new(
                "receiver-close-probe",
                Topic::<String>::new("close.receiver").expect("topic"),
            )
            .expect("subscribe request"),
            |_| {},
        )
        .expect("subscription");
    let expected_id = subscription.subscriber_id().as_str().to_owned();
    let cancel_error = match subscription.cancel().expect_err("receiver close failure retained") {
        qubit_event_bus::LifecycleError::SubscriptionClose(errors) => errors,
        _ => panic!("unexpected cancellation error variant"),
    };
    let cancel_failure = cancel_error.iter().next().expect("one cancellation failure");
    assert_eq!(cancel_failure.subscriber_id().as_str(), expected_id);
    assert!(matches!(
        cancel_failure.error(),
        SpiError::Operation {
            operation: "close_subscription",
            kind: "receiver_close_display_panicked",
            ..
        }
    ));
    assert!(cancel_failure.error().source().is_some());
    let ticket = bus.request_shutdown(ShutdownMode::Immediate).expect("request shutdown");
    let shutdown_error = ticket.wait(Some(LIMIT)).expect_err("close failure reported by bus");
    let shutdown_errors = match shutdown_error {
        ShutdownError::SubscriptionClose(errors) => errors,
        _ => panic!("unexpected shutdown error variant"),
    };
    let shutdown_failure = shutdown_errors.iter().next().expect("one shutdown failure");
    assert_eq!(shutdown_failure.subscriber_id(), cancel_failure.subscriber_id());
    assert_eq!(shutdown_failure.error().kind(), cancel_failure.error().kind());
    assert_eq!(shutdown_failure.error().operation(), cancel_failure.error().operation());
    assert!(std::ptr::eq(cancel_failure, shutdown_failure));
    assert!(
        shutdown_failure
            .error()
            .source()
            .unwrap()
            .downcast_ref::<PanickingDisplay>()
            .is_some()
    );
    assert!(
        cancel_failure
            .error()
            .source()
            .unwrap()
            .downcast_ref::<PanickingDisplay>()
            .is_some()
    );
    assert_eq!(close_count.load(Ordering::SeqCst), 1);
    assert_eq!(payload_drop_count.load(Ordering::SeqCst), 1);
    assert_eq!(shutdown_count.load(Ordering::SeqCst), 1);
    assert!(bus.shutdown_report().is_some());
}
