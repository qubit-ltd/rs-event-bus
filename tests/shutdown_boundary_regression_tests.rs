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
        )))
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        if self.panic_shutdown {
            return Err(SpiError::Operation {
                provider_id: "shutdown-probe".into(),
                operation: "shutdown",
                resource: None,
                kind: "display_panicked",
                retryable: Some(false),
                source: Box::new(PanickingDisplay),
            });
        }
        Ok(ShutdownOutcome::Complete)
    }
}

struct ProbeReceiver(Option<Reentry>, Arc<AtomicUsize>);

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
        Ok(())
    }
}

impl Drop for ProbeReceiver {
    fn drop(&mut self) {
        drop(self.0.take());
    }
}

struct PanickingDisplay;

impl Display for PanickingDisplay {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        panic!("injected error display panic");
    }
}

impl std::fmt::Debug for PanickingDisplay {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("PanickingDisplay")
    }
}

impl Error for PanickingDisplay {}

fn probe_bus(
    receiver_probe: Option<Reentry>,
    panic_shutdown: bool,
    receiver_close_count: Arc<AtomicUsize>,
) -> EventBus {
    EventBus::from_spi(
        ProviderId::new("shutdown-probe").expect("provider id"),
        Arc::new(ProbeSpi {
            receiver_probe: Mutex::new(receiver_probe),
            receiver_close_count,
            panic_shutdown,
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
        let bus = probe_bus(Some(probe), false, close_count.clone());
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
    let bus = probe_bus(None, true, Arc::new(AtomicUsize::new(0)));
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
