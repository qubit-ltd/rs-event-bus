// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for asynchronous shutdown mode escalation.

use std::future::Future;
use std::future::pending;
use std::io::Error;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::error::ShutdownError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome as SpiShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;

struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct ShutdownGateSpi {
    calls: Mutex<Vec<ShutdownMode>>,
    capabilities: EventBusCapabilities,
    fail_immediate: bool,
}

impl ShutdownGateSpi {
    fn new(fail_immediate: bool) -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            fail_immediate,
            capabilities: EventBusCapabilities::new(
                PayloadModes::Native,
                SettlementCapabilities::AcceptRetryReject,
                OrderingCapability::None,
                DelayedDeliveryCapability::None,
                DurabilityCapability::Ephemeral,
                SubscriptionModes::EPHEMERAL,
                false,
                ReplayCapability::None,
                PublishGuarantee::Accepted,
                PublishVisibility::Opaque,
            ),
        }
    }
}

impl AsyncEventBusSpi for ShutdownGateSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        self.capabilities
    }

    fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async { Ok(PublishAcknowledgement::DroppedByInterceptor) })
    }

    fn subscribe<'a>(
        &'a self,
        _: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async {
            Err(SpiError::Operation {
                provider_id: "shutdown-gate".into(),
                operation: "subscribe",
                resource: None,
                kind: "unsupported_test_operation",
                retryable: Some(false),
                source: Box::new(Error::other("unused")),
            })
        })
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<SpiShutdownOutcome, SpiError>> {
        self.calls.lock().expect("calls lock").push(mode);
        let fail_immediate = self.fail_immediate;
        Box::pin(async move {
            if matches!(mode, ShutdownMode::Graceful { .. }) {
                pending::<()>().await;
            }
            if fail_immediate && mode == ShutdownMode::Immediate {
                return Err(SpiError::Operation {
                    provider_id: "shutdown-gate".into(),
                    operation: "shutdown",
                    resource: None,
                    kind: "immediate_shutdown_failed",
                    retryable: Some(false),
                    source: Box::new(Error::other("injected immediate shutdown failure")),
                });
            }
            Ok(SpiShutdownOutcome::Complete)
        })
    }
}

#[test]
fn test_immediate_shutdown_escalates_active_graceful_provider_call() {
    let provider = Arc::new(ShutdownGateSpi::new(false));
    let bus = AsyncEventBus::from_spi(ProviderId::new("shutdown-gate").expect("provider ID"), provider.clone())
        .expect("facade");
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(30),
    }));
    assert!(
        graceful.as_mut().poll(&mut Context::from_waker(&waker)).is_pending(),
        "graceful provider shutdown should remain pending",
    );
    assert_eq!(
        vec![ShutdownMode::Graceful {
            timeout: Duration::from_secs(30)
        }],
        *provider.calls.lock().expect("calls lock"),
        "the provider should receive the initial graceful shutdown request",
    );

    let mut immediate = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(
        immediate
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "immediate shutdown should wait while escalating the active provider call",
    );
    assert!(
        counter.0.load(Ordering::SeqCst) > 0,
        "escalating shutdown should wake the graceful shutdown task",
    );
    let Poll::Ready(Ok(graceful_report)) = graceful.as_mut().poll(&mut Context::from_waker(&waker)) else {
        panic!("escalated graceful shutdown must return a successful report");
    };
    assert_eq!(graceful_report.outcome, SpiShutdownOutcome::Complete);
    assert_eq!(graceful_report.known_abandoned_deliveries, 0);
    assert_eq!(
        vec![
            ShutdownMode::Graceful {
                timeout: Duration::from_secs(30)
            },
            ShutdownMode::Immediate,
        ],
        *provider.calls.lock().expect("calls lock"),
        "the provider should receive graceful and immediate shutdown requests in order",
    );
    let Poll::Ready(Ok(immediate_report)) = immediate.as_mut().poll(&mut Context::from_waker(Waker::noop())) else {
        panic!("immediate shutdown must return a successful report");
    };
    assert_eq!(immediate_report.outcome, SpiShutdownOutcome::Complete);
    assert_eq!(immediate_report.known_abandoned_deliveries, 0);
}

#[test]
fn test_immediate_shutdown_provider_error_reaches_both_waiters() {
    let provider = Arc::new(ShutdownGateSpi::new(true));
    let bus = AsyncEventBus::from_spi(ProviderId::new("shutdown-gate").expect("provider ID"), provider.clone())
        .expect("facade");
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(30),
    }));
    assert!(graceful.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    let mut immediate = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(immediate.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());

    for result in [
        graceful.as_mut().poll(&mut Context::from_waker(&waker)),
        immediate.as_mut().poll(&mut Context::from_waker(&waker)),
    ] {
        let Poll::Ready(Err(ShutdownError::Spi(SpiError::Operation {
            operation: "shutdown",
            kind: "immediate_shutdown_failed",
            source,
            ..
        }))) = result
        else {
            panic!("both shutdown waiters must observe the provider failure");
        };
        assert_eq!(
            ErrorKind::Other,
            source.downcast_ref::<Error>().expect("source is IO error").kind()
        );
    }
    let calls = provider.calls.lock().expect("calls lock");
    assert_eq!(
        &[
            ShutdownMode::Graceful {
                timeout: Duration::from_secs(30)
            },
            ShutdownMode::Immediate,
        ],
        &calls[..2],
        "graceful shutdown is escalated before any immediate retry",
    );
    assert!(calls[2..].iter().all(|mode| *mode == ShutdownMode::Immediate));
}
