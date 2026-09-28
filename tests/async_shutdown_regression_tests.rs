// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for asynchronous shutdown mode escalation.

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
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
}

impl ShutdownGateSpi {
    fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            capabilities: EventBusCapabilities::new(
                PayloadModes::Native,
                SettlementCapabilities::AcceptRetryReject,
                OrderingCapability::None,
                DelayedDeliveryCapability::None,
                DurabilityCapability::Ephemeral,
                qubit_event_bus::spi::SubscriptionModes::EPHEMERAL,
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
                source: Box::new(std::io::Error::other("unused")),
            })
        })
    }

    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<SpiShutdownOutcome, SpiError>> {
        self.calls.lock().expect("calls lock").push(mode);
        Box::pin(async move {
            if matches!(mode, ShutdownMode::Graceful { .. }) {
                std::future::pending::<()>().await;
            }
            Ok(SpiShutdownOutcome::Complete)
        })
    }
}

#[test]
fn test_immediate_shutdown_escalates_active_graceful_provider_call() {
    let provider = Arc::new(ShutdownGateSpi::new());
    let bus = AsyncEventBus::from_spi(ProviderId::new("shutdown-gate").expect("provider ID"), provider.clone())
        .expect("facade");
    let counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    let mut graceful = Box::pin(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(30),
    }));
    assert!(graceful.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    assert_eq!(
        vec![ShutdownMode::Graceful {
            timeout: Duration::from_secs(30)
        }],
        *provider.calls.lock().expect("calls lock")
    );

    let mut immediate = Box::pin(bus.shutdown(ShutdownMode::Immediate));
    assert!(
        immediate
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(counter.0.load(Ordering::SeqCst) > 0);
    assert!(graceful.as_mut().poll(&mut Context::from_waker(&waker)).is_ready());
    assert_eq!(
        vec![
            ShutdownMode::Graceful {
                timeout: Duration::from_secs(30)
            },
            ShutdownMode::Immediate,
        ],
        *provider.calls.lock().expect("calls lock"),
    );
    assert!(
        immediate
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    );
}
