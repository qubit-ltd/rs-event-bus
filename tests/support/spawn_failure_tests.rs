// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::error::ShutdownError;
use crate::error::SpiError;
use crate::error::SubscribeError;
use crate::facade::EventBus;
use crate::facade::EventBusFacadeConfig;
use crate::facade::IntoHandlerResult;
use crate::facade::SyncDeliverySchedulerConfig;
use crate::model::ProviderId;
use crate::model::PublishAcknowledgement;
use crate::model::SubscribeRequest;
use crate::model::SubscriberId;
use crate::model::Topic;
use crate::spi::DelayedDeliveryCapability;
use crate::spi::DeliveryDisposition;
use crate::spi::DurabilityCapability;
use crate::spi::EventBusCapabilities;
use crate::spi::EventBusSpi;
use crate::spi::EventSubscriptionSpi;
use crate::spi::OutboundMessage;
use crate::spi::PayloadModes;
use crate::spi::PublishGuarantee;
use crate::spi::PublishVisibility;
use crate::spi::ReceiveOutcome;
use crate::spi::ReplayCapability;
use crate::spi::SettlementCapabilities;
use crate::spi::SettlementToken;
use crate::spi::ShutdownMode;
use crate::spi::ShutdownOutcome;
use crate::spi::SpiSubscriptionRequest;

struct EmptySpi;

#[test]
fn test_unit_handler_result_converts_to_success() {
    assert!(().into_handler_result().is_ok());
}

impl EventBusSpi for EmptySpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            crate::spi::OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        unreachable!("spawn cleanup does not publish")
    }

    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        unreachable!("spawn cleanup uses a directly constructed receiver")
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

struct CloseCounter {
    calls: Arc<AtomicUsize>,
    fail: bool,
}

impl EventSubscriptionSpi for CloseCounter {
    fn receive(&mut self, _: Duration) -> Result<ReceiveOutcome, SpiError> {
        Ok(ReceiveOutcome::Closed)
    }

    fn settle(&mut self, _: &SettlementToken, _: DeliveryDisposition) -> Result<(), SpiError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), SpiError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        if self.fail {
            Err(SpiError::Operation {
                provider_id: "spawn-test".into(),
                operation: "close_subscription",
                resource: None,
                kind: "close_failed",
                retryable: None,
                source: Box::new(std::io::Error::other("synthetic close failure")),
            })
        } else {
            Ok(())
        }
    }
}

#[test]
fn failed_worker_spawn_closes_receiver_and_keeps_the_spawn_error_source() {
    let bus = EventBus::new(
        ProviderId::new("spawn-test").expect("valid provider ID"),
        Arc::new(EmptySpi),
    );
    let close_calls = Arc::new(AtomicUsize::new(0));
    let receiver: Box<dyn EventSubscriptionSpi> = Box::new(CloseCounter {
        calls: close_calls.clone(),
        fail: false,
    });
    let error = super::cleanup_failed_worker_spawn(
        &bus.inner,
        &SubscriberId::new("spawn-failure").expect("valid ID"),
        Some(receiver),
        std::io::Error::other("synthetic thread spawn failure"),
    );

    assert_eq!(close_calls.load(Ordering::Acquire), 1);
    let source = std::error::Error::source(&error).expect("spawn I/O error remains chained");
    assert_eq!(source.to_string(), "synthetic thread spawn failure");
}

struct SpawnFailureSpi {
    close_calls: Arc<AtomicUsize>,
}

impl EventBusSpi for SpawnFailureSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        EmptySpi.capabilities()
    }

    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        unreachable!("spawn failure test does not publish")
    }

    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Ok(Box::new(CloseCounter {
            calls: self.close_calls.clone(),
            fail: true,
        }))
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

#[test]
fn scheduler_spawn_failure_closes_provider_subscription_and_keeps_both_errors() {
    let close_calls = Arc::new(AtomicUsize::new(0));
    let config = EventBusFacadeConfig::new()
        .with_sync_delivery_scheduler(SyncDeliverySchedulerConfig::new(2, 1).expect("valid scheduler config"));
    let bus = EventBus::with_config(
        ProviderId::new("spawn-test").expect("valid provider ID"),
        Arc::new(SpawnFailureSpi {
            close_calls: close_calls.clone(),
        }),
        config,
    );
    bus.inner.scheduler.fail_spawn_at(1);

    let request = SubscribeRequest::new(
        SubscriberId::new("scheduler-spawn-failure").expect("valid subscriber ID"),
        Topic::<String>::new("spawn.test").expect("valid topic"),
    );
    let error = match bus.subscribe(request, |_| ()) {
        Ok(_) => panic!("scheduler worker spawn fails"),
        Err(error) => error,
    };
    assert!(matches!(error, SubscribeError::Spi(_)));
    assert_eq!(close_calls.load(Ordering::Acquire), 1);
    let source = std::error::Error::source(&error).expect("spawn source retained");
    assert_eq!(source.to_string(), "synthetic scheduler worker spawn failure");

    bus.inner.scheduler.fail_spawn_at(usize::MAX);
    let retry_request = SubscribeRequest::new(
        SubscriberId::new("scheduler-retry").expect("valid subscriber ID"),
        Topic::<String>::new("spawn.test").expect("valid topic"),
    );
    let _subscription = bus
        .subscribe(retry_request, |_| ())
        .expect("scheduler can restart after partial spawn failure");
    let shutdown_error = bus
        .shutdown(ShutdownMode::Immediate)
        .expect_err("cleanup close failure remains observable");
    assert!(matches!(shutdown_error, ShutdownError::SubscriptionClose(errors) if errors.len() == 2));
    assert_eq!(close_calls.load(Ordering::Acquire), 2);
}
