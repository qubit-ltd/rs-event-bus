// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public error and synchronous SPI model contracts.

mod support;

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use qubit_event_bus::error::LifecycleError;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::error::ReceiveError;
use qubit_event_bus::error::SettlementError;
use qubit_event_bus::error::ShutdownError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::error::SubscribeError;
use qubit_event_bus::facade::EventBus;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;

struct CloseFailureProvider;

impl EventBusSpi for CloseFailureProvider {
    fn capabilities(&self) -> EventBusCapabilities {
        support::fake_spi::full_capabilities()
    }

    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        })
    }

    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Ok(Box::new(CloseFailureSubscription {
            subscriber_id: request.subscriber_id().as_str().to_owned(),
        }))
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

struct CloseFailureSubscription {
    subscriber_id: String,
}

impl EventSubscriptionSpi for CloseFailureSubscription {
    fn receive(&mut self, _: Duration) -> Result<ReceiveOutcome, SpiError> {
        Ok(ReceiveOutcome::TimedOut)
    }

    fn settle(&mut self, _: &SettlementToken, _: DeliveryDisposition) -> Result<(), SpiError> {
        Ok(())
    }

    fn close(&mut self) -> Result<(), SpiError> {
        Err(SpiError::Operation {
            provider_id: "close-failure".into(),
            operation: "close",
            resource: Some(self.subscriber_id.clone().into()),
            kind: "close_failed",
            retryable: Some(false),
            source: Box::new(std::io::Error::other(format!("cannot close {}", self.subscriber_id))),
        })
    }
}

#[test]
fn test_close_errors_aggregate_failures_and_preserve_the_source_chain() {
    let bus = EventBus::new(
        ProviderId::new("close-failure").expect("valid provider ID"),
        Arc::new(CloseFailureProvider),
    );
    let topic = Topic::<String>::new("coverage.close").expect("valid topic");
    let first = bus
        .subscribe(
            SubscribeRequest::new("first", topic.clone()).expect("valid subscriber ID"),
            |_| (),
        )
        .expect("first subscription starts");
    let second = bus
        .subscribe(
            SubscribeRequest::new("second", topic).expect("valid subscriber ID"),
            |_| (),
        )
        .expect("second subscription starts");

    assert!(matches!(first.cancel(), Err(LifecycleError::SubscriptionClose(errors)) if errors.len() == 1));
    assert!(matches!(second.cancel(), Err(LifecycleError::SubscriptionClose(errors)) if errors.len() == 1));

    let error = bus
        .shutdown(ShutdownMode::Immediate)
        .expect_err("close failures are reported by shutdown");
    let ShutdownError::SubscriptionClose(errors) = error else {
        panic!("expected aggregate subscription-close error");
    };
    assert_eq!(errors.len(), 2);
    assert!(!errors.is_empty());
    let failures = errors.iter().collect::<Vec<_>>();
    assert_eq!(failures[0].subscriber_id().as_str(), "first");
    assert_eq!(failures[1].subscriber_id().as_str(), "second");
    assert_eq!(failures[0].error().kind(), "close_failed");
    assert_eq!(failures[1].error().resource(), Some("second"));
    assert!(errors.to_string().contains("2 subscription close failure(s)"));

    let first_source = errors.source().expect("aggregate exposes first close failure");
    assert_eq!(
        first_source.to_string(),
        "subscription first: provider close-failure failed close (close_failed): cannot close first"
    );
    let spi_source = first_source.source().expect("close failure exposes SPI error");
    assert!(spi_source.to_string().contains("provider close-failure failed close"));
    assert_eq!(
        spi_source.source().map(ToString::to_string).as_deref(),
        Some("cannot close first")
    );
}

#[test]
fn test_error_variants_expose_stable_lifecycle_contracts() {
    assert_eq!(
        PublishError::Closed.to_string(),
        "cannot publish after event bus shutdown"
    );
    assert_eq!(
        SubscribeError::Closed.to_string(),
        "cannot subscribe after event bus shutdown"
    );
    assert_eq!(
        ReceiveError::Closed.to_string(),
        "cannot receive after subscription close"
    );
    assert_eq!(
        SettlementError::AlreadySettled.to_string(),
        "delivery has already been settled"
    );

    let deadlock = LifecycleError::WouldDeadlock { operation: "shutdown" };
    assert!(deadlock.to_string().contains("shutdown would deadlock"));
    let timeout = ShutdownError::TimedOut {
        timeout: Duration::from_millis(25),
    };
    assert!(timeout.to_string().contains("25ms"));
}

#[test]
fn test_sync_spi_default_identity_and_trait_object_contract() {
    let provider = support::fake_spi::FakeEventBusSpi::new();
    let erased: Arc<dyn EventBusSpi> = Arc::new(provider);
    assert!(erased.provider_id().is_none(), "provider identity defaults to absent");
    assert_eq!(erased.capabilities().payload_modes(), PayloadModes::Native);

    fn accepts_object_safe_spi(_: &dyn EventBusSpi) {}
    accepts_object_safe_spi(erased.as_ref());
}
