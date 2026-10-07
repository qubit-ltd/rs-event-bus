// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public construction, conversion, display, and source contracts for lifecycle
//! failures.

use std::error::Error;
use std::io::Error as IoError;
use std::sync::Arc;
use std::time::Duration;

use qubit_clock::TimeError;
use qubit_event_bus::LifecycleError;
use qubit_event_bus::SpiError;
use qubit_event_bus::facade::EventBus;
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

#[test]
fn test_lifecycle_error_timer_conversion_retains_time_error() {
    let error = LifecycleError::from(TimeError::InstantOverflow);

    assert_eq!(
        error.to_string(),
        "event bus timer failed: monotonic instant overflow"
    );
    assert!(matches!(
        &error,
        LifecycleError::Timer(TimeError::InstantOverflow)
    ));
    assert!(Error::source(&error).is_some_and(|source| source.is::<TimeError>()));
}

#[test]
fn test_lifecycle_error_would_deadlock_retains_operation() {
    let error = LifecycleError::WouldDeadlock {
        operation: "shutdown",
    };

    assert_eq!(
        error.to_string(),
        "shutdown would deadlock in this event bus execution context"
    );
    assert!(matches!(
        &error,
        LifecycleError::WouldDeadlock {
            operation: "shutdown"
        }
    ));
    assert!(Error::source(&error).is_none());
}

#[test]
fn test_lifecycle_error_closed_displays_stable_message() {
    let error = LifecycleError::Closed;

    assert_eq!(error.to_string(), "event bus is closed");
    assert!(matches!(&error, LifecycleError::Closed));
    assert!(Error::source(&error).is_none());
}

#[test]
fn test_lifecycle_error_idle_wait_unsupported_displays_stable_message() {
    let error = LifecycleError::IdleWaitUnsupported;

    assert_eq!(
        error.to_string(),
        "the event bus provider does not support waiting for topic idle"
    );
    assert!(matches!(&error, LifecycleError::IdleWaitUnsupported));
    assert!(Error::source(&error).is_none());
}

#[test]
fn test_lifecycle_error_spi_conversion_preserves_provider_source() {
    let spi_error = SpiError::Operation {
        provider_id: "memory".into(),
        operation: "wait_for_topic_idle",
        resource: Some("orders".into()),
        kind: "unavailable",
        retryable: Some(false),
        source: Box::new(IoError::other("idle probe failed")),
    };
    let error = LifecycleError::from(spi_error);

    assert_eq!(
        error.to_string(),
        "provider memory failed wait_for_topic_idle (unavailable): idle probe failed"
    );
    assert!(matches!(
        &error,
        LifecycleError::Spi(SpiError::Operation { provider_id, operation, .. })
            if provider_id.as_ref() == "memory" && *operation == "wait_for_topic_idle"
    ));
    assert_eq!(
        Error::source(&error).map(ToString::to_string).as_deref(),
        Some("idle probe failed")
    );
}

#[test]
fn test_lifecycle_error_subscription_close_preserves_public_failure_chain() {
    let bus = EventBus::from_spi(
        ProviderId::new("lifecycle-close-test").expect("provider ID should be valid"),
        Arc::new(CloseFailureProvider),
    )
    .expect("provider capabilities should be valid");
    let topic = Topic::<String>::new("lifecycle.close").expect("topic should be valid");
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("worker", topic).expect("subscriber ID should be valid"),
            |_| (),
        )
        .expect("subscription should start");

    let error = subscription
        .cancel()
        .expect_err("provider close failure should be reported");
    let LifecycleError::SubscriptionClose(errors) = &error else {
        panic!("subscription close failure should use the aggregate variant");
    };

    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors
            .iter()
            .next()
            .expect("one close failure")
            .subscriber_id()
            .as_str(),
        "worker"
    );
    assert!(
        errors
            .to_string()
            .contains("1 subscription close failure(s)")
    );
    let close_failure = Error::source(&error).expect("lifecycle error should expose close failure");
    assert_eq!(
        close_failure.to_string(),
        "subscription worker: provider lifecycle-close-test failed close_subscription (close_failed): close failed"
    );
    let spi_error = Error::source(close_failure).expect("close failure should expose SPI error");
    assert!(spi_error.is::<SpiError>());
    assert_eq!(
        Error::source(spi_error).map(ToString::to_string).as_deref(),
        Some("close failed")
    );
}

struct CloseFailureProvider;

impl EventBusSpi for CloseFailureProvider {
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
            PublishGuarantee::FireAndForget,
            PublishVisibility::Opaque,
        )
    }

    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        })
    }

    fn subscribe(
        &self,
        request: SpiSubscriptionRequest,
    ) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
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
            provider_id: "lifecycle-close-test".into(),
            operation: "close_subscription",
            resource: Some(self.subscriber_id.as_str().into()),
            kind: "close_failed",
            retryable: Some(false),
            source: Box::new(IoError::other("close failed")),
        })
    }
}
