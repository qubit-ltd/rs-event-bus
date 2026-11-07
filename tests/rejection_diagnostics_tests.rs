// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression coverage for destination rejection diagnostics.

use std::future::pending;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::AdmissionSummary;
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::pipeline::Diagnostic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;
use qubit_id::Id;

mod support;

use support::manual_async::block_on;

#[derive(Clone, Debug, Eq, PartialEq)]
struct RejectionDiagnostic {
    event_id: EventId,
    topic: Box<str>,
    subscription_id: Id,
    subscriber_id: SubscriberId,
    reason: Box<str>,
}

impl From<&Diagnostic> for RejectionDiagnostic {
    fn from(diagnostic: &Diagnostic) -> Self {
        let Diagnostic::AdmissionRejected {
            event_id,
            topic,
            subscription_id,
            subscriber_id,
            reason,
        } = diagnostic
        else {
            panic!("publication should emit only admission rejection diagnostics")
        };
        Self {
            event_id: event_id.clone(),
            topic: topic.clone(),
            subscription_id: *subscription_id,
            subscriber_id: subscriber_id.clone(),
            reason: reason.clone(),
        }
    }
}

struct FixedAdmissionSpi {
    acknowledgement: PublishAcknowledgement,
}

impl EventBusSpi for FixedAdmissionSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        capabilities()
    }

    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        Ok(self.acknowledgement.clone())
    }

    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        unreachable!("rejection diagnostic test does not subscribe")
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

impl AsyncEventBusSpi for FixedAdmissionSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        capabilities()
    }

    fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move { Ok(self.acknowledgement.clone()) })
    }

    fn subscribe<'a>(
        &'a self,
        _: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(pending())
    }

    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

fn capabilities() -> EventBusCapabilities {
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
        PublishVisibility::DestinationAdmissions,
    )
}

fn acknowledgement() -> PublishAcknowledgement {
    PublishAcknowledgement::DestinationAdmissions(vec![
        DestinationAdmission::new(
            Id::new(10),
            SubscriberId::new("accepted").unwrap(),
            AdmissionStatus::Accepted,
        ),
        DestinationAdmission::new(
            Id::new(11),
            SubscriberId::new("filtered").unwrap(),
            AdmissionStatus::Filtered,
        ),
        DestinationAdmission::new(
            Id::new(12),
            SubscriberId::new("rejected-capacity").unwrap(),
            AdmissionStatus::Rejected("capacity".into()),
        ),
        DestinationAdmission::new(
            Id::new(13),
            SubscriberId::new("rejected-closed").unwrap(),
            AdmissionStatus::Rejected("closed".into()),
        ),
    ])
}

fn expected_rejections(event_id: &EventId) -> Vec<RejectionDiagnostic> {
    vec![
        RejectionDiagnostic {
            event_id: event_id.clone(),
            topic: "rejection-tests".into(),
            subscription_id: Id::new(12),
            subscriber_id: SubscriberId::new("rejected-capacity").unwrap(),
            reason: "capacity".into(),
        },
        RejectionDiagnostic {
            event_id: event_id.clone(),
            topic: "rejection-tests".into(),
            subscription_id: Id::new(13),
            subscriber_id: SubscriberId::new("rejected-closed").unwrap(),
            reason: "closed".into(),
        },
    ]
}

fn expected_outcome() -> AdmissionOutcome {
    AdmissionOutcome::PartiallyAccepted(AdmissionSummary {
        accepted: 1,
        filtered: 1,
        rejected: 2,
    })
}

fn event_bus() -> EventBus {
    EventBus::from_spi(
        ProviderId::new("rejection-tests").unwrap(),
        Arc::new(FixedAdmissionSpi {
            acknowledgement: acknowledgement(),
        }),
    )
    .unwrap()
}

fn async_event_bus() -> AsyncEventBus {
    AsyncEventBus::from_spi(
        ProviderId::new("rejection-tests").unwrap(),
        Arc::new(FixedAdmissionSpi {
            acknowledgement: acknowledgement(),
        }),
    )
    .unwrap()
}

fn publish_request() -> PublishRequest<u32> {
    PublishRequest::new(Topic::new("rejection-tests").unwrap(), 42).unwrap()
}

fn assert_metrics(bus: &EventBus) {
    let metrics = bus.publish_metrics();
    assert_eq!(metrics.attempts, 1);
    assert_eq!(metrics.accepted_destinations, 1);
    assert_eq!(metrics.filtered_destinations, 1);
    assert_eq!(metrics.rejected_destinations, 2);
}

fn assert_async_metrics(bus: &AsyncEventBus) {
    let metrics = bus.publish_metrics();
    assert_eq!(metrics.attempts, 1);
    assert_eq!(metrics.accepted_destinations, 1);
    assert_eq!(metrics.filtered_destinations, 1);
    assert_eq!(metrics.rejected_destinations, 2);
}

#[test]
fn test_sync_publish_without_observers_preserves_receipt_and_metrics() {
    let bus = event_bus();
    let receipt = bus.publish(publish_request()).unwrap();

    assert_eq!(acknowledgement(), *receipt.acknowledgement());
    assert_eq!(expected_outcome(), receipt.admission_outcome());
    assert_metrics(&bus);
}

#[test]
fn test_sync_publish_notifies_one_observer_for_each_rejection() {
    let bus = event_bus();
    let diagnostics = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&diagnostics);
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        observed.lock().unwrap().push(RejectionDiagnostic::from(diagnostic));
    });

    let receipt = bus.publish(publish_request()).unwrap();

    assert_eq!(
        expected_rejections(receipt.dispatched_event_id().unwrap()),
        *diagnostics.lock().unwrap()
    );
    assert_metrics(&bus);
}

#[test]
fn test_sync_publish_emits_ordered_rejections_and_contains_observer_panics() {
    let bus = event_bus();
    let panic_calls = Arc::new(AtomicUsize::new(0));
    let panic_calls_observer = Arc::clone(&panic_calls);
    let _panic_observer = bus.observe_diagnostics(move |_| {
        panic_calls_observer.fetch_add(1, Ordering::Relaxed);
        panic!("observer panic is isolated");
    });
    let diagnostics = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&diagnostics);
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        observed.lock().unwrap().push(RejectionDiagnostic::from(diagnostic));
    });

    let receipt = bus.publish(publish_request()).unwrap();
    let event_id = receipt.dispatched_event_id().unwrap();
    assert_eq!(acknowledgement(), *receipt.acknowledgement());
    assert_eq!(expected_outcome(), receipt.admission_outcome());
    assert_eq!(2, panic_calls.load(Ordering::Relaxed));
    assert_eq!(expected_rejections(event_id), *diagnostics.lock().unwrap());
    assert_metrics(&bus);
}

#[test]
fn test_async_publish_without_observers_preserves_receipt_and_metrics() {
    let bus = async_event_bus();
    let receipt = block_on(bus.publish(publish_request())).unwrap();

    assert_eq!(acknowledgement(), *receipt.acknowledgement());
    assert_eq!(expected_outcome(), receipt.admission_outcome());
    assert_async_metrics(&bus);
}

#[test]
fn test_async_publish_notifies_one_observer_for_each_rejection() {
    let bus = async_event_bus();
    let diagnostics = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&diagnostics);
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        observed.lock().unwrap().push(RejectionDiagnostic::from(diagnostic));
    });

    let receipt = block_on(bus.publish(publish_request())).unwrap();

    assert_eq!(
        expected_rejections(receipt.dispatched_event_id().unwrap()),
        *diagnostics.lock().unwrap()
    );
    assert_async_metrics(&bus);
}

#[test]
fn test_async_publish_emits_ordered_rejections_and_contains_observer_panics() {
    let bus = async_event_bus();
    let panic_calls = Arc::new(AtomicUsize::new(0));
    let panic_calls_observer = Arc::clone(&panic_calls);
    let _panic_observer = bus.observe_diagnostics(move |_| {
        panic_calls_observer.fetch_add(1, Ordering::Relaxed);
        panic!("observer panic is isolated");
    });
    let diagnostics = Arc::new(Mutex::new(Vec::new()));
    let observed = Arc::clone(&diagnostics);
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        observed.lock().unwrap().push(RejectionDiagnostic::from(diagnostic));
    });

    let receipt = block_on(bus.publish(publish_request())).unwrap();
    let event_id = receipt.dispatched_event_id().unwrap();
    assert_eq!(acknowledgement(), *receipt.acknowledgement());
    assert_eq!(expected_outcome(), receipt.admission_outcome());
    assert_eq!(2, panic_calls.load(Ordering::Relaxed));
    assert_eq!(expected_rejections(event_id), *diagnostics.lock().unwrap());
    assert_async_metrics(&bus);
}
