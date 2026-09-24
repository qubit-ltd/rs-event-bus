// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public facade contract tests for provider admission acknowledgements.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::EventBus;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
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
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_id::Id;

struct AdmissionProvider {
    acknowledgements: Mutex<VecDeque<PublishAcknowledgement>>,
}

impl EventBusSpi for AdmissionProvider {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::DestinationAdmissions,
        )
    }

    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        Ok(self
            .acknowledgements
            .lock()
            .expect("acknowledgement queue lock")
            .pop_front()
            .expect("test acknowledgement was configured"))
    }

    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Err(provider_error("subscribe"))
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

fn provider_error(operation: &'static str) -> SpiError {
    SpiError::Operation {
        provider_id: "admission-test".into(),
        operation,
        resource: None,
        kind: "unsupported",
        retryable: Some(false),
        source: Box::new(std::io::Error::other("unsupported in this test provider")),
    }
}

fn publish(acknowledgement: PublishAcknowledgement) -> PublishAcknowledgement {
    let provider = Arc::new(AdmissionProvider {
        acknowledgements: Mutex::new(VecDeque::from([acknowledgement])),
    });
    let bus = EventBus::new(ProviderId::new("admission-test").expect("valid provider ID"), provider);
    let topic = Topic::<String>::new("orders.created").expect("valid topic");
    let request = PublishRequest::new(topic, "order-1".to_owned()).expect("request builds");
    let receipt = bus
        .publish(request)
        .expect("admission is a successful publication receipt");
    receipt.acknowledgement().clone()
}

#[test]
fn publish_receipt_preserves_empty_destination_snapshot() {
    assert_eq!(
        PublishAcknowledgement::DestinationAdmissions(Vec::new()),
        publish(PublishAcknowledgement::DestinationAdmissions(Vec::new()))
    );
}

#[test]
fn publish_receipt_preserves_partial_destination_admission() {
    let accepted_id = SubscriberId::new("accepted-subscriber").expect("valid subscriber ID");
    let rejected_id = SubscriberId::new("full-subscriber").expect("valid subscriber ID");
    let acknowledgement = publish(PublishAcknowledgement::DestinationAdmissions(vec![
        DestinationAdmission::new(Id::new(1), accepted_id, AdmissionStatus::Accepted),
        DestinationAdmission::new(
            Id::new(2),
            rejected_id,
            AdmissionStatus::Rejected("subscription queue is full".into()),
        ),
    ]));
    let PublishAcknowledgement::DestinationAdmissions(destinations) = acknowledgement else {
        panic!("provider destination admissions must be preserved");
    };
    assert_eq!(2, destinations.len());
    assert_eq!("accepted-subscriber", destinations[0].subscriber_id().as_str());
    assert_eq!(&AdmissionStatus::Accepted, destinations[0].status());
    assert_eq!("full-subscriber", destinations[1].subscriber_id().as_str());
    assert_eq!(
        &AdmissionStatus::Rejected("subscription queue is full".into()),
        destinations[1].status()
    );
}

#[test]
fn publish_receipt_is_successful_when_every_destination_rejects_admission() {
    let acknowledgement = publish(PublishAcknowledgement::DestinationAdmissions(vec![
        DestinationAdmission::new(
            Id::new(3),
            SubscriberId::new("full-subscriber").expect("valid subscriber ID"),
            AdmissionStatus::Rejected("subscription queue is full".into()),
        ),
    ]));
    let PublishAcknowledgement::DestinationAdmissions(destinations) = acknowledgement else {
        panic!("provider destination admissions must be preserved");
    };
    assert_eq!(1, destinations.len());
    assert!(matches!(destinations[0].status(), AdmissionStatus::Rejected(_)));
}
