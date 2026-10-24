// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public SPI contract for preserving native event identity and payload.

use std::any::TypeId;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;
use qubit_spi::ServiceProvider;

#[test]
fn test_published_event_keeps_identity_and_native_payload() {
    let config = EventBusConfig::default().with_provider_options(LocalEventBusConfig::new().provider_options());
    let spi = LocalEventBusProvider
        .create_configured(&config)
        .expect("valid local configuration");
    let topic = TopicAddress::new("internal.event.preservation").expect("valid topic");
    let mut receiver = spi
        .subscribe(SpiSubscriptionRequest::new(
            Id::new(9004),
            topic.clone(),
            SubscriberId::new("event-preservation").expect("valid subscriber ID"),
            None,
            SubscriptionDurability::Ephemeral,
            StartPosition::New,
            ProviderOptions::new(),
            TypeId::of::<u32>(),
        ))
        .expect("subscription is accepted");
    let event_id = EventId::new("preserved-event").expect("valid event ID");
    let outbound = OutboundMessage::new(
        topic,
        event_id.clone(),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(31_u32)),
    );

    let PublishAcknowledgement::DestinationAdmissions(admissions) = spi.publish(outbound).expect("publish succeeds")
    else {
        panic!("local provider reports destination admissions");
    };
    assert!(
        matches!(admissions[0].status(), AdmissionStatus::Accepted),
        "published event should be accepted by its destination"
    );
    let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO).expect("receive succeeds") else {
        panic!("published event is available immediately");
    };
    assert_eq!(event_id.as_str(), message.id().as_str());
    assert!(
        matches!(
            message.payload(),
            TransportPayload::Native(payload) if payload.downcast_ref::<u32>() == Some(&31)
        ),
        "published native payload should retain its u32 value"
    );
}
