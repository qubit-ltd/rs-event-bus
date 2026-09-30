// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public synchronous SPI contracts for the bounded local provider.

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

fn subscription_request(id: u64, topic: TopicAddress) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        topic,
        SubscriberId::new(format!("bounded-{id}")).expect("valid subscriber ID"),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::new(),
        TypeId::of::<u32>(),
    )
}

fn outbound(topic: TopicAddress, id: &str, value: u32) -> OutboundMessage {
    OutboundMessage::new(
        topic,
        EventId::new(id).expect("valid event ID"),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(value)),
    )
}

#[test]
fn test_full_subscription_queue_rejects_only_the_later_delivery() {
    let config = EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::new().queue_capacity(1).provider_options());
    let spi = LocalEventBusProvider
        .create_configured(&config)
        .expect("valid local configuration");
    let topic = TopicAddress::new("local.spi.bounded").expect("valid topic");
    let mut receiver = spi
        .subscribe(subscription_request(701, topic.clone()))
        .expect("subscription is accepted");

    let PublishAcknowledgement::DestinationAdmissions(first) = spi
        .publish(outbound(topic.clone(), "first", 1))
        .expect("first publish succeeds")
    else {
        panic!("local SPI reports destination admissions");
    };
    let PublishAcknowledgement::DestinationAdmissions(second) = spi
        .publish(outbound(topic, "second", 2))
        .expect("full queue is reported as admission rejection")
    else {
        panic!("local SPI reports destination admissions");
    };
    assert!(matches!(first[0].status(), AdmissionStatus::Accepted));
    assert!(matches!(second[0].status(), AdmissionStatus::Rejected(_)));
    let ReceiveOutcome::Message(message) = receiver.receive(Duration::ZERO).expect("queued message is received") else {
        panic!("the accepted message remains available");
    };
    assert_eq!("first", message.id().as_str());
}
