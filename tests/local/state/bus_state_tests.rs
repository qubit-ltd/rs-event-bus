// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Lifecycle contracts for local provider topic state.

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
fn test_dropped_destination_is_not_reused_when_topic_state_is_rebuilt() {
    let provider = LocalEventBusProvider;
    let config = EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::new().provider_options());
    let spi = provider
        .create_configured(&config)
        .expect("valid local configuration");
    let topic = TopicAddress::new("state.lifecycle").expect("valid topic");
    let stale = spi
        .subscribe(subscription_request(201, topic.clone()))
        .expect("first subscription is accepted");
    drop(stale);

    let mut current = spi
        .subscribe(subscription_request(202, topic.clone()))
        .expect("topic state can be rebuilt after its destination is dropped");
    let message = OutboundMessage::new(
        topic,
        EventId::new("state-rebuilt").expect("valid event ID"),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        None,
        TransportPayload::Native(Arc::new(42_u32)),
    );

    let PublishAcknowledgement::DestinationAdmissions(admissions) =
        spi.publish(message).expect("publish succeeds")
    else {
        panic!("local provider returns destination admissions");
    };
    assert_eq!(1, admissions.len());
    assert_eq!(Id::new(202), admissions[0].subscription_id());
    assert!(matches!(admissions[0].status(), AdmissionStatus::Accepted));
    assert!(matches!(
        current.receive(Duration::ZERO),
        Ok(ReceiveOutcome::Message(_))
    ));
}

fn subscription_request(id: u64, topic: TopicAddress) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        topic,
        SubscriberId::new(format!("subscriber-{id}")).expect("valid subscriber ID"),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::new(),
        TypeId::of::<u32>(),
    )
}
