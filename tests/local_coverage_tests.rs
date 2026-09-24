// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Additional edge-contract coverage for the built-in local provider.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::EventBusConfig;
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
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OrderingKey;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;
use qubit_spi::ServiceProvider;

/// Creates the built-in provider with the requested per-subscription bound.
fn create_local(queue_capacity: usize) -> Arc<dyn EventBusSpi> {
    let options = LocalEventBusConfig::new()
        .queue_capacity(queue_capacity)
        .provider_options();
    let config = EventBusConfig::default().with_provider_options(options);
    LocalEventBusProvider
        .create_configured(&config)
        .expect("valid local provider configuration creates an SPI")
}

/// Registers one ephemeral subscription on the given local provider.
fn subscribe(spi: &dyn EventBusSpi, id: u64, topic: &str) -> Box<dyn EventSubscriptionSpi> {
    spi.subscribe(SpiSubscriptionRequest::new(
        Id::new(id),
        TopicAddress::new(topic).expect("test topic is valid"),
        SubscriberId::new(format!("coverage-subscriber-{id}")).expect("test subscriber ID is valid"),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::new(),
    ))
    .expect("valid local subscription is accepted")
}

/// Builds a native event with optional key and not-before delay metadata.
fn outbound(
    topic: &str,
    event_id: &str,
    value: u32,
    ordering_key: Option<&str>,
    delay: Option<Duration>,
) -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new(topic).expect("test topic is valid"),
        EventId::new(event_id).expect("test event ID is valid"),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        ordering_key.map(|key| OrderingKey::new(key).expect("test ordering key is valid")),
        delay,
        TransportPayload::Native(Arc::new(value)),
    )
}

/// Extracts the local per-destination outcomes from a publication.
fn destination_admissions(
    acknowledgement: PublishAcknowledgement,
) -> Vec<qubit_event_bus::model::DestinationAdmission> {
    let PublishAcknowledgement::DestinationAdmissions(admissions) = acknowledgement else {
        panic!("local provider reports one admission result per matching subscription");
    };
    admissions
}

#[test]
fn test_close_removes_subscription_route_and_discards_pending_messages() {
    let spi = create_local(2);
    let mut subscription = subscribe(spi.as_ref(), 1, "coverage.close");

    let accepted = spi
        .publish(outbound("coverage.close", "before-close", 1, None, None))
        .expect("publish before close succeeds");
    assert!(matches!(
        destination_admissions(accepted).as_slice(),
        [admission] if matches!(admission.status(), AdmissionStatus::Accepted)
    ));

    subscription.close().expect("close releases the local queue");
    assert!(matches!(
        subscription.receive(Duration::ZERO).expect("closed receive succeeds"),
        ReceiveOutcome::Closed
    ));
    let after_close = spi
        .publish(outbound("coverage.close", "after-close", 2, None, None))
        .expect("publication without subscribers is accepted by the provider");
    assert!(destination_admissions(after_close).is_empty());
}

#[test]
fn test_immediate_shutdown_wakes_a_blocked_receiver() {
    let spi = create_local(1);
    let subscription = subscribe(spi.as_ref(), 2, "coverage.shutdown");
    let (started_tx, started_rx) = mpsc::channel();
    let (outcome_tx, outcome_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut subscription = subscription;
        started_tx.send(()).expect("test receiver remains alive");
        let outcome = subscription.receive(Duration::from_secs(30));
        outcome_tx.send(outcome).expect("test receiver remains alive");
    });

    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("receiver thread starts");
    assert!(
        outcome_rx.try_recv().is_err(),
        "empty receive remains blocked before shutdown"
    );
    assert_eq!(
        ShutdownOutcome::Complete,
        spi.shutdown(ShutdownMode::Immediate)
            .expect("immediate shutdown completes")
    );
    assert!(matches!(
        outcome_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("shutdown wakes receive"),
        Ok(ReceiveOutcome::Closed)
    ));
}

#[test]
fn test_capacity_is_per_subscription_and_excludes_in_flight_messages() {
    let spi = create_local(1);
    let mut first = subscribe(spi.as_ref(), 3, "coverage.capacity");
    let _second = subscribe(spi.as_ref(), 4, "coverage.capacity");

    let first_admissions = destination_admissions(
        spi.publish(outbound("coverage.capacity", "capacity-1", 1, None, None))
            .expect("first publication succeeds"),
    );
    assert_eq!(2, first_admissions.len());
    assert!(
        first_admissions
            .iter()
            .all(|admission| matches!(admission.status(), AdmissionStatus::Accepted))
    );

    let ReceiveOutcome::Message(mut in_flight) = first
        .receive(Duration::ZERO)
        .expect("first subscription has a pending event")
    else {
        panic!("first subscription receives its event");
    };
    let second_admissions = destination_admissions(
        spi.publish(outbound("coverage.capacity", "capacity-2", 2, None, None))
            .expect("second publication returns per-destination outcomes"),
    );
    assert_eq!(2, second_admissions.len());
    assert!(second_admissions.iter().all(|admission| {
        matches!(admission.status(), AdmissionStatus::Rejected(reason) if reason.as_ref() == "subscription queue is full")
            == (admission.subscription_id() == Id::new(4))
    }));

    let token = in_flight
        .take_settlement()
        .expect("delivery includes a settlement token");
    first
        .settle(&token, DeliveryDisposition::Accept)
        .expect("in-flight settlement remains valid after independent admission");
    assert!(matches!(
        first
            .receive(Duration::ZERO)
            .expect("first queue is available after receive"),
        ReceiveOutcome::Message(_)
    ));
}

#[test]
fn test_retry_at_full_queue_preserves_the_original_in_flight_delivery() {
    let spi = create_local(1);
    let mut subscription = subscribe(spi.as_ref(), 5, "coverage.retry-capacity");
    spi.publish(outbound(
        "coverage.retry-capacity",
        "retry-original",
        1,
        Some("partition-a"),
        None,
    ))
    .expect("original publication succeeds");
    let ReceiveOutcome::Message(mut original) = subscription
        .receive(Duration::ZERO)
        .expect("original delivery is available")
    else {
        panic!("original delivery is received");
    };
    let token = original
        .take_settlement()
        .expect("delivery includes its settlement token");
    spi.publish(outbound(
        "coverage.retry-capacity",
        "queue-filler",
        2,
        Some("partition-a"),
        None,
    ))
    .expect("the pending queue accepts its boundary event");

    subscription
        .settle(&token, DeliveryDisposition::Retry)
        .expect("retry is retained until bounded queue capacity becomes available");
    subscription
        .settle(&token, DeliveryDisposition::Retry)
        .expect("repeated retry settlement is idempotent while deferred");
    let ReceiveOutcome::Message(retry) = subscription
        .receive(Duration::ZERO)
        .expect("deferred retry takes precedence over a same-key successor")
    else {
        panic!("deferred retry is available");
    };
    assert_eq!("retry-original", retry.id().as_str());
    let ReceiveOutcome::Message(filler) = subscription
        .receive(Duration::ZERO)
        .expect("the same-key successor remains queued")
    else {
        panic!("queue filler remains available");
    };
    assert_eq!("queue-filler", filler.id().as_str());
}

#[test]
fn test_settlement_rejects_wrong_provider_token_state_without_consuming_real_token() {
    let spi = create_local(1);
    let mut subscription = subscribe(spi.as_ref(), 6, "coverage.forged-token");
    spi.publish(outbound("coverage.forged-token", "valid-token", 7, None, None))
        .expect("publication succeeds");
    let ReceiveOutcome::Message(mut message) = subscription
        .receive(Duration::ZERO)
        .expect("published event is available")
    else {
        panic!("published event is received");
    };
    let real_token = message.take_settlement().expect("event has a provider token");
    let forged_token = SettlementToken::new(Id::new(6), String::from("forged provider state"));

    let error = subscription
        .settle(&forged_token, DeliveryDisposition::Accept)
        .expect_err("matching facade identity cannot validate provider-private token state");
    assert_eq!("invalid_settlement_token", error.kind());
    subscription
        .settle(&real_token, DeliveryDisposition::Accept)
        .expect("rejecting a forged token leaves the authentic in-flight token usable");
}

#[test]
fn test_delayed_pending_event_consumes_subscription_capacity() {
    let spi = create_local(1);
    let mut subscription = subscribe(spi.as_ref(), 6, "coverage.delayed-capacity");
    let delayed = spi
        .publish(outbound(
            "coverage.delayed-capacity",
            "delayed-capacity-head",
            1,
            Some("partition-a"),
            Some(Duration::from_secs(5)),
        ))
        .expect("delayed publication is admitted");
    assert!(matches!(
        destination_admissions(delayed).as_slice(),
        [admission] if matches!(admission.status(), AdmissionStatus::Accepted)
    ));

    let rejected = spi
        .publish(outbound(
            "coverage.delayed-capacity",
            "over-capacity",
            2,
            Some("partition-b"),
            None,
        ))
        .expect("full bounded queue reports a destination rejection");
    assert!(matches!(
        destination_admissions(rejected).as_slice(),
        [admission] if matches!(admission.status(), AdmissionStatus::Rejected(reason) if reason.as_ref() == "subscription queue is full")
    ));
    assert!(matches!(
        subscription
            .receive(Duration::ZERO)
            .expect("not-yet-due head is not delivered"),
        ReceiveOutcome::TimedOut
    ));
}

#[test]
fn test_receive_uses_earliest_partition_deadline_without_overtaking_same_key() {
    let spi = create_local(4);
    let mut subscription = subscribe(spi.as_ref(), 8, "coverage.partition-deadlines");
    spi.publish(outbound(
        "coverage.partition-deadlines",
        "partition-a-head",
        1,
        Some("partition-a"),
        Some(Duration::from_millis(250)),
    ))
    .expect("long-delayed partition head is admitted");
    spi.publish(outbound(
        "coverage.partition-deadlines",
        "partition-a-successor",
        2,
        Some("partition-a"),
        Some(Duration::from_millis(20)),
    ))
    .expect("earlier-deadline same-key successor is admitted");
    spi.publish(outbound(
        "coverage.partition-deadlines",
        "partition-b-head",
        3,
        Some("partition-b"),
        Some(Duration::from_millis(20)),
    ))
    .expect("shorter-delayed independent partition is admitted");
    spi.publish(outbound("coverage.partition-deadlines", "unkeyed-ready", 4, None, None))
        .expect("unkeyed message is admitted");

    let ReceiveOutcome::Message(unkeyed) = subscription
        .receive(Duration::ZERO)
        .expect("ready unkeyed message is immediately available")
    else {
        panic!("ready unkeyed message must bypass delayed keyed partitions");
    };
    assert_eq!("unkeyed-ready", unkeyed.id().as_str());

    let ReceiveOutcome::Message(shorter_partition) = subscription
        .receive(Duration::from_millis(150))
        .expect("receive waits until the earliest key-head deadline")
    else {
        panic!("partition-b becomes ready before the later partition-a head");
    };
    assert_eq!("partition-b-head", shorter_partition.id().as_str());

    for expected in ["partition-a-head", "partition-a-successor"] {
        let ReceiveOutcome::Message(message) = subscription
            .receive(Duration::from_secs(1))
            .expect("delayed partition-a events eventually become ready")
        else {
            panic!("partition-a queue head is received after its deadline");
        };
        assert_eq!(expected, message.id().as_str());
    }
}

#[test]
fn test_graceful_shutdown_times_out_for_undelivered_delay_head() {
    let spi = create_local(1);
    let mut subscription = subscribe(spi.as_ref(), 7, "coverage.delayed-shutdown");
    spi.publish(outbound(
        "coverage.delayed-shutdown",
        "shutdown-delay",
        1,
        Some("partition-a"),
        Some(Duration::from_secs(5)),
    ))
    .expect("delayed event is queued");

    assert_eq!(
        ShutdownOutcome::TimedOut,
        spi.shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_millis(5),
        })
        .expect("graceful shutdown returns its timeout outcome")
    );
    assert!(matches!(
        subscription
            .receive(Duration::ZERO)
            .expect("shutdown closes its receiver"),
        ReceiveOutcome::Closed
    ));
}
