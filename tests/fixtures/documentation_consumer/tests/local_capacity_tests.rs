// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Checks local declared-weight admission through the public facade.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::CheckedPublishError;
use qubit_event_bus::EventBus;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

/// Builds configuration with independent count and declared-weight limits.
fn configured_bus(weight_budget: usize) -> EventBus {
    EventBus::local(
        LocalEventBusConfig::new()
            .queue_capacity(8)
            .max_total_outstanding(8)
            .max_total_outstanding_weight_bytes(
                NonZeroUsize::new(weight_budget).expect("positive weight budget"),
            ),
    )
    .expect("local event bus")
}

/// Creates a request whose `String` payload declares UTF-8 byte weight.
fn weighted_request(topic: &Topic<String>, payload: &str) -> PublishRequest<String> {
    let options = PublishOptions::<String>::builder()
        .native_payload_weight(|value| {
            NonZeroUsize::new(value.len().max(1)).expect("positive payload weight")
        })
        .build();
    PublishRequest::new(topic.clone(), payload.to_owned())
        .expect("publish request")
        .with_options(options)
}

/// Closes a local bus and requires graceful completion.
fn close_bus(bus: &EventBus) {
    let report = bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(3),
        })
        .expect("graceful shutdown");
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
}

/// Rejects an application-declared payload weight above the provider budget.
#[test]
fn test_declared_weight_over_budget_is_not_admitted() {
    let bus = configured_bus(4);
    let topic = Topic::<String>::new("capacity.over-budget").expect("topic");
    let _subscription = bus
        .subscribe(SubscribeRequest::new("consumer", topic.clone()).expect("subscription"), |_| {})
        .expect("subscribe");

    let error = bus
        .publish_checked(
            weighted_request(&topic, "12345"),
            AdmissionRequirement::AtLeastOneAccepted,
        )
        .expect_err("weight above budget must be rejected");
    match error {
        CheckedPublishError::Admission { receipt, .. } => {
            assert!(matches!(receipt.admission_outcome(), AdmissionOutcome::NoneAccepted(_)));
        }
        other => panic!("expected admission error, got {other:?}"),
    }
    close_bus(&bus);
}

/// Shows that one event's fanout consumes weight separately per destination.
#[test]
fn test_fanout_charges_declared_weight_for_each_accepted_target() {
    let bus = configured_bus(5);
    let topic = Topic::<String>::new("capacity.fanout").expect("topic");
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let release_receiver = Arc::new(Mutex::new(release_receiver));
    let mut subscriptions = Vec::new();
    for subscriber in ["first", "second"] {
        let entered_sender = entered_sender.clone();
        let release_receiver = Arc::clone(&release_receiver);
        subscriptions.push(
            bus.subscribe(
                SubscribeRequest::new(subscriber, topic.clone()).expect("subscription"),
                move |_| {
                    entered_sender.send(()).expect("notify handler entry");
                    release_receiver
                        .lock()
                        .expect("release receiver lock")
                        .recv()
                        .expect("release handler gate");
                },
            )
            .expect("subscribe"),
        );
    }

    let receipt = bus
        .publish_checked(
            weighted_request(&topic, "12345"),
            AdmissionRequirement::AtLeastOneAccepted,
        )
        .expect("one target should be admitted");
    assert!(matches!(
        receipt.admission_outcome(),
        AdmissionOutcome::PartiallyAccepted(summary)
            if summary.accepted == 1 && summary.rejected == 1
    ));
    entered_receiver
        .recv_timeout(Duration::from_secs(3))
        .expect("admitted handler entered");
    release_sender.send(()).expect("release handler");
    close_bus(&bus);
    drop(subscriptions);
}

/// Rejects a native publication when weighted admission has no weight callback.
#[test]
fn test_weight_budget_rejects_native_publish_without_declaration() {
    let bus = configured_bus(8);
    let topic = Topic::<String>::new("capacity.missing-weight").expect("topic");
    let (handler_sender, handler_receiver) = mpsc::channel();
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("consumer", topic.clone()).expect("subscription"),
            move |_| handler_sender.send(()).expect("handler receiver remains open"),
        )
        .expect("subscribe");
    let request = PublishRequest::new(topic, "payload".to_owned()).expect("request");

    let failure = bus
        .publish_checked(request, AdmissionRequirement::AtLeastOneAccepted)
        .expect_err("missing declared weight must be rejected");
    match failure {
        CheckedPublishError::Publish(failure) => {
            assert_eq!(failure.effect(), PublishEffect::NotAccepted);
            assert!(matches!(
                failure.cause(),
                PublishError::Spi(error)
                    if error.kind() == "missing_native_payload_weight"
                        && error.retryable() == Some(false)
            ));
        }
        other => panic!("expected known publish rejection, got {other:?}"),
    }
    assert!(matches!(
        handler_receiver.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    close_bus(&bus);
}
