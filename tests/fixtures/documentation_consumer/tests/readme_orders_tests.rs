// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use event_bus_documentation_consumer::readme_orders::OrderCreated;
use event_bus_documentation_consumer::readme_orders::publish_order_created;
use qubit_event_bus::EventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

#[test]
fn test_readme_publish_returns_complete_partial_admission_receipt() {
    let bus = EventBus::local(LocalEventBusConfig::new().queue_capacity(1)).expect("local bus");
    let topic = Topic::<OrderCreated>::new_static("orders.created");
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let slow = bus
        .subscribe(
            SubscribeRequest::new("audit-log", topic.clone()).expect("request"),
            move |_| {
                entered_tx.send(()).expect("test receiver remains open");
                release_rx
                    .lock()
                    .expect("release lock")
                    .recv()
                    .expect("test releases blocked handler");
            },
        )
        .expect("slow subscription");
    let (view_tx, view_rx) = mpsc::channel();
    let view = bus
        .subscribe(
            SubscribeRequest::new("customer-view", topic).expect("request"),
            move |delivery| {
                view_tx
                    .send(delivery.event().id().clone())
                    .expect("test receiver remains open");
            },
        )
        .expect("view subscription");

    let first = publish_order_created(&bus, "order-42".into(), "customer-7".into(), 1250)
        .expect("first publication returns receipt");
    assert!(matches!(first.admission_outcome(), AdmissionOutcome::Accepted(_)));
    entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("audit handler entered");
    let first_event_id = view_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first view handler completed");
    assert!(!first_event_id.as_str().is_empty());
    let deadline = Instant::now() + Duration::from_secs(2);
    while view.delivery_metrics().metrics.completed == 0 && Instant::now() < deadline {
        thread::yield_now();
    }
    assert_eq!(
        view.delivery_metrics().metrics.completed,
        1,
        "first view delivery settled"
    );

    let partial = publish_order_created(&bus, "order-43".into(), "customer-7".into(), 1500)
        .expect("partial publication remains a successful receipt");
    assert!(!partial.duplicate_possible());
    assert!(!partial.input_event_id().as_str().is_empty());
    assert_eq!(partial.dispatched_event_id(), Some(partial.input_event_id()));
    assert!(matches!(
        partial.admission_outcome(),
        AdmissionOutcome::PartiallyAccepted(_)
    ));
    let PublishAcknowledgement::DestinationAdmissions(admissions) = partial.acknowledgement() else {
        panic!("local provider reports per-destination admission details");
    };
    let rejected = admissions
        .iter()
        .find(|admission| admission.status() != &AdmissionStatus::Accepted)
        .expect("full audit queue is reported as rejected");
    assert_eq!(rejected.subscriber_id().as_str(), "audit-log");
    let AdmissionStatus::Rejected(reason) = rejected.status() else {
        panic!("expected rejection reason");
    };
    assert!(!reason.is_empty());
    assert!(admissions.iter().any(|admission| {
        admission.subscriber_id().as_str() == "customer-view" && admission.status() == &AdmissionStatus::Accepted
    }));

    release_tx.send(()).expect("release handler");
    assert_eq!(
        view_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("second event reached the provider")
            .as_str(),
        partial.dispatched_event_id().expect("event was dispatched").as_str(),
    );
    slow.cancel().expect("cancel audit");
    view.cancel().expect("cancel view");
    let report = bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(2),
        })
        .expect("shutdown");
    assert_eq!(report.outcome, qubit_event_bus::spi::ShutdownOutcome::Complete);
}
