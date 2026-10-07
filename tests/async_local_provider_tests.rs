// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Contract checks for the runtime-neutral async local provider.

mod support;

use std::any::TypeId;
use std::env;
use std::fs;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::mpsc;
use std::task::Poll;
use std::thread;
use std::time::Duration;

use qubit_clock::StdTimer;
use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::AsyncEventBusRegistry;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::AsyncConformanceHooks;
#[cfg(feature = "conformance")]
use qubit_event_bus::spi::conformance::run_async;
use qubit_id::Id;

use crate::support::manual_async::block_on;
use crate::support::manual_async::poll_once;

#[test]
fn test_async_local_delivers_and_settles_without_a_runtime_dependency() {
    let bus = block_on(AsyncEventBus::local(
        LocalEventBusConfig::new().queue_capacity(2),
    ))
    .expect("local event bus must be created from valid configuration");
    let topic =
        Topic::<String>::new("async.local.events").expect("static test topic must be valid");
    let subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("consumer", topic.clone())
                .expect("test subscription request must be valid"),
        ),
    )
    .expect("async subscription must be created");
    let (sender, receiver) = mpsc::channel();
    let runner = thread::spawn(move || {
        block_on(subscription.run(move |delivery| {
            let _ = sender.send(delivery.payload().clone());
            async { Ok(()) }
        }))
    });

    let _ = block_on(
        bus.publish(
            PublishRequest::new(topic, "message".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    assert_eq!(
        "message",
        receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("expected test message must arrive before timeout")
    );
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
    runner
        .join()
        .expect("subscription runner thread must not panic")
        .expect("subscription run must finish without an error");
}

#[test]
fn test_async_local_reports_capacity_rejection_per_destination() {
    let bus = block_on(AsyncEventBus::local(
        LocalEventBusConfig::new().queue_capacity(1),
    ))
    .expect("local event bus must be created from valid configuration");
    let topic =
        Topic::<String>::new("async.local.capacity").expect("static test topic must be valid");
    let _subscription = block_on(
        bus.subscribe(
            SubscribeRequest::new("consumer", topic.clone())
                .expect("test subscription request must be valid"),
        ),
    )
    .expect("async subscription must be created");

    let first = block_on(
        bus.publish(
            PublishRequest::new(topic.clone(), "first".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    let second = block_on(
        bus.publish(
            PublishRequest::new(topic, "second".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");

    assert!(matches!(
        first.admission_outcome(),
        AdmissionOutcome::Accepted(_)
    ));
    assert!(matches!(
        second.admission_outcome(),
        AdmissionOutcome::NoneAccepted(_)
    ));
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
}

#[test]
fn test_async_local_total_capacity_counts_in_flight_until_terminal_settlement() {
    let config = LocalEventBusConfig::new().max_total_outstanding(1);
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&config).expect("local SPI must accept its configuration"),
    );
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("local").expect("static local provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let first_topic =
        Topic::<String>::new("async.local.total-first").expect("static test topic must be valid");
    let second_topic =
        Topic::<String>::new("async.local.total-second").expect("static test topic must be valid");
    let mut first = block_on(spi.subscribe(spi_request(700, "first", "async.local.total-first")))
        .expect("async subscription must be created");
    let mut second =
        block_on(spi.subscribe(spi_request(701, "second", "async.local.total-second")))
            .expect("async subscription must be created");

    let accepted = block_on(
        bus.publish(
            PublishRequest::new(first_topic.clone(), "one".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    assert!(matches!(
        accepted.admission_outcome(),
        AdmissionOutcome::Accepted(_)
    ));
    let rejected = block_on(
        bus.publish(
            PublishRequest::new(second_topic.clone(), "two".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    assert!(matches!(
        rejected.admission_outcome(),
        AdmissionOutcome::NoneAccepted(_)
    ));

    let ReceiveOutcome::Message(mut message) =
        block_on(first.receive(Duration::ZERO)).expect("local receiver must return an outcome")
    else {
        panic!("accepted message is available for settlement");
    };
    let token = message
        .take_settlement()
        .expect("message must carry its expected settlement token");
    block_on(first.settle(&token, DeliveryDisposition::Retry))
        .expect("local settlement operation must succeed");
    let rejected = block_on(
        bus.publish(
            PublishRequest::new(second_topic.clone(), "three".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    assert!(matches!(
        rejected.admission_outcome(),
        AdmissionOutcome::NoneAccepted(_)
    ));

    let ReceiveOutcome::Message(mut retried) =
        block_on(first.receive(Duration::ZERO)).expect("local receiver must return an outcome")
    else {
        panic!("retried message remains available");
    };
    block_on(
        first.settle(
            &retried
                .take_settlement()
                .expect("message must carry its expected settlement token"),
            DeliveryDisposition::Accept,
        ),
    )
    .expect("local settlement operation must succeed");
    let accepted = block_on(
        bus.publish(
            PublishRequest::new(second_topic.clone(), "four".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    assert!(matches!(
        accepted.admission_outcome(),
        AdmissionOutcome::Accepted(_)
    ));

    let ReceiveOutcome::Message(mut second_message) =
        block_on(second.receive(Duration::ZERO)).expect("local receiver must return an outcome")
    else {
        panic!("accepted message is available for rejection");
    };
    block_on(
        second.settle(
            &second_message
                .take_settlement()
                .expect("message must carry its expected settlement token"),
            DeliveryDisposition::Reject,
        ),
    )
    .expect("local settlement operation must succeed");
    let accepted = block_on(
        bus.publish(
            PublishRequest::new(first_topic.clone(), "after-reject".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    assert!(matches!(
        accepted.admission_outcome(),
        AdmissionOutcome::Accepted(_)
    ));

    block_on(first.close()).expect("local receiver close must complete");
    let accepted = block_on(
        bus.publish(
            PublishRequest::new(second_topic, "after-close".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    assert!(matches!(
        accepted.admission_outcome(),
        AdmissionOutcome::Accepted(_)
    ));
    block_on(second.close()).expect("local receiver close must complete");
    let _ = block_on(spi.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
}

#[test]
fn test_async_local_drop_racing_publish_releases_capacity_after_close() {
    let config = LocalEventBusConfig::new().max_total_outstanding(1);
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&config).expect("local SPI must accept its configuration"),
    );
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("local").expect("static local provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let first_topic = Topic::<String>::new("async.local.drop-publish-first")
        .expect("static test topic must be valid");
    let second_topic = Topic::<String>::new("async.local.drop-publish-second")
        .expect("static test topic must be valid");
    let first =
        block_on(spi.subscribe(spi_request(702, "first", "async.local.drop-publish-first")))
            .expect("async subscription must be created");
    let mut second = block_on(spi.subscribe(spi_request(
        703,
        "second",
        "async.local.drop-publish-second",
    )))
    .expect("async subscription must be created");
    let _ = block_on(
        bus.publish(
            PublishRequest::new(first_topic, "occupy".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");

    let barrier = Arc::new(Barrier::new(2));
    let worker_barrier = Arc::clone(&barrier);
    let worker_bus = bus.clone();
    let worker_topic = second_topic.clone();
    let worker = thread::spawn(move || {
        worker_barrier.wait();
        drop(first);
        block_on(
            worker_bus.publish(
                PublishRequest::new(worker_topic, "racing".to_owned())
                    .expect("test publish request must be valid"),
            ),
        )
        .expect("publish request must be admitted")
    });
    barrier.wait();
    let raced = worker.join().expect("worker thread must not panic");
    if matches!(raced.admission_outcome(), AdmissionOutcome::Accepted(_)) {
        let ReceiveOutcome::Message(mut message) = block_on(second.receive(Duration::ZERO))
            .expect("local receiver must return an outcome")
        else {
            panic!("racing accepted message remains available");
        };
        block_on(
            second.settle(
                &message
                    .take_settlement()
                    .expect("message must carry its expected settlement token"),
                DeliveryDisposition::Accept,
            ),
        )
        .expect("local settlement operation must succeed");
    }
    let after_close = block_on(
        bus.publish(
            PublishRequest::new(second_topic, "after-close".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    assert!(matches!(
        after_close.admission_outcome(),
        AdmissionOutcome::Accepted(_)
    ));
    let _ = block_on(spi.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
}

#[test]
fn test_async_local_settlement_racing_shutdown_never_leaks_budget() {
    let config = LocalEventBusConfig::new().max_total_outstanding(1);
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&config).expect("local SPI must accept its configuration"),
    );
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("local").expect("static local provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let topic = Topic::<String>::new("async.local.settle-shutdown-race")
        .expect("static test topic must be valid");
    let mut receiver =
        block_on(spi.subscribe(spi_request(704, "race", "async.local.settle-shutdown-race")))
            .expect("async subscription must be created");
    let _ = block_on(
        bus.publish(
            PublishRequest::new(topic, "racing".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    let ReceiveOutcome::Message(mut message) =
        block_on(receiver.receive(Duration::ZERO)).expect("local receiver must return an outcome")
    else {
        panic!("accepted message is available for settlement");
    };
    let token = message
        .take_settlement()
        .expect("message must carry its expected settlement token");
    let barrier = Arc::new(Barrier::new(3));
    let settle_barrier = Arc::clone(&barrier);
    let settle = thread::spawn(move || {
        settle_barrier.wait();
        block_on(receiver.settle(&token, DeliveryDisposition::Accept))
    });
    let shutdown_barrier = Arc::clone(&barrier);
    let shutdown_spi = Arc::clone(&spi);
    let shutdown = thread::spawn(move || {
        shutdown_barrier.wait();
        block_on(shutdown_spi.shutdown(ShutdownMode::Immediate))
    });
    barrier.wait();

    let _ = settle.join().expect("worker thread must not panic");
    assert_eq!(
        ShutdownOutcome::Complete,
        shutdown
            .join()
            .expect("shutdown worker thread must not panic")
            .expect("immediate shutdown must complete successfully")
    );
}

#[test]
fn test_async_local_is_registered_in_the_async_provider_catalog() {
    let registry = AsyncEventBusRegistry::with_local()
        .expect("local async provider registry must be constructed");
    assert_eq!(
        vec!["local"],
        registry
            .provider_ids()
            .iter()
            .map(|id| id.as_str())
            .collect::<Vec<_>>()
    );
    let config = EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::new().provider_options());
    let bus =
        block_on(registry.create(&config)).expect("configured registry must create an event bus");
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
}

#[test]
fn test_async_local_registry_rejects_invalid_local_configuration() {
    let registry = AsyncEventBusRegistry::with_local()
        .expect("local async provider registry must be constructed");
    let config = EventBusConfig::default().with_provider_options(
        LocalEventBusConfig::new()
            .queue_capacity(0)
            .provider_options(),
    );
    assert!(block_on(registry.create(&config)).is_err());
    let unknown_option = EventBusConfig::default()
        .with_provider_options([(String::from("unknown.option"), String::from("1"))].into());
    assert!(block_on(registry.create(&unknown_option)).is_err());
}

#[test]
fn test_async_local_rejects_duplicate_and_type_conflicting_subscriptions() {
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new())
            .expect("local SPI must accept its configuration"),
    );
    let active = block_on(spi.subscribe(spi_request(31, "duplicate", "async.local.conflict")))
        .expect("async subscription must be created");
    let same_subscriber =
        block_on(spi.subscribe(spi_request(32, "duplicate", "async.local.conflict")))
            .expect("distinct subscription instances may share a logical subscriber ID");
    assert!(
        block_on(spi.subscribe(spi_request(31, "elsewhere", "async.local.other-topic"))).is_err()
    );
    let type_conflict = SpiSubscriptionRequest::new(
        Id::new(33),
        TopicAddress::new("async.local.conflict").expect("static SPI topic address must be valid"),
        SubscriberId::new("different").expect("static subscriber ID must be valid"),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::default(),
        TypeId::of::<u32>(),
    );
    assert!(block_on(spi.subscribe(type_conflict)).is_err());
    drop(active);
    drop(same_subscriber);
    let _ = block_on(spi.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
}

#[test]
fn test_async_local_with_timer_rejects_zero_limits() {
    for config in [
        LocalEventBusConfig::new().queue_capacity(0),
        LocalEventBusConfig::new().max_total_outstanding(0),
    ] {
        match AsyncLocalEventBusSpi::with_timer(&config, Arc::new(StdTimer::new())) {
            Err(ConfigurationError::InvalidField { .. }) => {}
            Err(error) => panic!("expected invalid configuration, got {error}"),
            Ok(_) => panic!("zero capacities must be rejected at construction"),
        }
    }
}

#[test]
fn test_async_local_topic_index_preserves_fanout_and_removes_closed_routes() {
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new())
            .expect("local SPI must accept its configuration"),
    );
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("local").expect("static local provider ID must be valid"),
        spi,
    )
    .expect("valid provider capabilities");
    let topic =
        Topic::<String>::new("async.local.topic-index").expect("static test topic must be valid");
    let cold =
        Topic::<String>::new("async.local.cold-index").expect("static test topic must be valid");
    let mut first = block_on(
        bus.subscribe(
            SubscribeRequest::new("same", topic.clone())
                .expect("test subscription request must be valid"),
        ),
    )
    .expect("async subscription must be created");
    let second = block_on(
        bus.subscribe(
            SubscribeRequest::new("same", topic.clone())
                .expect("test subscription request must be valid"),
        ),
    )
    .expect("async subscription must be created");
    let _cold = block_on(bus.subscribe(
        SubscribeRequest::new("cold", cold).expect("test subscription request must be valid"),
    ))
    .expect("async subscription must be created");

    let receipt = block_on(
        bus.publish(
            PublishRequest::new(topic.clone(), "one".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    let PublishAcknowledgement::DestinationAdmissions(admissions) = receipt.acknowledgement()
    else {
        panic!("local provider reports per-subscription admissions");
    };
    assert_eq!(2, admissions.len());

    block_on(first.close()).expect("local receiver close must complete");
    let receipt = block_on(bus.publish(
        PublishRequest::new(topic, "two".to_owned()).expect("test publish request must be valid"),
    ))
    .expect("publish request must be admitted");
    let PublishAcknowledgement::DestinationAdmissions(admissions) = receipt.acknowledgement()
    else {
        panic!("local provider reports per-subscription admissions");
    };
    assert_eq!(1, admissions.len());
    drop(second);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
}

#[test]
fn test_async_local_budget_admission_follows_subscription_id() {
    let config = LocalEventBusConfig::new().max_total_outstanding(1);
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&config).expect("local SPI must accept its configuration"),
    );
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("local").expect("static local provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let mut later = block_on(spi.subscribe(spi_request(22, "later", "async.local.ordered-budget")))
        .expect("async subscription must be created");
    let mut earlier =
        block_on(spi.subscribe(spi_request(11, "earlier", "async.local.ordered-budget")))
            .expect("async subscription must be created");
    let topic = Topic::<String>::new("async.local.ordered-budget")
        .expect("static test topic must be valid");

    for payload in ["first", "second"] {
        let receipt = block_on(
            bus.publish(
                PublishRequest::new(topic.clone(), payload.to_owned())
                    .expect("test publish request must be valid"),
            ),
        )
        .expect("publish request must be admitted");
        let PublishAcknowledgement::DestinationAdmissions(admissions) = receipt.acknowledgement()
        else {
            panic!("local provider reports per-subscription admissions");
        };
        assert_eq!(11, admissions[0].subscription_id().value());
        assert!(matches!(admissions[0].status(), AdmissionStatus::Accepted));
        assert_eq!(22, admissions[1].subscription_id().value());
        assert!(matches!(
            admissions[1].status(),
            AdmissionStatus::Rejected(_)
        ));
        let ReceiveOutcome::Message(mut message) = block_on(earlier.receive(Duration::ZERO))
            .expect("local receiver must return an outcome")
        else {
            panic!("lower subscription ID is selected first");
        };
        block_on(
            earlier.settle(
                &message
                    .take_settlement()
                    .expect("message must carry its expected settlement token"),
                DeliveryDisposition::Accept,
            ),
        )
        .expect("local settlement operation must succeed");
    }
    block_on(earlier.close()).expect("local receiver close must complete");
    block_on(later.close()).expect("local receiver close must complete");
    let _ = block_on(spi.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
}

#[test]
fn test_async_local_broadcasts_to_same_subscriber_instances_and_closes_them_independently() {
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new())
            .expect("local SPI must accept its configuration"),
    );
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("local").expect("static local provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let topic = Topic::<String>::new("async.local.same-subscriber")
        .expect("static test topic must be valid");
    let mut first = block_on(spi.subscribe(spi_request(
        50,
        "same-subscriber",
        "async.local.same-subscriber",
    )))
    .expect("async subscription must be created");
    let mut second = block_on(spi.subscribe(spi_request(
        51,
        "same-subscriber",
        "async.local.same-subscriber",
    )))
    .expect("async subscription must be created");

    let first_receipt = block_on(
        bus.publish(
            PublishRequest::new(topic.clone(), "first".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    let PublishAcknowledgement::DestinationAdmissions(first_admissions) =
        first_receipt.acknowledgement()
    else {
        panic!("local provider reports each subscription admission");
    };
    assert_eq!(2, first_admissions.len());
    assert_ne!(
        first_admissions[0].subscription_id(),
        first_admissions[1].subscription_id()
    );
    for admission in first_admissions {
        assert!(matches!(admission.status(), AdmissionStatus::Accepted));
    }
    let ReceiveOutcome::Message(first_message) =
        block_on(first.receive(Duration::ZERO)).expect("local receiver must return an outcome")
    else {
        panic!("first subscription receives the broadcast event");
    };
    let ReceiveOutcome::Message(second_message) =
        block_on(second.receive(Duration::ZERO)).expect("local receiver must return an outcome")
    else {
        panic!("second subscription receives the broadcast event");
    };
    let TransportPayload::Native(first_payload) = first_message.payload() else {
        panic!("local provider returns native payloads");
    };
    assert_eq!(
        "first",
        first_payload
            .downcast_ref::<String>()
            .expect("native payload must contain a String")
    );
    let TransportPayload::Native(second_payload) = second_message.payload() else {
        panic!("local provider returns native payloads");
    };
    assert_eq!(
        "first",
        second_payload
            .downcast_ref::<String>()
            .expect("native payload must contain a String")
    );
    block_on(first.close()).expect("local receiver close must complete");

    let remaining = block_on(
        bus.publish(
            PublishRequest::new(topic, "remaining".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    let PublishAcknowledgement::DestinationAdmissions(remaining_admissions) =
        remaining.acknowledgement()
    else {
        panic!("local provider reports each remaining subscription admission");
    };
    assert_eq!(1, remaining_admissions.len());
    assert_eq!(Id::new(51), remaining_admissions[0].subscription_id());
    assert!(matches!(
        block_on(first.receive(Duration::ZERO)).expect("local receiver must return an outcome"),
        ReceiveOutcome::Closed
    ));
    let ReceiveOutcome::Message(remaining_message) =
        block_on(second.receive(Duration::ZERO)).expect("local receiver must return an outcome")
    else {
        panic!("remaining subscription receives the next event");
    };
    let TransportPayload::Native(payload) = remaining_message.payload() else {
        panic!("local provider returns native payloads");
    };
    assert_eq!(
        "remaining",
        payload
            .downcast_ref::<String>()
            .expect("native payload must contain a String")
    );
    block_on(second.close()).expect("local receiver close must complete");
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
}

#[test]
fn test_async_local_repeated_close_of_old_subscription_preserves_reused_id() {
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new())
            .expect("local SPI must accept its configuration"),
    );
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("local").expect("static local provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let topic =
        Topic::<String>::new("async.local.reused-id").expect("static test topic must be valid");
    let mut old = block_on(spi.subscribe(spi_request(70, "reused", "async.local.reused-id")))
        .expect("async subscription must be created");
    block_on(old.close()).expect("local receiver close must complete");

    let mut replacement =
        block_on(spi.subscribe(spi_request(70, "replacement", "async.local.reused-id")))
            .expect("async subscription must be created");
    block_on(old.close()).expect("local receiver close must complete");
    let receipt = block_on(
        bus.publish(
            PublishRequest::new(topic, "still-registered".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    assert!(matches!(
        receipt.admission_outcome(),
        AdmissionOutcome::Accepted(_)
    ));
    assert!(matches!(
        block_on(replacement.receive(Duration::ZERO))
            .expect("local receiver must return an outcome"),
        ReceiveOutcome::Message(_)
    ));

    block_on(replacement.close()).expect("local receiver close must complete");
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
}

#[test]
fn test_async_local_same_subscriber_instances_report_independent_queue_capacity() {
    let config = LocalEventBusConfig::new().queue_capacity(1);
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&config).expect("local SPI must accept its configuration"),
    );
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("local").expect("static local provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let topic = Topic::<String>::new("async.local.same-subscriber-capacity")
        .expect("static test topic must be valid");
    let mut first = block_on(spi.subscribe(spi_request(
        71,
        "same",
        "async.local.same-subscriber-capacity",
    )))
    .expect("async subscription must be created");
    let mut second = block_on(spi.subscribe(spi_request(
        72,
        "same",
        "async.local.same-subscriber-capacity",
    )))
    .expect("async subscription must be created");

    let _ = block_on(
        bus.publish(
            PublishRequest::new(topic.clone(), "occupy".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    let ReceiveOutcome::Message(mut first_occupy) =
        block_on(first.receive(Duration::ZERO)).expect("local receiver must return an outcome")
    else {
        panic!("first mailbox receives the first event");
    };
    block_on(
        first.settle(
            &first_occupy
                .take_settlement()
                .expect("message must carry its expected settlement token"),
            DeliveryDisposition::Accept,
        ),
    )
    .expect("local settlement operation must succeed");

    let second_receipt = block_on(
        bus.publish(
            PublishRequest::new(topic, "independent".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    let PublishAcknowledgement::DestinationAdmissions(admissions) =
        second_receipt.acknowledgement()
    else {
        panic!("local provider reports each subscription admission");
    };
    let first_admission = admissions
        .iter()
        .find(|item| item.subscription_id() == Id::new(71))
        .expect("admission for the expected subscription must be present");
    let second_admission = admissions
        .iter()
        .find(|item| item.subscription_id() == Id::new(72))
        .expect("admission for the expected subscription must be present");
    assert!(matches!(
        first_admission.status(),
        AdmissionStatus::Accepted
    ));
    assert!(matches!(
        second_admission.status(),
        AdmissionStatus::Rejected(_)
    ));

    let ReceiveOutcome::Message(mut first_independent) =
        block_on(first.receive(Duration::ZERO)).expect("local receiver must return an outcome")
    else {
        panic!("first mailbox accepted its second event");
    };
    block_on(
        first.settle(
            &first_independent
                .take_settlement()
                .expect("message must carry its expected settlement token"),
            DeliveryDisposition::Accept,
        ),
    )
    .expect("local settlement operation must succeed");
    let ReceiveOutcome::Message(mut second_occupy) =
        block_on(second.receive(Duration::ZERO)).expect("local receiver must return an outcome")
    else {
        panic!("second mailbox retained its original event");
    };
    block_on(
        second.settle(
            &second_occupy
                .take_settlement()
                .expect("message must carry its expected settlement token"),
            DeliveryDisposition::Accept,
        ),
    )
    .expect("local settlement operation must succeed");
    block_on(first.close()).expect("local receiver close must complete");
    block_on(second.close()).expect("local receiver close must complete");
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
}

#[test]
fn test_async_local_drop_discards_pending_messages_for_the_same_subscriber() {
    let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::new()))
        .expect("local event bus must be created from valid configuration");
    let topic =
        Topic::<String>::new("async.local.recovery").expect("static test topic must be valid");
    let first = block_on(
        bus.subscribe(
            SubscribeRequest::new("recoverable", topic.clone())
                .expect("test subscription request must be valid"),
        ),
    )
    .expect("async subscription must be created");
    let _ = block_on(
        bus.publish(
            PublishRequest::new(topic.clone(), "retained".to_owned())
                .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    drop(first);

    drop(
        block_on(
            bus.subscribe(
                SubscribeRequest::new("recoverable", topic.clone())
                    .expect("test subscription request must be valid"),
            ),
        )
        .expect("async subscription must be created"),
    );
    let resumed = block_on(
        bus.subscribe(
            SubscribeRequest::new("recoverable", topic.clone())
                .expect("test subscription request must be valid"),
        ),
    )
    .expect("async subscription must be created");
    let (sender, receiver) = mpsc::channel();
    let runner = thread::spawn(move || {
        block_on(resumed.run(move |delivery| {
            let _ = sender.send(delivery.payload().clone());
            async { Ok(()) }
        }))
    });
    let _ = block_on(bus.publish(
        PublishRequest::new(topic, "fresh".to_owned()).expect("test publish request must be valid"),
    ))
    .expect("publish request must be admitted");
    assert_eq!(
        "fresh",
        receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("expected test message must arrive before timeout")
    );
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
    runner
        .join()
        .expect("subscription runner thread must not panic")
        .expect("subscription run must finish without an error");
}

#[test]
#[cfg(target_os = "linux")]
fn test_async_local_subscription_count_does_not_create_receiver_threads() {
    let name = "test_async_local_subscription_count_does_not_create_receiver_threads";
    if env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() != Ok("thread-count") {
        crate::support::isolated_process::run_case(name, "thread-count");
        return;
    }
    let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::new()))
        .expect("local event bus must be created from valid configuration");
    let before = fs::read_dir("/proc/self/task")
        .expect("Linux task directory must be readable")
        .count();
    let topic = Topic::<u32>::new("async.local.scale").expect("static test topic must be valid");
    let subscriptions = block_on(async {
        let mut subscriptions = Vec::new();
        for index in 0..128 {
            subscriptions.push(
                bus.subscribe(
                    SubscribeRequest::new(&format!("consumer-{index}"), topic.clone())
                        .expect("test subscription request must be valid"),
                )
                .await
                .expect("async subscription must be created"),
            );
        }
        subscriptions
    });
    let after = fs::read_dir("/proc/self/task")
        .expect("Linux task directory must be readable")
        .count();
    assert_eq!(
        before, after,
        "subscription creation must not spawn a receiver thread"
    );
    drop(subscriptions);
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
}

#[cfg(all(test, not(target_os = "linux")))]
#[test]
#[ignore = "receiver thread-count probe requires Linux /proc/self/task"]
fn test_async_local_subscription_count_does_not_create_receiver_threads() {}

#[test]
fn test_async_local_receive_cancellation_keeps_the_message_available() {
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new())
            .expect("local SPI must accept its configuration"),
    );
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("local").expect("static local provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let mut receiver = block_on(spi.subscribe(spi_request(1, "cancel-safe", "async.local.cancel")))
        .expect("async subscription must be created");
    assert!(matches!(
        block_on(receiver.receive(Duration::ZERO)).expect("local receiver must return an outcome"),
        ReceiveOutcome::TimedOut
    ));
    assert!(matches!(
        block_on(receiver.receive(Duration::from_millis(5)))
            .expect("local receiver must return an outcome"),
        ReceiveOutcome::TimedOut
    ));
    let mut pending = Box::pin(receiver.receive(Duration::MAX));
    assert!(poll_once(pending.as_mut()).is_pending());
    drop(pending);

    let _ = block_on(
        bus.publish(
            PublishRequest::new(
                Topic::<String>::new("async.local.cancel")
                    .expect("static test topic must be valid"),
                "survives".to_owned(),
            )
            .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    let outcome =
        block_on(receiver.receive(Duration::ZERO)).expect("local receiver must return an outcome");
    assert!(matches!(outcome, ReceiveOutcome::Message(_)));
}

#[test]
fn test_async_local_receiver_close_wakes_pending_receive() {
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new())
            .expect("local SPI must accept its configuration"),
    );
    let mut receiver =
        block_on(spi.subscribe(spi_request(30, "close-waiter", "async.local.close")))
            .expect("async subscription must be created");
    assert!(matches!(block_on(receiver.close()), Ok(())));
    assert!(matches!(
        block_on(receiver.receive(Duration::MAX)).expect("local receiver must return an outcome"),
        ReceiveOutcome::Closed
    ));
}

#[test]
fn test_async_local_close_removes_the_destination_and_topic_type_binding() {
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new())
            .expect("local SPI must accept its configuration"),
    );
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("local").expect("static local provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let mut receiver = block_on(spi.subscribe(spi_request(
        40,
        "close-removes",
        "async.local.close-removes",
    )))
    .expect("async subscription must be created");
    block_on(receiver.close()).expect("local receiver close must complete");

    let no_destination = block_on(
        bus.publish(
            PublishRequest::new(
                Topic::<u32>::new("async.local.close-removes")
                    .expect("static test topic must be valid"),
                1,
            )
            .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    assert!(matches!(
        no_destination.acknowledgement(),
        PublishAcknowledgement::DestinationAdmissions(admissions) if admissions.is_empty()
    ));

    let new_type = spi_request_with_type(
        41,
        "new-type",
        "async.local.close-removes",
        TypeId::of::<String>(),
    );
    let mut replacement =
        block_on(spi.subscribe(new_type)).expect("async subscription must be created");
    assert!(matches!(
        block_on(replacement.receive(Duration::ZERO))
            .expect("local receiver must return an outcome"),
        ReceiveOutcome::TimedOut
    ));
    block_on(replacement.close()).expect("local receiver close must complete");
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
}

#[test]
fn test_async_local_drop_does_not_leave_stale_destinations() {
    let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::new()))
        .expect("local event bus must be created from valid configuration");
    let topic = Topic::<u32>::new("async.local.stale-destinations")
        .expect("static test topic must be valid");
    for index in 0..100 {
        let subscriber = format!("consumer-{index}");
        let subscription = block_on(
            bus.subscribe(
                SubscribeRequest::new(&subscriber, topic.clone())
                    .expect("test subscription request must be valid"),
            ),
        )
        .expect("async subscription must be created");
        drop(subscription);
    }

    let receipt = block_on(
        bus.publish(PublishRequest::new(topic, 1).expect("test publish request must be valid")),
    )
    .expect("publish request must be admitted");
    assert!(matches!(
        receipt.acknowledgement(),
        PublishAcknowledgement::DestinationAdmissions(admissions) if admissions.is_empty()
    ));
    let _ = block_on(bus.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
}

#[test]
fn test_async_local_drop_discards_an_unsettled_in_flight_delivery() {
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new())
            .expect("local SPI must accept its configuration"),
    );
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("local").expect("static local provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let mut first =
        block_on(spi.subscribe(spi_request(10, "in-flight-recovery", "async.local.requeue")))
            .expect("async subscription must be created");
    let _ = block_on(
        bus.publish(
            PublishRequest::new(
                Topic::<String>::new("async.local.requeue")
                    .expect("static test topic must be valid"),
                "redeliver".to_owned(),
            )
            .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    let ReceiveOutcome::Message(mut first_message) =
        block_on(first.receive(Duration::MAX)).expect("local receiver must return an outcome")
    else {
        panic!("message should be delivered");
    };
    assert!(first_message.take_settlement().is_some());
    drop(first);

    let mut second =
        block_on(spi.subscribe(spi_request(11, "in-flight-recovery", "async.local.requeue")))
            .expect("async subscription must be created");
    assert!(matches!(
        block_on(second.receive(Duration::ZERO)).expect("local receiver must return an outcome"),
        ReceiveOutcome::TimedOut
    ));
    block_on(second.close()).expect("local receiver close must complete");
}

#[test]
fn test_async_local_settle_and_close_can_be_retried_after_unpolled_future_drop() {
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new())
            .expect("local SPI must accept its configuration"),
    );
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("local").expect("static local provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let mut receiver = block_on(spi.subscribe(spi_request(
        35,
        "cancelled-ops",
        "async.local.cancelled-ops",
    )))
    .expect("async subscription must be created");
    let _ = block_on(
        bus.publish(
            PublishRequest::new(
                Topic::<String>::new("async.local.cancelled-ops")
                    .expect("static test topic must be valid"),
                "settle after retry".to_owned(),
            )
            .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    let ReceiveOutcome::Message(mut message) =
        block_on(receiver.receive(Duration::ZERO)).expect("local receiver must return an outcome")
    else {
        panic!("message should be delivered");
    };
    let token = message
        .take_settlement()
        .expect("local delivery has a settlement token");

    drop(receiver.settle(&token, DeliveryDisposition::Accept));
    block_on(receiver.settle(&token, DeliveryDisposition::Accept))
        .expect("local settlement operation must succeed");
    drop(receiver.close());
    block_on(receiver.close()).expect("local receiver close must complete");
    assert!(matches!(
        block_on(receiver.receive(Duration::ZERO)).expect("local receiver must return an outcome"),
        ReceiveOutcome::Closed
    ));
    let _ = block_on(spi.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
}

#[test]
fn test_async_local_shutdown_wakes_pending_receives_and_cancelled_shutdown_can_retry() {
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new())
            .expect("local SPI must accept its configuration"),
    );
    let mut receiver =
        block_on(spi.subscribe(spi_request(20, "shutdown-waiter", "async.local.cancel")))
            .expect("async subscription must be created");
    let mut receive = Box::pin(receiver.receive(Duration::MAX));
    assert!(poll_once(receive.as_mut()).is_pending());
    let _ = block_on(spi.shutdown(ShutdownMode::Immediate))
        .expect("local event bus shutdown must complete");
    assert!(matches!(
        poll_once(receive.as_mut()),
        Poll::Ready(Ok(ReceiveOutcome::Closed))
    ));
    drop(receive);

    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new())
            .expect("local SPI must accept its configuration"),
    );
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("local").expect("static local provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let _receiver =
        block_on(spi.subscribe(spi_request(21, "graceful-waiter", "async.local.cancel")))
            .expect("async subscription must be created");
    let _ = block_on(
        bus.publish(
            PublishRequest::new(
                Topic::<String>::new("async.local.cancel")
                    .expect("static test topic must be valid"),
                "pending".to_owned(),
            )
            .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    let mut graceful = Box::pin(spi.shutdown(ShutdownMode::Graceful {
        timeout: Duration::MAX,
    }));
    assert!(poll_once(graceful.as_mut()).is_pending());
    drop(graceful);
    assert!(matches!(
        block_on(spi.shutdown(ShutdownMode::Immediate))
            .expect("local event bus shutdown must complete"),
        ShutdownOutcome::Complete
    ));
}

#[test]
fn test_async_local_graceful_shutdown_observes_finite_timeout() {
    let spi = Arc::new(
        AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new())
            .expect("local SPI must accept its configuration"),
    );
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("local").expect("static local provider ID must be valid"),
        spi.clone(),
    )
    .expect("valid provider capabilities");
    let _receiver =
        block_on(spi.subscribe(spi_request(34, "finite-shutdown", "async.local.finite")))
            .expect("async subscription must be created");
    let _ = block_on(
        bus.publish(
            PublishRequest::new(
                Topic::<String>::new("async.local.finite")
                    .expect("static test topic must be valid"),
                "pending".to_owned(),
            )
            .expect("test publish request must be valid"),
        ),
    )
    .expect("publish request must be admitted");
    assert!(matches!(
        block_on(spi.shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_millis(1),
        }))
        .expect("local event bus shutdown must complete"),
        ShutdownOutcome::TimedOut
    ));
}

/// Builds a local SPI request for a native `String` payload.
fn spi_request(id: u64, subscriber: &str, topic: &str) -> SpiSubscriptionRequest {
    spi_request_with_type(id, subscriber, topic, TypeId::of::<String>())
}

/// Builds a local SPI request with an explicit native payload type identity.
fn spi_request_with_type(
    id: u64,
    subscriber: &str,
    topic: &str,
    payload_type_id: TypeId,
) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        TopicAddress::new(topic).expect("static SPI topic address must be valid"),
        SubscriberId::new(subscriber).expect("static subscriber ID must be valid"),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::default(),
        payload_type_id,
    )
}

#[cfg(feature = "conformance")]
#[test]
fn test_async_local_passes_public_spi_conformance_publish_cases() {
    let report = block_on(run_async(
        || async {
            Arc::new(
                AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new())
                    .expect("local SPI must accept its configuration"),
            ) as Arc<dyn AsyncEventBusSpi>
        },
        &AsyncConformanceHooks::default(),
    ));
    assert!(report.all_passed());
    report.assert_all_passed();
}
