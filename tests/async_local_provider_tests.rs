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
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::ProviderOptions;
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
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use support::manual_async::block_on;

#[test]
fn async_local_delivers_and_settles_without_a_runtime_dependency() {
    let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::new().queue_capacity(2))).unwrap();
    let topic = Topic::<String>::new("async.local.events").unwrap();
    let mut subscription = block_on(bus.subscribe(SubscribeRequest::new("consumer", topic.clone()).unwrap())).unwrap();
    let (sender, receiver) = mpsc::channel();
    let runner = std::thread::spawn(move || {
        block_on(subscription.run(move |delivery| {
            let _ = sender.send(delivery.payload().clone());
            async { Ok(()) }
        }))
    });

    block_on(bus.publish(PublishRequest::new(topic, "message".to_owned()).unwrap())).unwrap();
    assert_eq!("message", receiver.recv_timeout(Duration::from_secs(2)).unwrap());
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    runner.join().unwrap().unwrap();
}

#[test]
fn async_local_reports_capacity_rejection_per_destination() {
    let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::new().queue_capacity(1))).unwrap();
    let topic = Topic::<String>::new("async.local.capacity").unwrap();
    let _subscription = block_on(bus.subscribe(SubscribeRequest::new("consumer", topic.clone()).unwrap())).unwrap();

    let first = block_on(bus.publish(PublishRequest::new(topic.clone(), "first".to_owned()).unwrap())).unwrap();
    let second = block_on(bus.publish(PublishRequest::new(topic, "second".to_owned()).unwrap())).unwrap();

    assert!(matches!(first.admission_outcome(), AdmissionOutcome::Accepted(_)));
    assert!(matches!(second.admission_outcome(), AdmissionOutcome::NoneAccepted(_)));
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn async_local_total_capacity_counts_in_flight_until_terminal_settlement() {
    let config = LocalEventBusConfig::new().max_total_outstanding(1);
    let spi = Arc::new(qubit_event_bus::local::AsyncLocalEventBusSpi::new(&config).unwrap());
    let bus = AsyncEventBus::from_spi(ProviderId::new("local").unwrap(), spi.clone());
    let first_topic = Topic::<String>::new("async.local.total-first").unwrap();
    let second_topic = Topic::<String>::new("async.local.total-second").unwrap();
    let mut first = block_on(spi.subscribe(spi_request(700, "first", "async.local.total-first"))).unwrap();
    let mut second = block_on(spi.subscribe(spi_request(701, "second", "async.local.total-second"))).unwrap();

    let accepted = block_on(bus.publish(PublishRequest::new(first_topic.clone(), "one".to_owned()).unwrap())).unwrap();
    assert!(matches!(accepted.admission_outcome(), AdmissionOutcome::Accepted(_)));
    let rejected = block_on(bus.publish(PublishRequest::new(second_topic.clone(), "two".to_owned()).unwrap())).unwrap();
    assert!(matches!(
        rejected.admission_outcome(),
        AdmissionOutcome::NoneAccepted(_)
    ));

    let ReceiveOutcome::Message(mut message) = block_on(first.receive(Duration::ZERO)).unwrap() else {
        panic!("accepted message is available for settlement");
    };
    let token = message.take_settlement().unwrap();
    block_on(first.settle(&token, DeliveryDisposition::Retry)).unwrap();
    let rejected =
        block_on(bus.publish(PublishRequest::new(second_topic.clone(), "three".to_owned()).unwrap())).unwrap();
    assert!(matches!(
        rejected.admission_outcome(),
        AdmissionOutcome::NoneAccepted(_)
    ));

    let ReceiveOutcome::Message(mut retried) = block_on(first.receive(Duration::ZERO)).unwrap() else {
        panic!("retried message remains available");
    };
    block_on(first.settle(&retried.take_settlement().unwrap(), DeliveryDisposition::Accept)).unwrap();
    let accepted =
        block_on(bus.publish(PublishRequest::new(second_topic.clone(), "four".to_owned()).unwrap())).unwrap();
    assert!(matches!(accepted.admission_outcome(), AdmissionOutcome::Accepted(_)));

    let ReceiveOutcome::Message(mut second_message) = block_on(second.receive(Duration::ZERO)).unwrap() else {
        panic!("accepted message is available for rejection");
    };
    block_on(second.settle(&second_message.take_settlement().unwrap(), DeliveryDisposition::Reject)).unwrap();
    let accepted =
        block_on(bus.publish(PublishRequest::new(first_topic.clone(), "after-reject".to_owned()).unwrap())).unwrap();
    assert!(matches!(accepted.admission_outcome(), AdmissionOutcome::Accepted(_)));

    block_on(first.close()).unwrap();
    let accepted = block_on(bus.publish(PublishRequest::new(second_topic, "after-close".to_owned()).unwrap())).unwrap();
    assert!(matches!(accepted.admission_outcome(), AdmissionOutcome::Accepted(_)));
    block_on(second.close()).unwrap();
    block_on(spi.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn async_local_drop_racing_publish_releases_capacity_after_close() {
    let config = LocalEventBusConfig::new().max_total_outstanding(1);
    let spi = Arc::new(qubit_event_bus::local::AsyncLocalEventBusSpi::new(&config).unwrap());
    let bus = AsyncEventBus::from_spi(ProviderId::new("local").unwrap(), spi.clone());
    let first_topic = Topic::<String>::new("async.local.drop-publish-first").unwrap();
    let second_topic = Topic::<String>::new("async.local.drop-publish-second").unwrap();
    let first = block_on(spi.subscribe(spi_request(702, "first", "async.local.drop-publish-first"))).unwrap();
    let mut second = block_on(spi.subscribe(spi_request(703, "second", "async.local.drop-publish-second"))).unwrap();
    block_on(bus.publish(PublishRequest::new(first_topic, "occupy".to_owned()).unwrap())).unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let worker_barrier = Arc::clone(&barrier);
    let worker_bus = bus.clone();
    let worker_topic = second_topic.clone();
    let worker = std::thread::spawn(move || {
        worker_barrier.wait();
        drop(first);
        block_on(worker_bus.publish(PublishRequest::new(worker_topic, "racing".to_owned()).unwrap())).unwrap()
    });
    barrier.wait();
    let raced = worker.join().unwrap();
    if matches!(raced.admission_outcome(), AdmissionOutcome::Accepted(_)) {
        let ReceiveOutcome::Message(mut message) = block_on(second.receive(Duration::ZERO)).unwrap() else {
            panic!("racing accepted message remains available");
        };
        block_on(second.settle(&message.take_settlement().unwrap(), DeliveryDisposition::Accept)).unwrap();
    }
    let after_close =
        block_on(bus.publish(PublishRequest::new(second_topic, "after-close".to_owned()).unwrap())).unwrap();
    assert!(matches!(after_close.admission_outcome(), AdmissionOutcome::Accepted(_)));
    block_on(spi.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn async_local_settlement_racing_shutdown_never_leaks_budget() {
    let config = LocalEventBusConfig::new().max_total_outstanding(1);
    let spi = Arc::new(qubit_event_bus::local::AsyncLocalEventBusSpi::new(&config).unwrap());
    let bus = AsyncEventBus::from_spi(ProviderId::new("local").unwrap(), spi.clone());
    let topic = Topic::<String>::new("async.local.settle-shutdown-race").unwrap();
    let mut receiver = block_on(spi.subscribe(spi_request(704, "race", "async.local.settle-shutdown-race"))).unwrap();
    block_on(bus.publish(PublishRequest::new(topic, "racing".to_owned()).unwrap())).unwrap();
    let ReceiveOutcome::Message(mut message) = block_on(receiver.receive(Duration::ZERO)).unwrap() else {
        panic!("accepted message is available for settlement");
    };
    let token = message.take_settlement().unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let settle_barrier = Arc::clone(&barrier);
    let settle = std::thread::spawn(move || {
        settle_barrier.wait();
        block_on(receiver.settle(&token, DeliveryDisposition::Accept))
    });
    let shutdown_barrier = Arc::clone(&barrier);
    let shutdown_spi = Arc::clone(&spi);
    let shutdown = std::thread::spawn(move || {
        shutdown_barrier.wait();
        block_on(shutdown_spi.shutdown(ShutdownMode::Immediate))
    });
    barrier.wait();

    let _ = settle.join().unwrap();
    assert_eq!(
        qubit_event_bus::spi::ShutdownOutcome::Complete,
        shutdown.join().unwrap().unwrap()
    );
}

#[test]
fn async_local_is_registered_in_the_async_provider_catalog() {
    let registry = qubit_event_bus::AsyncEventBusRegistry::with_local().unwrap();
    assert_eq!(
        vec!["local"],
        registry.provider_ids().iter().map(|id| id.as_str()).collect::<Vec<_>>()
    );
    let config =
        qubit_event_bus::EventBusConfig::default().with_provider_options(LocalEventBusConfig::new().provider_options());
    let bus = block_on(registry.create(&config)).unwrap();
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn async_local_registry_rejects_invalid_local_configuration() {
    let registry = qubit_event_bus::AsyncEventBusRegistry::with_local().unwrap();
    let config = qubit_event_bus::EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::new().queue_capacity(0).provider_options());
    assert!(block_on(registry.create(&config)).is_err());
    let unknown_option = qubit_event_bus::EventBusConfig::default()
        .with_provider_options([(String::from("unknown.option"), String::from("1"))].into());
    assert!(block_on(registry.create(&unknown_option)).is_err());
}

#[test]
fn async_local_rejects_duplicate_and_type_conflicting_subscriptions() {
    let spi = Arc::new(qubit_event_bus::local::AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap());
    let active = block_on(spi.subscribe(spi_request(31, "duplicate", "async.local.conflict"))).unwrap();
    assert!(block_on(spi.subscribe(spi_request(32, "duplicate", "async.local.conflict"))).is_err());
    let type_conflict = SpiSubscriptionRequest::new(
        qubit_id::Id::new(33),
        TopicAddress::new("async.local.conflict").unwrap(),
        SubscriberId::new("different").unwrap(),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::default(),
        TypeId::of::<u32>(),
    );
    assert!(block_on(spi.subscribe(type_conflict)).is_err());
    drop(active);
    block_on(spi.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn async_local_drop_discards_pending_messages_for_the_same_subscriber() {
    let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::new())).unwrap();
    let topic = Topic::<String>::new("async.local.recovery").unwrap();
    let first = block_on(bus.subscribe(SubscribeRequest::new("recoverable", topic.clone()).unwrap())).unwrap();
    block_on(bus.publish(PublishRequest::new(topic.clone(), "retained".to_owned()).unwrap())).unwrap();
    drop(first);

    drop(block_on(bus.subscribe(SubscribeRequest::new("recoverable", topic.clone()).unwrap())).unwrap());
    let mut resumed = block_on(bus.subscribe(SubscribeRequest::new("recoverable", topic.clone()).unwrap())).unwrap();
    let (sender, receiver) = mpsc::channel();
    let runner = std::thread::spawn(move || {
        block_on(resumed.run(move |delivery| {
            let _ = sender.send(delivery.payload().clone());
            async { Ok(()) }
        }))
    });
    block_on(bus.publish(PublishRequest::new(topic, "fresh".to_owned()).unwrap())).unwrap();
    assert_eq!("fresh", receiver.recv_timeout(Duration::from_secs(2)).unwrap());
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    runner.join().unwrap().unwrap();
}

#[test]
fn async_local_subscription_count_does_not_create_receiver_threads() {
    let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::new())).unwrap();
    let before = process_thread_count();
    let topic = Topic::<u32>::new("async.local.scale").unwrap();
    let subscriptions = block_on(async {
        let mut subscriptions = Vec::new();
        for index in 0..128 {
            subscriptions.push(
                bus.subscribe(SubscribeRequest::new(&format!("consumer-{index}"), topic.clone()).unwrap())
                    .await
                    .unwrap(),
            );
        }
        subscriptions
    });
    let after = process_thread_count();
    if let (Some(before), Some(after)) = (before, after) {
        assert!(
            after <= before + 3,
            "async local created receiver threads: {before} -> {after}"
        );
    }
    drop(subscriptions);
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn async_local_receive_cancellation_keeps_the_message_available() {
    let spi = Arc::new(qubit_event_bus::local::AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap());
    let bus = AsyncEventBus::from_spi(ProviderId::new("local").unwrap(), spi.clone());
    let mut receiver = block_on(spi.subscribe(spi_request(1, "cancel-safe", "async.local.cancel"))).unwrap();
    assert!(matches!(
        block_on(receiver.receive(Duration::ZERO)).unwrap(),
        ReceiveOutcome::TimedOut
    ));
    assert!(matches!(
        block_on(receiver.receive(Duration::from_millis(5))).unwrap(),
        ReceiveOutcome::TimedOut
    ));
    let mut pending = Box::pin(receiver.receive(Duration::MAX));
    assert!(support::manual_async::poll_once(pending.as_mut()).is_pending());
    drop(pending);

    block_on(
        bus.publish(
            PublishRequest::new(
                Topic::<String>::new("async.local.cancel").unwrap(),
                "survives".to_owned(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let outcome = block_on(receiver.receive(Duration::ZERO)).unwrap();
    assert!(matches!(outcome, ReceiveOutcome::Message(_)));
}

#[test]
fn async_local_receiver_close_wakes_pending_receive() {
    let spi = Arc::new(qubit_event_bus::local::AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap());
    let mut receiver = block_on(spi.subscribe(spi_request(30, "close-waiter", "async.local.close"))).unwrap();
    assert!(matches!(block_on(receiver.close()), Ok(())));
    assert!(matches!(
        block_on(receiver.receive(Duration::MAX)).unwrap(),
        ReceiveOutcome::Closed
    ));
}

#[test]
fn async_local_close_removes_the_destination_and_topic_type_binding() {
    let spi = Arc::new(qubit_event_bus::local::AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap());
    let bus = AsyncEventBus::from_spi(ProviderId::new("local").unwrap(), spi.clone());
    let mut receiver = block_on(spi.subscribe(spi_request(40, "close-removes", "async.local.close-removes"))).unwrap();
    block_on(receiver.close()).unwrap();

    let no_destination =
        block_on(bus.publish(PublishRequest::new(Topic::<u32>::new("async.local.close-removes").unwrap(), 1).unwrap()))
            .unwrap();
    assert!(matches!(
        no_destination.acknowledgement(),
        qubit_event_bus::model::PublishAcknowledgement::DestinationAdmissions(admissions) if admissions.is_empty()
    ));

    let new_type = spi_request_with_type(41, "new-type", "async.local.close-removes", TypeId::of::<String>());
    let mut replacement = block_on(spi.subscribe(new_type)).unwrap();
    assert!(matches!(
        block_on(replacement.receive(Duration::ZERO)).unwrap(),
        ReceiveOutcome::TimedOut
    ));
    block_on(replacement.close()).unwrap();
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn async_local_drop_does_not_leave_stale_destinations() {
    let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::new())).unwrap();
    let topic = Topic::<u32>::new("async.local.stale-destinations").unwrap();
    for index in 0..100 {
        let subscriber = format!("consumer-{index}");
        let subscription = block_on(bus.subscribe(SubscribeRequest::new(&subscriber, topic.clone()).unwrap())).unwrap();
        drop(subscription);
    }

    let receipt = block_on(bus.publish(PublishRequest::new(topic, 1).unwrap())).unwrap();
    assert!(matches!(
        receipt.acknowledgement(),
        qubit_event_bus::model::PublishAcknowledgement::DestinationAdmissions(admissions) if admissions.is_empty()
    ));
    block_on(bus.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn async_local_drop_discards_an_unsettled_in_flight_delivery() {
    let spi = Arc::new(qubit_event_bus::local::AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap());
    let bus = AsyncEventBus::from_spi(ProviderId::new("local").unwrap(), spi.clone());
    let mut first = block_on(spi.subscribe(spi_request(10, "in-flight-recovery", "async.local.requeue"))).unwrap();
    block_on(
        bus.publish(
            PublishRequest::new(
                Topic::<String>::new("async.local.requeue").unwrap(),
                "redeliver".to_owned(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let ReceiveOutcome::Message(mut first_message) = block_on(first.receive(Duration::MAX)).unwrap() else {
        panic!("message should be delivered");
    };
    assert!(first_message.take_settlement().is_some());
    drop(first);

    let mut second = block_on(spi.subscribe(spi_request(11, "in-flight-recovery", "async.local.requeue"))).unwrap();
    assert!(matches!(
        block_on(second.receive(Duration::ZERO)).unwrap(),
        ReceiveOutcome::TimedOut
    ));
    block_on(second.close()).unwrap();
}

#[test]
fn async_local_settle_and_close_can_be_retried_after_unpolled_future_drop() {
    let spi = Arc::new(qubit_event_bus::local::AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap());
    let bus = AsyncEventBus::from_spi(ProviderId::new("local").unwrap(), spi.clone());
    let mut receiver = block_on(spi.subscribe(spi_request(35, "cancelled-ops", "async.local.cancelled-ops"))).unwrap();
    block_on(
        bus.publish(
            PublishRequest::new(
                Topic::<String>::new("async.local.cancelled-ops").unwrap(),
                "settle after retry".to_owned(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let ReceiveOutcome::Message(mut message) = block_on(receiver.receive(Duration::ZERO)).unwrap() else {
        panic!("message should be delivered");
    };
    let token = message
        .take_settlement()
        .expect("local delivery has a settlement token");

    drop(receiver.settle(&token, DeliveryDisposition::Accept));
    block_on(receiver.settle(&token, DeliveryDisposition::Accept)).unwrap();
    drop(receiver.close());
    block_on(receiver.close()).unwrap();
    assert!(matches!(
        block_on(receiver.receive(Duration::ZERO)).unwrap(),
        ReceiveOutcome::Closed
    ));
    block_on(spi.shutdown(ShutdownMode::Immediate)).unwrap();
}

#[test]
fn async_local_shutdown_wakes_pending_receives_and_cancelled_shutdown_can_retry() {
    let spi = Arc::new(qubit_event_bus::local::AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap());
    let mut receiver = block_on(spi.subscribe(spi_request(20, "shutdown-waiter", "async.local.cancel"))).unwrap();
    let mut receive = Box::pin(receiver.receive(Duration::MAX));
    assert!(support::manual_async::poll_once(receive.as_mut()).is_pending());
    block_on(spi.shutdown(ShutdownMode::Immediate)).unwrap();
    assert!(matches!(
        support::manual_async::poll_once(receive.as_mut()),
        std::task::Poll::Ready(Ok(ReceiveOutcome::Closed))
    ));
    drop(receive);

    let spi = Arc::new(qubit_event_bus::local::AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap());
    let bus = AsyncEventBus::from_spi(ProviderId::new("local").unwrap(), spi.clone());
    let _receiver = block_on(spi.subscribe(spi_request(21, "graceful-waiter", "async.local.cancel"))).unwrap();
    block_on(
        bus.publish(
            PublishRequest::new(
                Topic::<String>::new("async.local.cancel").unwrap(),
                "pending".to_owned(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    let mut graceful = Box::pin(spi.shutdown(ShutdownMode::Graceful { timeout: Duration::MAX }));
    assert!(support::manual_async::poll_once(graceful.as_mut()).is_pending());
    drop(graceful);
    assert!(matches!(
        block_on(spi.shutdown(ShutdownMode::Immediate)).unwrap(),
        qubit_event_bus::spi::ShutdownOutcome::Complete
    ));
}

#[test]
fn async_local_graceful_shutdown_observes_finite_timeout() {
    let spi = Arc::new(qubit_event_bus::local::AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap());
    let bus = AsyncEventBus::from_spi(ProviderId::new("local").unwrap(), spi.clone());
    let _receiver = block_on(spi.subscribe(spi_request(34, "finite-shutdown", "async.local.finite"))).unwrap();
    block_on(
        bus.publish(
            PublishRequest::new(
                Topic::<String>::new("async.local.finite").unwrap(),
                "pending".to_owned(),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    assert!(matches!(
        block_on(spi.shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_millis(1),
        }))
        .unwrap(),
        qubit_event_bus::spi::ShutdownOutcome::TimedOut
    ));
}

/// Builds a local SPI request for a native `String` payload.
fn spi_request(id: u64, subscriber: &str, topic: &str) -> SpiSubscriptionRequest {
    spi_request_with_type(id, subscriber, topic, TypeId::of::<String>())
}

/// Builds a local SPI request with an explicit native payload type identity.
fn spi_request_with_type(id: u64, subscriber: &str, topic: &str, payload_type_id: TypeId) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        qubit_id::Id::new(id),
        TopicAddress::new(topic).unwrap(),
        SubscriberId::new(subscriber).unwrap(),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::default(),
        payload_type_id,
    )
}

fn process_thread_count() -> Option<usize> {
    #[cfg(target_os = "linux")]
    {
        Some(std::fs::read_dir("/proc/self/task").ok()?.count())
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(feature = "conformance")]
#[test]
fn async_local_passes_public_spi_conformance_publish_cases() {
    let report = block_on(qubit_event_bus::spi::conformance::run_async(
        || async {
            Arc::new(qubit_event_bus::local::AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap())
                as Arc<dyn AsyncEventBusSpi>
        },
        &qubit_event_bus::spi::conformance::ConformanceHooks::default(),
    ));
    assert!(report.all_passed());
    report.assert_all_passed();
}
