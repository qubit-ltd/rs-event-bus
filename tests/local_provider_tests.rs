// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Contract tests for the built-in synchronous local SPI provider.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::EventBus;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::facade::SyncDeliverySchedulerConfig;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::model::Topic;
use qubit_event_bus::pipeline::Diagnostic;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventBusSpi;
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
use qubit_spi::ProviderMetadata;
use qubit_spi::ProviderSelection;
use qubit_spi::ProviderSelector;
use qubit_spi::ServiceProvider;

fn provider() -> LocalEventBusProvider {
    LocalEventBusProvider
}

fn outbound(topic: &str, value: u32) -> OutboundMessage {
    outbound_with_delay(topic, value, None)
}

fn outbound_with_delay(topic: &str, value: u32, delay: Option<Duration>) -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new(topic).unwrap(),
        EventId::new(format!("event-{value}")).unwrap(),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        None,
        delay,
        TransportPayload::Native(Arc::new(value)),
    )
}

fn outbound_with_key_and_delay(topic: &str, value: u32, key: &str, delay: Option<Duration>) -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new(topic).unwrap(),
        EventId::new(format!("event-{value}")).unwrap(),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        Some(OrderingKey::new(key).unwrap()),
        delay,
        TransportPayload::Native(Arc::new(value)),
    )
}

fn request(id: u64, topic: &str) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        TopicAddress::new(topic).unwrap(),
        SubscriberId::new(format!("subscriber-{id}")).unwrap(),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::new(),
    )
}

fn create(config: &LocalEventBusConfig) -> Arc<dyn EventBusSpi> {
    let config = EventBusConfig::default().with_provider_options(config.provider_options());
    provider().create_configured(&config).unwrap()
}

#[test]
fn provider_registers_local_identity_and_supported_aliases() {
    let descriptor = provider().descriptor();

    assert_eq!("local", descriptor.id().as_str());
    assert_eq!(
        ["memory", "in-process"],
        descriptor
            .aliases()
            .iter()
            .map(ProviderSelector::as_str)
            .collect::<Vec<_>>()
            .as_slice()
    );
}

#[test]
fn facade_and_registry_local_entries_share_the_registered_provider_path() {
    let local = EventBus::local(LocalEventBusConfig::new().queue_capacity(8)).unwrap();
    let local_receipt = local
        .publish(PublishRequest::new(Topic::<u32>::new("local.events").unwrap(), 1).unwrap())
        .unwrap();
    assert_eq!("local", local_receipt.provider_id().as_str());
    local.shutdown(ShutdownMode::Immediate).unwrap();

    let registry = EventBusRegistry::with_local().unwrap();
    let config = EventBusConfig::default().with_selection(ProviderSelection::named("memory").unwrap());
    let from_registry = registry.create(&config).unwrap();
    let registry_receipt = from_registry
        .publish(PublishRequest::new(Topic::<u32>::new("local.events").unwrap(), 2).unwrap())
        .unwrap();
    assert_eq!("local", registry_receipt.provider_id().as_str());
    from_registry.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn local_facade_delivers_owned_string_payload_without_a_clone_bound() {
    let bus = EventBus::local(LocalEventBusConfig::default()).unwrap();
    let topic = Topic::<String>::new("local.strings").unwrap();
    let (delivered_tx, delivered_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new(SubscriberId::new("string-subscriber").unwrap(), topic.clone()),
            move |delivery| delivered_tx.send(delivery.payload().to_owned()).unwrap(),
        )
        .unwrap();

    bus.publish(PublishRequest::new(topic, String::from("owned payload")).unwrap())
        .unwrap();
    assert_eq!(
        "owned payload",
        delivered_rx.recv_timeout(Duration::from_secs(2)).unwrap()
    );
    subscription.cancel().unwrap();
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn local_facade_reports_rejected_admission_in_receipt_and_diagnostic() {
    let facade = qubit_event_bus::EventBusFacadeConfig::new()
        .with_sync_delivery_scheduler(SyncDeliverySchedulerConfig::new(1, 0).unwrap());
    let registry = EventBusRegistry::with_local().unwrap();
    let config = EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::new().queue_capacity(1).provider_options())
        .with_facade_config(facade);
    let bus = registry.create(&config).unwrap();
    let topic = Topic::<u32>::new("local.admission").unwrap();
    let (handler_entered_tx, handler_entered_rx) = mpsc::channel();
    let (handler_release_tx, handler_release_rx) = mpsc::channel();
    let handler_release_rx = Mutex::new(handler_release_rx);
    let subscription = bus
        .subscribe(
            SubscribeRequest::new(SubscriberId::new("blocked-subscriber").unwrap(), topic.clone()),
            move |_| {
                handler_entered_tx.send(()).unwrap();
                handler_release_rx.lock().unwrap().recv().unwrap();
            },
        )
        .unwrap();
    let (diagnostic_tx, diagnostic_rx) = mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::AdmissionRejected {
            event_id,
            subscriber_id,
            reason,
            ..
        } = diagnostic
        {
            diagnostic_tx
                .send((event_id.clone(), subscriber_id.clone(), reason.clone()))
                .unwrap();
        }
    });

    bus.publish(PublishRequest::new(topic.clone(), 1).unwrap()).unwrap();
    handler_entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let rejected = (2..=8)
        .map(|value| bus.publish(PublishRequest::new(topic.clone(), value).unwrap()).unwrap())
        .find(|receipt| {
            matches!(receipt.acknowledgement(), PublishAcknowledgement::DestinationAdmissions(admissions)
            if admissions.iter().any(|admission| matches!(
                admission.status(),
                AdmissionStatus::Rejected(reason) if reason.as_ref() == "subscription queue is full"
            )))
        })
        .expect("bounded local/provider and facade buffers eventually reject a destination");
    let (event_id, subscriber_id, reason) = diagnostic_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(event_id, *rejected.input_event_id());
    assert_eq!(subscriber_id.as_str(), "blocked-subscriber");
    assert_eq!(reason.as_ref(), "subscription queue is full");

    handler_release_tx.send(()).unwrap();
    subscription.cancel().unwrap();
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn local_provider_admits_only_matching_topic_subscriptions_and_reports_capacity_rejection() {
    let config = LocalEventBusConfig::new().queue_capacity(1);
    let spi = create(&config);
    let mut first = spi.subscribe(request(1, "orders.created")).unwrap();
    let _other_topic = spi.subscribe(request(2, "orders.cancelled")).unwrap();

    let first_admission = spi.publish(outbound("orders.created", 1)).unwrap();
    let PublishAcknowledgement::DestinationAdmissions(first_admission) = first_admission else {
        panic!("local provider reports per-destination admission");
    };
    assert_eq!(1, first_admission.len());
    assert!(matches!(
        first_admission[0].status(),
        qubit_event_bus::model::AdmissionStatus::Accepted
    ));

    let second_admission = spi.publish(outbound("orders.created", 2)).unwrap();
    let PublishAcknowledgement::DestinationAdmissions(second_admission) = second_admission else {
        panic!("local provider reports per-destination admission");
    };
    assert_eq!(1, second_admission.len());
    assert!(matches!(
        second_admission[0].status(),
        qubit_event_bus::model::AdmissionStatus::Rejected(_)
    ));
    assert!(matches!(
        first.receive(Duration::ZERO).unwrap(),
        ReceiveOutcome::Message(_)
    ));

    let no_subscribers = spi.publish(outbound("orders.unknown", 3)).unwrap();
    assert!(
        matches!(no_subscribers, PublishAcknowledgement::DestinationAdmissions(destinations) if destinations.is_empty())
    );
}

/// Verifies that the local facade serializes deliveries with a shared key in
/// enqueue order.
#[test]
fn local_facade_serializes_same_ordering_key_and_preserves_enqueue_order() {
    let bus = EventBus::local(LocalEventBusConfig::new().queue_capacity(8)).unwrap();
    let topic = Topic::<u32>::new("local.ordered").unwrap();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let max_active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (first_started_tx, first_started_rx) = mpsc::channel();
    let (release_first_tx, release_first_rx) = mpsc::channel();
    let (handler_done_tx, handler_done_rx) = mpsc::channel();
    let release_first_rx = Mutex::new(release_first_rx);
    let observed_by_handler = observed.clone();
    let active_by_handler = active.clone();
    let maximum_by_handler = max_active.clone();
    let done_by_handler = handler_done_tx.clone();
    let options = qubit_event_bus::model::SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new(SubscriberId::new("ordered-local").unwrap(), topic.clone()).with_options(options),
            move |delivery: qubit_event_bus::model::Delivery<u32>| {
                let current = active_by_handler.fetch_add(1, std::sync::atomic::Ordering::AcqRel) + 1;
                maximum_by_handler.fetch_max(current, std::sync::atomic::Ordering::AcqRel);
                observed_by_handler.lock().unwrap().push(*delivery.payload());
                if *delivery.payload() == 1 {
                    first_started_tx.send(()).unwrap();
                    release_first_rx.lock().unwrap().recv().unwrap();
                }
                done_by_handler.send(*delivery.payload()).unwrap();
                active_by_handler.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            },
        )
        .unwrap();

    for value in [1, 2] {
        let request = PublishRequest::builder()
            .topic(topic.clone())
            .payload(value)
            .ordering_key("customer-1")
            .build()
            .unwrap();
        bus.publish(request).unwrap();
        if value == 1 {
            first_started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        }
    }
    release_first_tx.send(()).unwrap();
    assert_eq!(handler_done_rx.recv_timeout(Duration::from_secs(2)).unwrap(), 1);
    assert_eq!(handler_done_rx.recv_timeout(Duration::from_secs(2)).unwrap(), 2);
    bus.wait_for_idle(&topic, Some(Duration::from_secs(2))).unwrap();

    assert_eq!(*observed.lock().unwrap(), [1, 2]);
    assert_eq!(max_active.load(std::sync::atomic::Ordering::Acquire), 1);
    subscription.cancel().unwrap();
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn local_facade_allows_a_different_ordering_key_to_progress_while_one_handler_is_blocked() {
    let bus = EventBus::local(LocalEventBusConfig::new().queue_capacity(4)).unwrap();
    let topic = Topic::<u32>::new("local.cross-key").unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let (done_tx, done_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new(SubscriberId::new("cross-key").unwrap(), topic.clone()).with_options(
                qubit_event_bus::model::SubscribeOptions::builder()
                    .ordering_policy(OrderingPolicy::PerKey)
                    .build(),
            ),
            move |delivery: qubit_event_bus::model::Delivery<u32>| {
                if *delivery.payload() == 1 {
                    started_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                }
                done_tx.send(*delivery.payload()).unwrap();
            },
        )
        .unwrap();
    let publish = |value, key| {
        PublishRequest::builder()
            .topic(topic.clone())
            .payload(value)
            .ordering_key(key)
            .build()
            .unwrap()
    };
    bus.publish(publish(1, "key-a")).unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    bus.publish(publish(2, "key-a")).unwrap();
    bus.publish(publish(3, "key-b")).unwrap();

    let progressed = done_rx.recv_timeout(Duration::from_millis(150)).ok();
    let _ = release_tx.send(());
    let mut completed = vec![done_rx.recv_timeout(Duration::from_secs(2)).unwrap()];
    completed.push(done_rx.recv_timeout(Duration::from_secs(2)).unwrap());
    bus.wait_for_idle(&topic, Some(Duration::from_secs(2))).unwrap();
    subscription.cancel().unwrap();
    bus.shutdown(ShutdownMode::Immediate).unwrap();

    assert_eq!(progressed, Some(3), "key-b delivery must not wait behind key-a handler");
    assert!(completed.contains(&1));
    assert!(completed.contains(&2));
}

#[test]
fn local_facade_delayed_message_does_not_block_immediate_message_on_another_key() {
    let bus = EventBus::local(LocalEventBusConfig::new().queue_capacity(2)).unwrap();
    let topic = Topic::<u32>::new("local.delay-cross-key").unwrap();
    let (done_tx, done_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new(SubscriberId::new("delay-cross-key").unwrap(), topic.clone()).with_options(
                qubit_event_bus::model::SubscribeOptions::builder()
                    .ordering_policy(OrderingPolicy::PerKey)
                    .build(),
            ),
            move |delivery: qubit_event_bus::model::Delivery<u32>| {
                done_tx.send(*delivery.payload()).unwrap();
            },
        )
        .unwrap();
    let delayed = PublishRequest::builder()
        .topic(topic.clone())
        .payload(1)
        .ordering_key("key-a")
        .delay(Duration::from_secs(3))
        .build()
        .unwrap();
    let immediate = PublishRequest::builder()
        .topic(topic.clone())
        .payload(2)
        .ordering_key("key-b")
        .build()
        .unwrap();
    bus.publish(delayed).unwrap();
    let immediate_receipt = bus.publish(immediate).unwrap();
    let progressed = done_rx.recv_timeout(Duration::from_millis(150)).ok();
    subscription.cancel().unwrap();
    bus.shutdown(ShutdownMode::Immediate).unwrap();

    assert_eq!(progressed, Some(2), "a not-yet-due key-a message must not block key-b");
    assert!(
        matches!(immediate_receipt.acknowledgement(), PublishAcknowledgement::DestinationAdmissions(items)
        if matches!(items.as_slice(), [item] if matches!(item.status(), AdmissionStatus::Accepted)))
    );
}

#[test]
fn local_subscription_redelivers_retry_and_settlement_is_idempotent() {
    let spi = create(&LocalEventBusConfig::default());
    let mut subscription = spi.subscribe(request(11, "events")).unwrap();
    spi.publish(outbound("events", 7)).unwrap();

    let ReceiveOutcome::Message(mut message) = subscription.receive(Duration::ZERO).unwrap() else {
        panic!("published message is available");
    };
    let token = message.take_settlement().unwrap();
    subscription.settle(&token, DeliveryDisposition::Retry).unwrap();
    subscription.settle(&token, DeliveryDisposition::Retry).unwrap();
    let conflict = subscription.settle(&token, DeliveryDisposition::Accept).unwrap_err();
    assert_eq!("invalid_settlement_token", conflict.kind());

    let ReceiveOutcome::Message(mut redelivery) = subscription.receive(Duration::ZERO).unwrap() else {
        panic!("retry disposition requeues the same event");
    };
    assert_eq!("event-7", redelivery.id().as_str());
    let redelivery_token = redelivery.take_settlement().unwrap();
    subscription
        .settle(&redelivery_token, DeliveryDisposition::Accept)
        .unwrap();
    subscription
        .settle(&redelivery_token, DeliveryDisposition::Accept)
        .unwrap();
    assert_eq!(
        "invalid_settlement_token",
        subscription
            .settle(&redelivery_token, DeliveryDisposition::Retry)
            .unwrap_err()
            .kind()
    );
    assert!(matches!(
        subscription.receive(Duration::ZERO).unwrap(),
        ReceiveOutcome::TimedOut
    ));
}

#[test]
fn local_subscription_defers_retry_when_regular_queue_is_full_without_losing_same_key_order() {
    let spi = create(&LocalEventBusConfig::new().queue_capacity(1));
    let mut subscription = spi.subscribe(request(41, "events")).unwrap();
    spi.publish(outbound_with_key_and_delay("events", 1, "orders", None))
        .unwrap();

    let ReceiveOutcome::Message(mut first) = subscription.receive(Duration::ZERO).unwrap() else {
        panic!("first event is available");
    };
    let first_token = first.take_settlement().expect("first event requires settlement");
    spi.publish(outbound_with_key_and_delay("events", 2, "orders", None))
        .unwrap();
    subscription
        .settle(&first_token, DeliveryDisposition::Retry)
        .expect("full regular queue defers the retry without losing it");

    let ReceiveOutcome::Message(mut redelivery) = subscription.receive(Duration::ZERO).unwrap() else {
        panic!("deferred retry is immediately available");
    };
    assert_eq!("event-1", redelivery.id().as_str());
    subscription
        .settle(&redelivery.take_settlement().unwrap(), DeliveryDisposition::Accept)
        .unwrap();
    let ReceiveOutcome::Message(second) = subscription.receive(Duration::ZERO).unwrap() else {
        panic!("same-key successor follows its retried predecessor");
    };
    assert_eq!("event-2", second.id().as_str());
}

#[test]
fn local_native_delay_hides_the_message_until_its_deadline() {
    let spi = create(&LocalEventBusConfig::default());
    assert_eq!(DelayedDeliveryCapability::Native, spi.capabilities().delayed_delivery());
    let mut subscription = spi.subscribe(request(16, "events")).unwrap();
    spi.publish(outbound_with_delay("events", 9, Some(Duration::from_millis(25))))
        .unwrap();
    spi.publish(outbound("events", 10)).unwrap();

    assert!(matches!(
        subscription.receive(Duration::ZERO).unwrap(),
        ReceiveOutcome::TimedOut
    ));
    for expected in [9, 10] {
        let ReceiveOutcome::Message(message) = subscription.receive(Duration::from_secs(1)).unwrap() else {
            panic!("local queue delivers each event after the preceding delayed event");
        };
        assert_eq!(format!("event-{expected}"), message.id().as_str());
    }
}

#[test]
fn local_spi_delayed_key_does_not_block_ready_other_key_but_keeps_its_own_order() {
    let spi = create(&LocalEventBusConfig::default());
    assert_eq!(
        qubit_event_bus::spi::OrderingCapability::PerKey,
        spi.capabilities().ordering()
    );
    let mut subscription = spi.subscribe(request(17, "events")).unwrap();
    spi.publish(outbound_with_key_and_delay(
        "events",
        20,
        "key-a",
        Some(Duration::from_millis(120)),
    ))
    .unwrap();
    spi.publish(outbound_with_key_and_delay("events", 30, "key-b", None))
        .unwrap();
    spi.publish(outbound_with_key_and_delay("events", 21, "key-a", None))
        .unwrap();

    let ReceiveOutcome::Message(other_key) = subscription.receive(Duration::ZERO).unwrap() else {
        panic!("ready key-b message can pass a delayed key-a message");
    };
    assert_eq!("event-30", other_key.id().as_str());
    assert!(matches!(
        subscription.receive(Duration::from_millis(20)).unwrap(),
        ReceiveOutcome::TimedOut
    ));
    for expected in [20, 21] {
        let ReceiveOutcome::Message(message) = subscription.receive(Duration::from_secs(1)).unwrap() else {
            panic!("key-a successor waits for its delayed predecessor");
        };
        assert_eq!(format!("event-{expected}"), message.id().as_str());
    }
}

#[test]
fn local_subscription_timeout_close_and_bus_shutdown_are_stable() {
    let spi = create(&LocalEventBusConfig::default());
    let mut subscription = spi.subscribe(request(21, "events")).unwrap();
    assert!(matches!(
        subscription.receive(Duration::ZERO).unwrap(),
        ReceiveOutcome::TimedOut
    ));
    assert!(matches!(
        subscription.receive(Duration::from_millis(10)).unwrap(),
        ReceiveOutcome::TimedOut
    ));

    subscription.close().unwrap();
    assert!(matches!(
        subscription.receive(Duration::ZERO).unwrap(),
        ReceiveOutcome::Closed
    ));
    let mode = ShutdownMode::Graceful {
        timeout: Duration::ZERO,
    };
    assert_eq!(ShutdownOutcome::Complete, spi.shutdown(mode).unwrap());
    assert_eq!(ShutdownOutcome::Complete, spi.shutdown(mode).unwrap());
    assert_eq!(
        "provider_closed",
        spi.publish(outbound("events", 1)).unwrap_err().kind()
    );
}

#[test]
fn immediate_shutdown_discards_pending_messages_and_closes_receivers() {
    let spi = create(&LocalEventBusConfig::default());
    let mut subscription = spi.subscribe(request(24, "events")).unwrap();
    spi.publish(outbound("events", 4)).unwrap();

    assert_eq!(
        ShutdownOutcome::Complete,
        spi.shutdown(ShutdownMode::Immediate).unwrap()
    );
    assert!(matches!(
        subscription.receive(Duration::ZERO).unwrap(),
        ReceiveOutcome::Closed
    ));
    assert_eq!(
        ShutdownOutcome::Complete,
        spi.shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(1)
        })
        .unwrap()
    );
}

#[test]
fn graceful_shutdown_reports_when_queued_delivery_cannot_be_drained() {
    let spi = create(&LocalEventBusConfig::default());
    let mut subscription = spi.subscribe(request(26, "events")).unwrap();
    spi.publish(outbound("events", 2)).unwrap();

    let result = spi
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_millis(1),
        })
        .unwrap();
    assert_eq!(ShutdownOutcome::TimedOut, result);
    assert!(matches!(
        subscription.receive(Duration::ZERO).unwrap(),
        ReceiveOutcome::Closed
    ));
    assert_eq!(result, spi.shutdown(ShutdownMode::Immediate).unwrap());
}

#[test]
fn graceful_shutdown_waits_for_in_flight_settlement() {
    let spi = create(&LocalEventBusConfig::default());
    let mut subscription = spi.subscribe(request(27, "events")).unwrap();
    spi.publish(outbound("events", 3)).unwrap();
    let shutdown_spi = spi.clone();
    let shutdown = std::thread::spawn(move || {
        shutdown_spi.shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(1),
        })
    });

    let ReceiveOutcome::Message(mut message) = subscription.receive(Duration::from_secs(1)).unwrap() else {
        panic!("graceful shutdown keeps an in-flight delivery available for settlement");
    };
    subscription
        .settle(&message.take_settlement().unwrap(), DeliveryDisposition::Accept)
        .unwrap();
    assert_eq!(ShutdownOutcome::Complete, shutdown.join().unwrap().unwrap());
}

#[test]
fn local_config_rejects_zero_capacity_and_provider_options() {
    assert!(LocalEventBusConfig::new().queue_capacity(0).validate().is_err());

    let invalid =
        EventBusConfig::default().with_provider_options([("local.queue_capacity".to_owned(), "0".to_owned())].into());
    assert!(provider().create_configured(&invalid).is_err());
}

#[test]
fn settlement_rejects_token_from_another_subscription() {
    let spi = create(&LocalEventBusConfig::default());
    let mut subscription = spi.subscribe(request(31, "events")).unwrap();
    let foreign = SettlementToken::new(Id::new(32), "event-x".to_owned());
    let error: SpiError = subscription.settle(&foreign, DeliveryDisposition::Accept).unwrap_err();
    assert_eq!("invalid_settlement_token", error.kind());
}

#[test]
fn settlement_rejects_forged_provider_state_without_losing_real_token() {
    let spi = create(&LocalEventBusConfig::default());
    let mut subscription = spi.subscribe(request(42, "events")).unwrap();
    spi.publish(outbound("events", 9)).unwrap();
    let ReceiveOutcome::Message(mut message) = subscription.receive(Duration::ZERO).unwrap() else {
        panic!("published message is available");
    };
    let token = message.take_settlement().expect("message requires settlement");
    let forged = SettlementToken::new(Id::new(42), "forged state".to_owned());

    let error = subscription
        .settle(&forged, DeliveryDisposition::Accept)
        .expect_err("a matching subscription ID does not validate forged provider state");
    assert_eq!("invalid_settlement_token", error.kind());
    subscription.settle(&token, DeliveryDisposition::Accept).unwrap();
}
