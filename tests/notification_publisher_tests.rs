// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public contract tests for the bounded notification publisher.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::sync::Weak;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::NotificationOutcome;
use qubit_event_bus::NotificationPublisher;
use qubit_event_bus::TryPublishError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
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
use qubit_event_bus::spi::TransportPayload;

#[test]
fn notification_publisher_bounds_queue_and_drains_in_order_on_close() {
    let spi = Arc::new(GatedSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi.clone());
    let (outcome_sender, outcome_receiver) = mpsc::channel();
    let publisher = NotificationPublisher::new(
        bus,
        Topic::<String>::new_static("notification.events"),
        std::num::NonZeroUsize::new(1).unwrap(),
        move |outcome| outcome_sender.send(outcome).unwrap(),
    )
    .unwrap();

    publisher.try_publish("first".into()).unwrap();
    assert_eq!(
        "first",
        spi.entered
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .expect("worker begins the first publish")
    );
    publisher.try_publish("second".into()).unwrap();
    let full = publisher.try_publish("third".into()).unwrap_err();
    assert!(matches!(full, TryPublishError::Full(value) if value == "third"));

    spi.release();
    publisher.close().unwrap();

    let outcomes = outcome_receiver.try_iter().collect::<Vec<_>>();
    assert!(matches!(
        outcomes.as_slice(),
        [NotificationOutcome::Published(_), NotificationOutcome::Published(_)]
    ));
    assert_eq!(vec!["first", "second"], *spi.published.lock().unwrap());
    assert_eq!(1, publisher.stats().queue_full());
}

#[test]
fn notification_publisher_close_is_idempotent_and_does_not_close_bus() {
    let spi = Arc::new(GatedSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi.clone());
    let publisher = NotificationPublisher::new(
        bus,
        Topic::<String>::new_static("notification.events"),
        std::num::NonZeroUsize::new(1).unwrap(),
        |_| {},
    )
    .unwrap();

    publisher.close().unwrap();
    publisher.close().unwrap();
    assert!(matches!(publisher.try_publish("late".into()), Err(TryPublishError::Closed(value)) if value == "late"));
    assert_eq!(0, spi.shutdown_calls.load(std::sync::atomic::Ordering::Acquire));
    let error = publisher.try_publish("again".into()).unwrap_err();
    assert_eq!("notification publisher is closed", error.to_string());
    assert_eq!("Closed(..)", format!("{error:?}"));
    assert_eq!(2, publisher.stats().queue_closed());
}

#[test]
fn notification_stats_snapshot_exposes_all_counters() {
    assert_eq!(256, NotificationPublisher::<String>::default_capacity().get());
    let spi = Arc::new(GatedSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi.clone());
    let publisher = NotificationPublisher::new(
        bus,
        Topic::<String>::new_static("notification.events"),
        std::num::NonZeroUsize::new(1).unwrap(),
        |_| {},
    )
    .unwrap();
    publisher.try_publish("ok".into()).unwrap();
    spi.release();
    publisher.close().unwrap();
    let snapshot = publisher.stats();
    assert_eq!(1, snapshot.enqueued());
    assert_eq!(0, snapshot.queue_full());
    assert_eq!(0, snapshot.publish_errors());
    assert_eq!(0, snapshot.request_errors());
    assert_eq!(1, snapshot.published());
    assert_eq!(0, snapshot.observer_panicked());
    assert_eq!(0, snapshot.worker_panicked());
}

#[test]
fn notification_publisher_concurrent_close_callers_share_worker_completion() {
    let spi = Arc::new(GatedSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi);
    let publisher = Arc::new(
        NotificationPublisher::new(
            bus,
            Topic::<String>::new_static("notification.events"),
            std::num::NonZeroUsize::new(1).unwrap(),
            |_| {},
        )
        .unwrap(),
    );
    let first = {
        let publisher = publisher.clone();
        std::thread::spawn(move || publisher.close())
    };
    let second = {
        let publisher = publisher.clone();
        std::thread::spawn(move || publisher.close())
    };
    first.join().unwrap().unwrap();
    second.join().unwrap().unwrap();
}

#[test]
fn notification_publisher_contains_observer_panics_and_continues() {
    let spi = Arc::new(GatedSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi.clone());
    let publisher = NotificationPublisher::new(
        bus,
        Topic::<String>::new_static("notification.events"),
        std::num::NonZeroUsize::new(2).unwrap(),
        |_| panic!("observer panic is contained"),
    )
    .unwrap();

    publisher.try_publish("first".into()).unwrap();
    publisher.try_publish("second".into()).unwrap();
    spi.release();
    publisher.close().unwrap();

    assert_eq!(vec!["first", "second"], *spi.published.lock().unwrap());
    assert_eq!(2, publisher.stats().observer_panicked());
}

#[test]
fn notification_observer_cannot_close_its_own_worker() {
    let spi = Arc::new(GatedSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi.clone());
    let publisher_ref = Arc::new(OnceLock::<Weak<NotificationPublisher<String>>>::new());
    let callback_publisher_ref = Arc::clone(&publisher_ref);
    let (close_sender, close_receiver) = mpsc::channel();
    let publisher = Arc::new(
        NotificationPublisher::new(
            bus,
            Topic::<String>::new_static("notification.events"),
            std::num::NonZeroUsize::new(2).unwrap(),
            move |_| {
                let publisher = callback_publisher_ref
                    .get()
                    .and_then(Weak::upgrade)
                    .expect("publisher remains alive during its observer");
                let result = publisher.close().map_err(|error| (error.kind(), error.to_string()));
                close_sender.send(result).expect("test receives close result");
            },
        )
        .unwrap(),
    );
    assert!(publisher_ref.set(Arc::downgrade(&publisher)).is_ok());

    publisher.try_publish("first".into()).unwrap();
    assert_eq!(
        "first",
        spi.entered
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .expect("worker begins first publish")
    );
    publisher.try_publish("second".into()).unwrap();
    spi.release();

    for _ in 0..2 {
        let (kind, message) = close_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("observer close returns without blocking its worker")
            .expect_err("worker-thread close must be rejected");
        assert_eq!(std::io::ErrorKind::Other, kind);
        assert_eq!("notification publisher cannot close from its worker thread", message);
    }

    publisher.close().expect("external close drains queued notifications");
    assert_eq!(vec!["first", "second"], *spi.published.lock().unwrap());
}

struct GatedSpi {
    entered: Mutex<mpsc::Receiver<String>>,
    entered_sender: Mutex<Option<mpsc::Sender<String>>>,
    released: (Mutex<bool>, Condvar),
    published: Mutex<Vec<String>>,
    publish_count: std::sync::atomic::AtomicUsize,
    shutdown_calls: std::sync::atomic::AtomicUsize,
}

impl GatedSpi {
    fn new() -> Self {
        let (entered_sender, entered) = mpsc::channel();
        Self {
            entered: Mutex::new(entered),
            entered_sender: Mutex::new(Some(entered_sender)),
            released: (Mutex::new(false), Condvar::new()),
            published: Mutex::new(Vec::new()),
            publish_count: std::sync::atomic::AtomicUsize::new(0),
            shutdown_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn release(&self) {
        *self.released.0.lock().unwrap() = true;
        self.released.1.notify_all();
    }
}

impl EventBusSpi for GatedSpi {
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
            PublishVisibility::Opaque,
        )
    }

    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        let TransportPayload::Native(payload) = message.payload() else {
            panic!("notification publisher sends native payloads");
        };
        let event = payload
            .downcast_ref::<String>()
            .expect("test notification payload has the expected type")
            .clone();
        let call = self.publish_count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        if call == 0 {
            if let Some(sender) = self.entered_sender.lock().unwrap().take() {
                sender.send(event.clone()).unwrap();
            }
            let mut released = self.released.0.lock().unwrap();
            while !*released {
                released = self.released.1.wait(released).unwrap();
            }
        }
        self.published.lock().unwrap().push(event);
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        })
    }

    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Err(SpiError::Operation {
            provider_id: "notification-test".into(),
            operation: "subscribe",
            resource: None,
            kind: "unsupported",
            retryable: Some(false),
            source: Box::new(std::io::Error::other("subscriptions are unsupported")),
        })
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        self.shutdown_calls.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Ok(ShutdownOutcome::Complete)
    }
}
