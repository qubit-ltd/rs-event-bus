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
use qubit_event_bus::spi::SubscriptionModes;
use qubit_event_bus::spi::TransportPayload;

mod support;

use support::isolated_process;

#[test]
fn test_notification_publisher_bounds_queue_and_drains_in_order_on_close() {
    let spi = Arc::new(GatedSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi.clone())
        .expect("valid provider capabilities");
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
fn test_notification_publisher_close_is_idempotent_and_does_not_close_bus() {
    let spi = Arc::new(GatedSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi.clone())
        .expect("valid provider capabilities");
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
fn test_notification_publisher_close_with_timeout_can_be_retried_after_provider_unblocks() {
    let spi = Arc::new(GatedSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi.clone())
        .expect("valid provider capabilities");
    let publisher = NotificationPublisher::new(
        bus,
        Topic::<String>::new_static("notification.close-timeout"),
        std::num::NonZeroUsize::new(1).unwrap(),
        |_| {},
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
    let error = publisher
        .close_with_timeout(Duration::from_millis(20))
        .expect_err("blocked provider work exceeds the close deadline");
    assert_eq!(std::io::ErrorKind::TimedOut, error.kind());
    assert!(matches!(
        publisher.try_publish("after-timeout".into()),
        Err(TryPublishError::Closed(value)) if value == "after-timeout"
    ));

    spi.release();
    publisher
        .close_with_timeout(Duration::from_secs(2))
        .expect("a later close waits for the accepted notification to drain");
    assert_eq!(vec!["first"], *spi.published.lock().unwrap());
    assert_eq!(0, publisher.stats().worker_panicked());
}

#[test]
fn test_notification_publisher_zero_timeout_is_nonblocking_and_finished_close_succeeds() {
    let spi = Arc::new(GatedSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi.clone())
        .expect("valid provider capabilities");
    let publisher = NotificationPublisher::new(
        bus,
        Topic::<String>::new_static("notification.close-zero-timeout"),
        std::num::NonZeroUsize::new(1).unwrap(),
        |_| {},
    )
    .unwrap();

    publisher.try_publish("blocked".into()).unwrap();
    assert_eq!(
        "blocked",
        spi.entered
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .expect("worker begins the publish")
    );
    let error = publisher
        .close_with_timeout(Duration::ZERO)
        .expect_err("zero timeout must not wait for a blocked provider");
    assert_eq!(std::io::ErrorKind::TimedOut, error.kind());
    spi.release();
    publisher.close().expect("unbounded close drains the accepted item");
    publisher
        .close_with_timeout(Duration::ZERO)
        .expect("an already finished worker closes immediately");
}

#[test]
fn test_notification_publisher_concurrent_timed_close_callers_can_timeout_and_retry() {
    let spi = Arc::new(GatedSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi.clone())
        .expect("valid provider capabilities");
    let publisher = Arc::new(
        NotificationPublisher::new(
            bus,
            Topic::<String>::new_static("notification.concurrent-close-timeout"),
            std::num::NonZeroUsize::new(1).unwrap(),
            |_| {},
        )
        .unwrap(),
    );
    publisher.try_publish("blocked".into()).unwrap();
    assert_eq!(
        "blocked",
        spi.entered
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(2))
            .expect("worker begins the publish")
    );

    let (first_started_sender, first_started_receiver) = mpsc::channel();
    let first_publisher = Arc::clone(&publisher);
    let first = std::thread::spawn(move || {
        first_started_sender.send(()).unwrap();
        first_publisher.close_with_timeout(Duration::from_millis(20))
    });
    let (second_started_sender, second_started_receiver) = mpsc::channel();
    let second_publisher = Arc::clone(&publisher);
    let second = std::thread::spawn(move || {
        second_started_sender.send(()).unwrap();
        second_publisher.close_with_timeout(Duration::from_secs(2))
    });
    first_started_receiver.recv_timeout(Duration::from_secs(1)).unwrap();
    second_started_receiver.recv_timeout(Duration::from_secs(1)).unwrap();

    let error = first.join().unwrap().expect_err("short close caller times out");
    assert_eq!(std::io::ErrorKind::TimedOut, error.kind());
    spi.release();
    second.join().unwrap().expect("long close caller observes completion");
    assert_eq!(vec!["blocked"], *spi.published.lock().unwrap());
}

#[test]
fn test_notification_stats_snapshot_exposes_all_counters() {
    assert_eq!(256, NotificationPublisher::<String>::default_capacity().get());
    let spi = Arc::new(GatedSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi.clone())
        .expect("valid provider capabilities");
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
fn test_notification_publisher_concurrent_close_callers_share_worker_completion() {
    let spi = Arc::new(GatedSpi::new());
    let bus =
        EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi).expect("valid provider capabilities");
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
fn test_notification_publisher_contains_observer_panics_and_continues() {
    let spi = Arc::new(GatedSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi.clone())
        .expect("valid provider capabilities");
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
fn test_notification_observer_cannot_close_its_own_worker() {
    let spi = Arc::new(GatedSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("notification-test").unwrap(), spi.clone())
        .expect("valid provider capabilities");
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
                let results = [
                    publisher.close().map_err(|error| (error.kind(), error.to_string())),
                    publisher
                        .close_with_timeout(Duration::from_secs(1))
                        .map_err(|error| (error.kind(), error.to_string())),
                ];
                close_sender.send(results).expect("test receives close results");
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
        let results = close_receiver
            .recv_timeout(Duration::from_secs(2))
            .expect("observer close calls return without blocking its worker");
        for result in results {
            let (kind, message) = result.expect_err("worker-thread close must be rejected");
            assert_eq!(std::io::ErrorKind::Other, kind);
            assert_eq!("notification publisher cannot close from its worker thread", message);
        }
    }

    publisher
        .try_publish("third".into())
        .expect("worker-thread close attempts must leave admission open");
    publisher.close().expect("external close drains queued notifications");
    assert_eq!(vec!["first", "second", "third"], *spi.published.lock().unwrap());
}

/// Regression for a destructor panic after an idle worker drains its queue.
#[test]
fn test_notification_observer_drop_panic_releases_all_close_callers() {
    let test_name = "test_notification_observer_drop_panic_releases_all_close_callers";
    if std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() != Ok(test_name) {
        isolated_process::run_case_with_timeout(test_name, test_name, Duration::from_secs(5));
        return;
    }
    let bus = EventBus::local(Default::default()).expect("local bus starts");
    let captured = PanicOnDrop;
    let publisher = Arc::new(
        NotificationPublisher::new(
            bus,
            Topic::<String>::new_static("notification.drop-panic"),
            std::num::NonZeroUsize::new(1).expect("nonzero capacity"),
            move |_| {
                std::hint::black_box(&captured);
            },
        )
        .expect("publisher starts"),
    );
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let closers = (0..2)
        .map(|_| {
            let publisher = Arc::clone(&publisher);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                publisher
                    .close_with_timeout(Duration::from_secs(1))
                    .map_err(|error| (error.kind(), error.to_string()))
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    for closer in closers {
        assert_eq!(
            Err((
                std::io::ErrorKind::Other,
                "notification publisher worker panicked".into()
            )),
            closer.join().expect("close caller returns"),
        );
    }
    assert_eq!(1, publisher.stats().worker_panicked());
    assert_eq!(0, publisher.stats().observer_panicked());
    assert_eq!(
        std::io::ErrorKind::Other,
        publisher
            .close()
            .expect_err("repeated close preserves panic outcome")
            .kind()
    );
    assert_eq!(
        std::io::ErrorKind::Other,
        publisher
            .close_with_timeout(Duration::ZERO)
            .expect_err("finished panic beats zero timeout")
            .kind()
    );
}

/// Cleanup remains in flight until its destructor returns or panics.
#[test]
fn test_notification_cleanup_timeout_can_retry_and_preserves_worker_identity() {
    let test_name = "test_notification_cleanup_timeout_can_retry_and_preserves_worker_identity";
    if std::env::var("QUBIT_EVENT_BUS_ISOLATED_CASE").as_deref() != Ok(test_name) {
        isolated_process::run_case_with_timeout(test_name, test_name, Duration::from_secs(5));
        return;
    }
    let publisher_ref = Arc::new(OnceLock::<Weak<NotificationPublisher<String>>>::new());
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let captured = GatedPanicOnDrop {
        publisher: Arc::clone(&publisher_ref),
        entered: entered_sender,
        release: Mutex::new(release_receiver),
    };
    let bus = EventBus::local(Default::default()).expect("local bus starts");
    let publisher = Arc::new(
        NotificationPublisher::new(
            bus,
            Topic::<String>::new_static("notification.cleanup-timeout"),
            std::num::NonZeroUsize::new(1).expect("nonzero capacity"),
            move |_| {
                std::hint::black_box(&captured);
            },
        )
        .expect("publisher starts"),
    );
    assert!(publisher_ref.set(Arc::downgrade(&publisher)).is_ok());
    assert_eq!(
        std::io::ErrorKind::TimedOut,
        publisher
            .close_with_timeout(Duration::ZERO)
            .expect_err("cleanup has not finished")
            .kind()
    );
    let self_close_error = entered_receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("observer destructor entered");
    assert_eq!(std::io::ErrorKind::Other, self_close_error);
    assert_eq!(0, publisher.stats().worker_panicked());
    assert_eq!(
        std::io::ErrorKind::TimedOut,
        publisher
            .close_with_timeout(Duration::ZERO)
            .expect_err("blocked destructor keeps completion pending")
            .kind()
    );
    release_sender.send(()).expect("release observer destructor");
    assert_eq!(
        std::io::ErrorKind::Other,
        publisher
            .close_with_timeout(Duration::from_secs(1))
            .expect_err("cleanup panic is preserved after timeout")
            .kind()
    );
    assert_eq!(1, publisher.stats().worker_panicked());
    assert_eq!(
        std::io::ErrorKind::Other,
        publisher.close().expect_err("all retries retain panic outcome").kind()
    );
}

/// Holds cleanup until the test releases it, then panics after reentrant close.
struct GatedPanicOnDrop {
    publisher: Arc<OnceLock<Weak<NotificationPublisher<String>>>>,
    entered: mpsc::Sender<std::io::ErrorKind>,
    release: Mutex<mpsc::Receiver<()>>,
}

impl Drop for GatedPanicOnDrop {
    fn drop(&mut self) {
        let publisher = self
            .publisher
            .get()
            .and_then(Weak::upgrade)
            .expect("publisher lives while destructor runs");
        let error = publisher
            .close_with_timeout(Duration::ZERO)
            .expect_err("cleanup cannot close its own worker");
        self.entered.send(error.kind()).expect("test observes cleanup");
        self.release
            .lock()
            .expect("cleanup release mutex")
            .recv()
            .expect("test releases cleanup");
        panic!("gated observer destructor panic");
    }
}

/// Panics when the observer's captured resource is released on worker exit.
struct PanicOnDrop;

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("observer destructor panic");
    }
}

/// Blocks only the first publication until the test releases the worker.
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

    /// Opens the first-publication gate and wakes its blocked worker.
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
            SubscriptionModes::EPHEMERAL,
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
