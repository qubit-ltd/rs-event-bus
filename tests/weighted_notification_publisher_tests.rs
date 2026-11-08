// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::NotificationOutcome;
use qubit_event_bus::TryPublishError;
use qubit_event_bus::WeightedNotificationPublisher;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::Topic;

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test values are nonzero")
}

#[test]
fn test_cumulative_weight_is_held_through_observer_and_released_afterward() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus");
    let (observed_tx, observed_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let publisher = WeightedNotificationPublisher::new(
        bus,
        Topic::<String>::new("weighted.notifications").unwrap(),
        nz(2),
        nz(10),
        move |_| {
            observed_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            finished_tx.send(()).unwrap();
        },
    )
    .unwrap();

    publisher.try_publish("first".to_owned(), nz(6)).unwrap();
    observed_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("worker entered observer");
    let error = publisher
        .try_publish("second".to_owned(), nz(5))
        .expect_err("outstanding weight is still reserved during observer");
    assert!(matches!(error, TryPublishError::Full(payload) if payload == "second"));
    let stats = publisher.stats();
    assert_eq!(1, stats.queue_full());
    assert_eq!(0, stats.queue_capacity_rejections());
    assert_eq!(1, stats.weight_capacity_rejections());
    assert_eq!(0, stats.oversized_weight_rejections());

    release_tx.send(()).unwrap();
    finished_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("observer returned and reservation was released");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        match publisher.try_publish("third".to_owned(), nz(5)) {
            Ok(()) => break,
            Err(TryPublishError::Full(payload)) => {
                assert_eq!("third", payload);
                assert!(std::time::Instant::now() < deadline, "reservation was not released");
                thread::yield_now();
            }
            Err(TryPublishError::Closed(_)) => panic!("publisher unexpectedly closed"),
        }
    }
    release_tx.send(()).unwrap();
    publisher.close().unwrap();
}

#[test]
fn test_one_item_may_use_the_entire_weight_budget() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus");
    let publisher = WeightedNotificationPublisher::new(
        bus,
        Topic::<String>::new("weighted.boundary").unwrap(),
        nz(1),
        nz(10),
        |_| {},
    )
    .unwrap();
    let error = publisher
        .try_publish("eleven".to_owned(), nz(11))
        .expect_err("one notification cannot exceed the full budget");
    assert!(matches!(error, TryPublishError::Full(payload) if payload == "eleven"));
    let stats = publisher.stats();
    assert_eq!(1, stats.queue_full());
    assert_eq!(0, stats.queue_capacity_rejections());
    assert_eq!(0, stats.weight_capacity_rejections());
    assert_eq!(1, stats.oversized_weight_rejections());
    publisher.try_publish("ten".to_owned(), nz(10)).unwrap();
    publisher.close().unwrap();
}

#[test]
fn test_oversized_weight_and_overflow_are_rejected_without_losing_payloads() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus");
    let (observed_tx, observed_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let publisher = WeightedNotificationPublisher::new(
        bus,
        Topic::<String>::new("weighted.overflow").unwrap(),
        nz(2),
        nz(usize::MAX),
        move |_| {
            observed_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        },
    )
    .unwrap();
    publisher.try_publish("largest".to_owned(), nz(usize::MAX)).unwrap();
    observed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let error = publisher
        .try_publish("overflow".to_owned(), nz(1))
        .expect_err("checked addition rejects overflow");
    assert!(matches!(error, TryPublishError::Full(payload) if payload == "overflow"));
    let stats = publisher.stats();
    assert_eq!(1, stats.queue_full());
    assert_eq!(1, stats.weight_capacity_rejections());
    assert_eq!(0, stats.oversized_weight_rejections());
    release_tx.send(()).unwrap();
    publisher.close().unwrap();
}

#[test]
fn test_queue_capacity_and_weight_capacity_both_report_full() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus");
    let (observed_tx, observed_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let publisher = WeightedNotificationPublisher::new(
        bus,
        Topic::<String>::new("weighted.capacity").unwrap(),
        nz(1),
        nz(10),
        move |_| {
            observed_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        },
    )
    .unwrap();
    publisher.try_publish("active".to_owned(), nz(1)).unwrap();
    observed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    publisher.try_publish("queued".to_owned(), nz(1)).unwrap();
    let error = publisher
        .try_publish("queue-full".to_owned(), nz(1))
        .expect_err("the waiting queue is full");
    assert!(matches!(error, TryPublishError::Full(payload) if payload == "queue-full"));
    let stats = publisher.stats();
    assert_eq!(1, stats.queue_full());
    assert_eq!(1, stats.queue_capacity_rejections());
    assert_eq!(0, stats.weight_capacity_rejections());
    assert_eq!(0, stats.oversized_weight_rejections());
    release_tx.send(()).unwrap();
    release_tx.send(()).unwrap();
    publisher.close().unwrap();
}

#[test]
fn test_weight_rejection_has_priority_when_queue_is_also_full() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus");
    let (observed_tx, observed_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let publisher = WeightedNotificationPublisher::new(
        bus,
        Topic::<String>::new("weighted.priority").unwrap(),
        nz(1),
        nz(10),
        move |_| {
            observed_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        },
    )
    .unwrap();
    publisher.try_publish("active".to_owned(), nz(6)).unwrap();
    observed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    publisher.try_publish("queued".to_owned(), nz(4)).unwrap();
    let error = publisher
        .try_publish("both-full".to_owned(), nz(1))
        .expect_err("weight reservation is checked before the full queue");
    assert!(matches!(error, TryPublishError::Full(payload) if payload == "both-full"));
    let stats = publisher.stats();
    assert_eq!(1, stats.queue_full());
    assert_eq!(0, stats.queue_capacity_rejections());
    assert_eq!(1, stats.weight_capacity_rejections());
    assert_eq!(0, stats.oversized_weight_rejections());
    release_tx.send(()).unwrap();
    release_tx.send(()).unwrap();
    publisher.close().unwrap();
}

#[test]
fn test_closed_publisher_does_not_increment_capacity_rejection_counters() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus");
    let publisher = WeightedNotificationPublisher::new(
        bus,
        Topic::<String>::new("weighted.closed").unwrap(),
        nz(1),
        nz(10),
        |_| {},
    )
    .unwrap();
    publisher.close().unwrap();
    let before = publisher.stats();
    let error = publisher
        .try_publish("closed".to_owned(), nz(1))
        .expect_err("closed publisher rejects admission");
    assert!(matches!(error, TryPublishError::Closed(payload) if payload == "closed"));
    let after = publisher.stats();
    assert_eq!(before.queue_full(), after.queue_full());
    assert_eq!(before.queue_capacity_rejections(), after.queue_capacity_rejections());
    assert_eq!(before.weight_capacity_rejections(), after.weight_capacity_rejections());
    assert_eq!(
        before.oversized_weight_rejections(),
        after.oversized_weight_rejections()
    );
    assert_eq!(1, after.queue_closed());
}

#[test]
fn test_concurrent_reservations_never_exceed_the_cumulative_limit() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus");
    let (observed_tx, observed_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let first = Arc::new(AtomicBool::new(true));
    let first_observer = first.clone();
    let publisher = Arc::new(
        WeightedNotificationPublisher::new(
            bus,
            Topic::<String>::new("weighted.concurrent").unwrap(),
            nz(8),
            nz(10),
            move |_| {
                if first_observer.swap(false, Ordering::SeqCst) {
                    observed_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                }
            },
        )
        .unwrap(),
    );
    publisher.try_publish("worker-held".to_owned(), nz(3)).unwrap();
    observed_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    let count = 8;
    let start = Arc::new(Barrier::new(count));
    let workers: Vec<_> = (0..count)
        .map(|index| {
            let publisher = publisher.clone();
            let start = start.clone();
            thread::spawn(move || {
                start.wait();
                publisher.try_publish(format!("item-{index}"), nz(3))
            })
        })
        .collect();
    let accepted = workers
        .into_iter()
        .map(|worker| worker.join().unwrap().is_ok())
        .filter(|accepted| *accepted)
        .count();
    assert_eq!(2, accepted, "the held weight leaves room for two items");

    release_tx.send(()).unwrap();
    publisher.close().unwrap();
}

#[test]
fn test_close_returns_the_original_payload_for_late_publish() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus");
    let publisher = WeightedNotificationPublisher::new(
        bus,
        Topic::<String>::new("weighted.closed").unwrap(),
        nz(1),
        nz(10),
        |_| {},
    )
    .unwrap();
    publisher.close().unwrap();
    let error = publisher
        .try_publish("preserved".to_owned(), nz(1))
        .expect_err("closed publisher rejects admission");
    assert!(matches!(error, TryPublishError::Closed(payload) if payload == "preserved"));
}

#[test]
fn test_close_with_timeout_can_be_retried_while_weight_reservation_is_held() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus");
    let (observed_tx, observed_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let publisher = WeightedNotificationPublisher::new(
        bus,
        Topic::<String>::new("weighted.timeout").unwrap(),
        nz(1),
        nz(2),
        move |_| {
            observed_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        },
    )
    .unwrap();
    publisher.try_publish("held".to_owned(), nz(2)).unwrap();
    observed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(
        std::io::ErrorKind::TimedOut,
        publisher.close_with_timeout(Duration::ZERO).unwrap_err().kind()
    );
    release_tx.send(()).unwrap();
    thread::yield_now();
    publisher.close().unwrap();
}

#[test]
fn test_publish_error_releases_its_reservation_after_the_observer() {
    let bus = EventBus::local(LocalEventBusConfig::new().max_total_outstanding_weight_bytes(nz(10)))
        .expect("local bus with required native payload weight");
    let (observed_tx, observed_rx) = mpsc::channel();
    let publisher = WeightedNotificationPublisher::new(
        bus,
        Topic::<String>::new("weighted.publish-error").unwrap(),
        nz(1),
        nz(10),
        move |outcome| {
            assert!(matches!(outcome, NotificationOutcome::PublishFailed(_)));
            observed_tx.send(()).unwrap();
        },
    )
    .unwrap();

    publisher.try_publish("first".to_owned(), nz(4)).unwrap();
    observed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        match publisher.try_publish("second".to_owned(), nz(10)) {
            Ok(()) => break,
            Err(TryPublishError::Full(payload)) => {
                assert_eq!("second", payload);
                assert!(
                    std::time::Instant::now() < deadline,
                    "publish error leaked its reservation"
                );
                thread::yield_now();
            }
            Err(TryPublishError::Closed(_)) => panic!("publisher unexpectedly closed"),
        }
    }
    observed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    publisher.close().unwrap();
}

#[test]
fn test_observer_panic_releases_reservation_and_worker_continues() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus");
    let (observed_tx, observed_rx) = mpsc::channel();
    let first = Arc::new(AtomicBool::new(true));
    let first_observer = first.clone();
    let publisher = WeightedNotificationPublisher::new(
        bus,
        Topic::<String>::new("weighted.observer-panic").unwrap(),
        nz(1),
        nz(10),
        move |_| {
            if first_observer.swap(false, Ordering::SeqCst) {
                panic!("contained observer panic");
            }
            observed_tx.send(()).unwrap();
        },
    )
    .unwrap();

    publisher.try_publish("first".to_owned(), nz(4)).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        match publisher.try_publish("second".to_owned(), nz(10)) {
            Ok(()) => break,
            Err(TryPublishError::Full(payload)) => {
                assert_eq!("second", payload);
                assert!(
                    std::time::Instant::now() < deadline,
                    "observer panic leaked its reservation"
                );
                thread::yield_now();
            }
            Err(TryPublishError::Closed(_)) => panic!("publisher unexpectedly closed"),
        }
    }
    observed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    publisher.close().unwrap();
    assert_eq!(1, publisher.stats().observer_panicked());
    assert_eq!(0, publisher.stats().worker_panicked());
}

#[test]
fn test_worker_unwind_disconnect_returns_payload_and_does_not_deadlock_drop() {
    let bus = EventBus::local(LocalEventBusConfig::new()).expect("local event bus");
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let (callback_tx, callback_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let options = PublishOptions::<WorkerPayload>::builder()
        .native_payload_weight(move |_| {
            callback_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            nz(1)
        })
        .build();
    let publisher = Arc::new(
        WeightedNotificationPublisher::with_publish_options(
            bus,
            Topic::<WorkerPayload>::new("weighted.worker-unwind").unwrap(),
            nz(2),
            nz(10),
            options,
            |_| {},
        )
        .unwrap(),
    );
    publisher
        .try_publish(WorkerPayload::panicking(dropped_tx), nz(6))
        .unwrap();
    callback_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let (queued_drop_tx, queued_drop_rx) = mpsc::channel();
    publisher
        .try_publish(
            WorkerPayload::reentrant(Arc::downgrade(&publisher), queued_drop_tx),
            nz(4),
        )
        .unwrap();
    release_tx.send(()).unwrap();
    dropped_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(
        queued_drop_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("queued payload was dropped after receiver unwind")
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while publisher.stats().worker_panicked() == 0 {
        assert!(std::time::Instant::now() < deadline, "worker did not finish unwinding");
        thread::yield_now();
    }

    let error = publisher
        .try_publish(WorkerPayload::inert(), nz(10))
        .expect_err("a worker-unwound receiver disconnects admission");
    assert!(matches!(error, TryPublishError::Closed(_)));
    drop(error);
    assert_eq!(std::io::ErrorKind::Other, publisher.close().unwrap_err().kind());
}

struct WorkerPayload {
    panic_signal: Option<mpsc::Sender<()>>,
    publisher: Option<std::sync::Weak<WeightedNotificationPublisher<WorkerPayload>>>,
    reentered: Option<mpsc::Sender<bool>>,
}

impl WorkerPayload {
    fn panicking(signal: mpsc::Sender<()>) -> Self {
        Self {
            panic_signal: Some(signal),
            publisher: None,
            reentered: None,
        }
    }

    fn reentrant(
        publisher: std::sync::Weak<WeightedNotificationPublisher<WorkerPayload>>,
        reentered: mpsc::Sender<bool>,
    ) -> Self {
        Self {
            panic_signal: None,
            publisher: Some(publisher),
            reentered: Some(reentered),
        }
    }

    fn inert() -> Self {
        Self {
            panic_signal: None,
            publisher: None,
            reentered: None,
        }
    }
}

impl Drop for WorkerPayload {
    fn drop(&mut self) {
        if let Some(sender) = self.panic_signal.take() {
            sender.send(()).unwrap();
            panic!("payload destructor unwinds notification worker");
        }
        if let Some(weak) = self.publisher.take() {
            let publisher = weak.upgrade().expect("publisher remains alive");
            let was_closed = match publisher.try_publish(WorkerPayload::inert(), nz(10)) {
                Err(TryPublishError::Closed(_)) => true,
                Err(TryPublishError::Full(_)) | Ok(()) => false,
            };
            if let Some(sender) = self.reentered.take() {
                let _ = sender.send(was_closed);
            }
        }
    }
}
