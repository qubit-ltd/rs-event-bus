// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Notification worker completion contracts observed through its public API.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_event_bus::EventBus;
use qubit_event_bus::NotificationPublisher;
use qubit_event_bus::TryPublishError;
use qubit_event_bus::model::Topic;

#[test]
fn test_close_waits_for_accepted_work_and_reports_observer_panics_separately() {
    let bus = EventBus::local(Default::default()).expect("local bus should initialize");
    let observed = Arc::new(AtomicUsize::new(0));
    let observer_calls = Arc::clone(&observed);
    let publisher = NotificationPublisher::new(
        bus,
        Topic::<String>::new_static("completion.guard"),
        NonZeroUsize::new(2).expect("capacity should be nonzero"),
        move |_| {
            observer_calls.fetch_add(1, Ordering::SeqCst);
            panic!("observer panic is contained by the worker");
        },
    )
    .expect("worker should start");

    publisher
        .try_publish("first".to_owned())
        .expect("first item queues");
    publisher
        .try_publish("second".to_owned())
        .expect("second item queues");
    publisher
        .close()
        .expect("close should observe worker completion");

    assert_eq!(2, observed.load(Ordering::SeqCst));
    assert_eq!(2, publisher.stats().observer_panicked());
    assert_eq!(0, publisher.stats().worker_panicked());
    assert_eq!(2, publisher.stats().published());
    assert!(matches!(
        publisher.try_publish("after-close".to_owned()),
        Err(TryPublishError::Closed(payload)) if payload == "after-close"
    ));
}

#[test]
fn test_empty_worker_closes_successfully_and_close_is_idempotent() {
    let bus = EventBus::local(Default::default()).expect("local bus should initialize");
    let publisher = NotificationPublisher::new(
        bus,
        Topic::<String>::new_static("completion.empty"),
        NonZeroUsize::new(1).expect("capacity should be nonzero"),
        |_| {},
    )
    .expect("worker should start");

    publisher.close().expect("empty worker should drain");
    publisher.close().expect("subsequent close should succeed");

    assert_eq!(0, publisher.stats().published());
    assert_eq!(0, publisher.stats().worker_panicked());
    assert!(matches!(
        publisher.try_publish("late".to_owned()),
        Err(TryPublishError::Closed(payload)) if payload == "late"
    ));
}
