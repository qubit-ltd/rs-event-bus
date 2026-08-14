// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for the in-process event bus implementation.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::AckMode;
use qubit_event_bus::DEAD_LETTER_SUBSCRIBER_ID;
use qubit_event_bus::DeadLetterPayload;
use qubit_event_bus::DeadLetterRecord;
use qubit_event_bus::EventBusError;
use qubit_event_bus::EventBusResult;
use qubit_event_bus::EventEnvelope;
use qubit_event_bus::EventEnvelopeMetadata;
use qubit_event_bus::LocalEventBus;
use qubit_event_bus::LocalEventBusFactory;
use qubit_event_bus::PublishOptions;
use qubit_event_bus::SubscribeOptions;
use qubit_event_bus::SubscriberInterceptorAnyChain;
use qubit_event_bus::SubscriberInterceptorChain;
use qubit_event_bus::Topic;
use qubit_event_bus::discard_dead_letters;
use qubit_event_bus::standard_dead_letters_to;
use qubit_retry::RetryPolicy;

use crate::support::PanicHookGuard;

fn create_topic(name: &str) -> Topic<String> {
    Topic::try_new(name).expect("topic should build")
}

fn received_payloads(
    events: &Arc<Mutex<Vec<EventEnvelope<String>>>>,
) -> Vec<String> {
    let mut payloads = events
        .lock()
        .expect("received events should lock")
        .iter()
        .map(|event| event.payload().clone())
        .collect::<Vec<_>>();
    payloads.sort();
    payloads
}

fn create_dead_letter_topic(name: &str) -> Topic<DeadLetterPayload> {
    Topic::try_new(name).expect("dead letter topic should build")
}

fn retry_options(max_attempts: u32) -> RetryPolicy {
    RetryPolicy::builder()
        .max_attempts(max_attempts)
        .build()
        .expect("retry policy should build")
}

fn retry_options_with_attempt_timeout() -> RetryPolicy {
    RetryPolicy::builder()
        .max_attempts(2)
        .build()
        .expect("retry policy should build")
}

const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

fn wait_for_count(counter: &Arc<(Mutex<usize>, Condvar)>, expected: usize) {
    let (lock, condvar) = &**counter;
    let mut count = lock.lock().expect("counter should lock");
    let started_at = Instant::now();
    while *count < expected {
        let remaining = TEST_WAIT_TIMEOUT
            .checked_sub(started_at.elapsed())
            .unwrap_or_else(|| panic!("timed out waiting for counter to reach {expected}; current value is {count}"));
        let (next_count, wait_result) = condvar
            .wait_timeout(count, remaining)
            .expect("counter wait should not poison");
        count = next_count;
        assert!(
            !wait_result.timed_out() || *count >= expected,
            "timed out waiting for counter to reach {expected}; current value is {count}"
        );
    }
}

fn wait_for_gate(gate: &Arc<(Mutex<bool>, Condvar)>) {
    let (lock, condvar) = &**gate;
    let mut released = lock.lock().expect("gate should lock");
    let started_at = Instant::now();
    while !*released {
        let remaining = TEST_WAIT_TIMEOUT
            .checked_sub(started_at.elapsed())
            .unwrap_or_else(|| panic!("timed out waiting for gate to open"));
        let (next_released, wait_result) = condvar
            .wait_timeout(released, remaining)
            .expect("gate wait should not poison");
        released = next_released;
        assert!(
            !wait_result.timed_out() || *released,
            "timed out waiting for gate to open"
        );
    }
}

fn release_gate(gate: &Arc<(Mutex<bool>, Condvar)>) {
    let (lock, condvar) = &**gate;
    let mut released = lock.lock().expect("gate should lock");
    *released = true;
    condvar.notify_all();
}

#[test]
fn test_lifecycle_rejects_use_until_started_and_is_idempotent() {
    let bus = LocalEventBus::new();
    let topic = create_topic("lifecycle");

    assert_eq!(
        bus.publish(&topic, "payload".to_string())
            .expect_err("bus should reject publish"),
        EventBusError::not_started()
    );
    assert!(bus.start().expect("bus should start"));
    assert!(!bus.start().expect("start should be idempotent"));
    let subscription = bus
        .subscribe("sub-1", &topic, |_| Ok(()))
        .expect("subscribe should work after start");
    assert!(subscription.is_active());
    assert_eq!(subscription.subscriber_id(), "sub-1");
    assert_eq!(subscription.topic(), &topic);

    assert!(bus.shutdown());
    assert!(!subscription.is_active());
    assert!(!bus.shutdown());
    assert_eq!(
        bus.publish(&topic, "payload".to_string())
            .expect_err("bus should reject publish"),
        EventBusError::not_started()
    );
}

#[test]
fn test_default_bus_is_stopped_and_publish_all_propagates_first_error() {
    let bus = LocalEventBus::default();
    let topic = create_topic("default-bus");
    let envelope = EventEnvelope::create(topic, "payload".to_string());

    assert_eq!(
        bus.publish_all(vec![envelope])
            .expect_err("stopped bus should reject batch publish"),
        EventBusError::not_started()
    );
}

#[test]
fn test_subscribe_with_options_rejects_stopped_bus() {
    let bus = LocalEventBus::new();
    let topic = create_topic("subscribe-stopped");

    let error = match bus.subscribe_with_options(
        "sub",
        &topic,
        |_| Ok::<(), EventBusError>(()),
        SubscribeOptions::empty(),
    ) {
        Ok(_) => panic!("stopped bus should reject subscribe with options"),
        Err(error) => error,
    };

    assert_eq!(error, EventBusError::not_started());
}

#[test]
fn test_publish_delivers_event_to_single_subscriber() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("single");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    bus.subscribe("sub-1", &topic, move |event| {
        captured
            .lock()
            .expect("received events should lock")
            .push(event);
        Ok(())
    })
    .expect("subscribe should work");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(received_payloads(&received), vec!["payload".to_string()]);
}

#[test]
fn test_publish_delay_defers_local_delivery() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("delayed-delivery");
    let (received_tx, received_rx) = mpsc::channel();
    bus.subscribe("sub-1", &topic, move |event| {
        received_tx
            .send((Instant::now(), event.payload().clone()))
            .expect("received timestamp should send");
        Ok(())
    })
    .expect("subscribe should work");

    let delay = Duration::from_millis(80);
    let started_at = Instant::now();
    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "payload".to_string())
            .with_delay(delay),
    )
    .expect("publish should accept delayed event");

    assert!(
        received_rx.recv_timeout(Duration::from_millis(25)).is_err(),
        "handler should not run before the configured delay"
    );
    let (received_at, payload) = received_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("delayed handler should eventually run");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(payload, "payload");
    assert!(
        received_at.duration_since(started_at) >= delay,
        "handler ran before the configured delay elapsed"
    );
}

#[test]
fn test_delayed_delivery_does_not_occupy_handler_worker() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("delayed-does-not-block-worker");
    let (received_tx, received_rx) = mpsc::channel::<(String, Instant)>();

    bus.subscribe("sub", &topic, move |event| {
        received_tx
            .send((event.payload().clone(), Instant::now()))
            .expect("received payload should send");
        Ok(())
    })
    .expect("subscribe should work");

    let published_at = Instant::now();
    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "delayed".to_string())
            .with_delay(Duration::from_millis(250)),
    )
    .expect("delayed publish should work");
    bus.publish(&topic, "immediate".to_string())
        .expect("immediate publish should work");

    let first = received_rx
        .recv_timeout(Duration::from_millis(150))
        .expect("immediate event should not wait behind delayed delivery");
    assert_eq!(first.0, "immediate");
    assert!(first.1.duration_since(published_at) < Duration::from_millis(150));

    let second = received_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("delayed event should eventually be delivered");
    assert_eq!(second.0, "delayed");
    assert!(
        second.1.duration_since(published_at) >= Duration::from_millis(250)
    );
    bus.wait_for_idle(&topic).expect("topic should become idle");
}

#[test]
fn test_short_delayed_delivery_does_not_wait_behind_long_delays() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("short-delay-before-long-delay");
    let (received_tx, received_rx) = mpsc::channel::<(String, Instant)>();

    bus.subscribe("sub", &topic, move |event| {
        received_tx
            .send((event.payload().clone(), Instant::now()))
            .expect("received payload should send");
        Ok(())
    })
    .expect("subscription should register");

    let published_at = Instant::now();
    for index in 0..4 {
        bus.publish_envelope(
            EventEnvelope::create(topic.clone(), format!("long-{index}"))
                .with_delay(Duration::from_millis(300)),
        )
        .expect("long delayed publish should work");
    }
    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "short".to_string())
            .with_delay(Duration::from_millis(30)),
    )
    .expect("short delayed publish should work");

    let first = received_rx
        .recv_timeout(Duration::from_millis(180))
        .expect("short delay should not wait behind long delayed events");
    assert_eq!(first.0, "short");
    assert!(first.1.duration_since(published_at) < Duration::from_millis(180));
    bus.wait_for_idle(&topic).expect("topic should become idle");
}

#[test]
fn test_ordered_delayed_delivery_does_not_occupy_handler_worker() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("ordered-delayed-does-not-block-worker");
    let (received_tx, received_rx) = mpsc::channel::<(String, Instant)>();

    bus.subscribe("sub", &topic, move |event| {
        received_tx
            .send((event.payload().clone(), Instant::now()))
            .expect("received payload should send");
        Ok(())
    })
    .expect("subscribe should work");

    let published_at = Instant::now();
    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "ordered-delayed".to_string())
            .with_ordering_key("same-key")
            .with_delay(Duration::from_millis(250)),
    )
    .expect("ordered delayed publish should work");
    bus.publish(&topic, "immediate".to_string())
        .expect("immediate publish should work");

    let first = received_rx.recv_timeout(Duration::from_millis(150)).expect(
        "immediate event should not wait behind ordered delayed delivery",
    );
    assert_eq!(first.0, "immediate");
    assert!(first.1.duration_since(published_at) < Duration::from_millis(150));

    let second = received_rx
        .recv_timeout(Duration::from_millis(500))
        .expect("ordered delayed event should eventually be delivered");
    assert_eq!(second.0, "ordered-delayed");
    assert!(
        second.1.duration_since(published_at) >= Duration::from_millis(250)
    );
    bus.wait_for_idle(&topic).expect("topic should become idle");
}

#[test]
fn test_ordered_delayed_delivery_preserves_same_key_order() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(2)
        .expect("pool size should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("ordered-delayed-preserves-order");
    let (received_tx, received_rx) = mpsc::channel::<String>();

    bus.subscribe("sub", &topic, move |event| {
        received_tx
            .send(event.payload().clone())
            .expect("received payload should send");
        Ok(())
    })
    .expect("subscribe should work");

    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "first".to_string())
            .with_ordering_key("same-key")
            .with_delay(Duration::from_millis(120)),
    )
    .expect("first delayed publish should work");
    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "second".to_string())
            .with_ordering_key("same-key"),
    )
    .expect("second publish should work");

    assert!(
        received_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "same-key delivery should wait for the delayed first event"
    );
    assert_eq!(
        received_rx
            .recv_timeout(Duration::from_millis(500))
            .expect("first event should arrive"),
        "first"
    );
    assert_eq!(
        received_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("second event should arrive after first"),
        "second"
    );
    bus.wait_for_idle(&topic).expect("topic should become idle");
}

#[test]
fn test_ordered_huge_delay_does_not_become_immediately_ready() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("ordered-huge-delay");
    let (received_tx, received_rx) = mpsc::channel::<String>();
    let subscription = bus
        .subscribe("sub", &topic, move |event| {
            received_tx
                .send(event.payload().clone())
                .expect("received payload should send");
            Ok(())
        })
        .expect("subscription should register");

    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "delayed".to_string())
            .with_ordering_key("same-key")
            .with_delay(Duration::MAX),
    )
    .expect("huge ordered delayed publish should be accepted");

    assert!(
        received_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "huge ordered delay should not overflow into immediate delivery"
    );
    subscription
        .cancel()
        .expect("subscription cancellation should succeed");
    assert!(
        bus.wait_for_idle_timeout(&topic, Duration::from_millis(150))
            .expect("cancelled huge delayed delivery should become idle")
    );
}

#[test]
fn test_delayed_delivery_runs_when_handler_queue_is_saturated_at_delay_expiry()
{
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    factory
        .set_subscription_handler_queue_capacity(Some(1))
        .expect("bounded queue should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("delayed-saturated-at-expiry");
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let captured_release = Arc::clone(&release);
    let (received_tx, received_rx) = mpsc::channel::<String>();

    bus.subscribe("sub", &topic, move |event| {
        let payload = event.payload().clone();
        received_tx
            .send(payload.clone())
            .expect("received payload should send");
        if payload == "first" {
            wait_for_gate(&captured_release);
        }
        Ok(())
    })
    .expect("subscription should register");

    bus.publish(&topic, "first".to_string())
        .expect("first publish should occupy the worker");
    assert_eq!(
        received_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("first event should start"),
        "first"
    );
    bus.publish(&topic, "second".to_string())
        .expect("second publish should fill the handler queue");
    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "delayed".to_string())
            .with_delay(Duration::from_millis(30)),
    )
    .expect("delayed publish should be accepted before expiry");

    assert_eq!(
        received_rx
            .recv_timeout(Duration::from_millis(250))
            .expect("delayed event should still run after expiry"),
        "delayed"
    );
    release_gate(&release);
    assert_eq!(
        received_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("queued second event should run after release"),
        "second"
    );
    bus.wait_for_idle(&topic).expect("topic should become idle");
}

#[test]
fn test_wait_for_idle_timeout_reports_busy_topic() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("wait-for-idle-timeout");
    let started = Arc::new((Mutex::new(0_usize), Condvar::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let captured_started = Arc::clone(&started);
    let captured_release = Arc::clone(&release);

    bus.subscribe("sub", &topic, move |_event| {
        let (started_lock, started_condvar) = &*captured_started;
        let mut started_count =
            started_lock.lock().expect("started count should lock");
        *started_count += 1;
        started_condvar.notify_all();
        drop(started_count);
        wait_for_gate(&captured_release);
        Ok(())
    })
    .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should start handler work");
    wait_for_count(&started, 1);

    assert!(
        !bus.wait_for_idle_timeout(&topic, Duration::from_millis(10))
            .expect("busy topic wait should return timeout result")
    );
    release_gate(&release);
    assert!(
        bus.wait_for_idle_timeout(&topic, Duration::from_secs(1))
            .expect("released topic should become idle")
    );
}

#[test]
fn test_ordering_key_serializes_same_key_delivery_on_multi_worker_pool() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(2)
        .expect("pool size should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("ordering-key-serializes");
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let sequence = Arc::new(Mutex::new(Vec::<String>::new()));
    let (first_started_tx, first_started_rx) = mpsc::channel();
    let (second_started_tx, second_started_rx) = mpsc::channel();
    let captured_release = Arc::clone(&release);
    let captured_sequence = Arc::clone(&sequence);

    bus.subscribe("sub-1", &topic, move |event| {
        if event.payload() == "first" {
            first_started_tx.send(()).expect("first start should send");
            let (lock, condvar) = &*captured_release;
            let mut released = lock.lock().expect("release gate should lock");
            while !*released {
                released = condvar
                    .wait(released)
                    .expect("release gate wait should not poison");
            }
        } else if event.payload() == "second" {
            second_started_tx
                .send(())
                .expect("second start should send");
        }
        captured_sequence
            .lock()
            .expect("sequence should lock")
            .push(event.payload().clone());
        Ok(())
    })
    .expect("subscribe should work");

    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "first".to_string())
            .with_ordering_key("account-1"),
    )
    .expect("first publish should work");
    first_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first handler should start");
    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "second".to_string())
            .with_ordering_key("account-1"),
    )
    .expect("second publish should work");

    assert!(
        second_started_rx
            .recv_timeout(Duration::from_millis(30))
            .is_err(),
        "same ordering key should wait for the previous delivery"
    );
    release_gate(&release);
    second_started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("second handler should start after first completes");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(
        sequence.lock().expect("sequence should lock").as_slice(),
        ["first", "second"]
    );
}

#[test]
fn test_ordering_key_delivery_respects_bounded_queue_capacity() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    factory
        .set_subscription_handler_queue_capacity(Some(1))
        .expect("bounded queue should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("ordered-bounded-handler-queue");
    let started = Arc::new((Mutex::new(0_usize), Condvar::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let captured_started = Arc::clone(&started);
    let captured_release = Arc::clone(&release);
    bus.subscribe("sub", &topic, move |_event| {
        let (started_lock, started_condvar) = &*captured_started;
        let mut started_count =
            started_lock.lock().expect("started count should lock");
        *started_count += 1;
        started_condvar.notify_all();
        drop(started_count);

        let (release_lock, release_condvar) = &*captured_release;
        let mut released =
            release_lock.lock().expect("release gate should lock");
        while !*released {
            released = release_condvar
                .wait(released)
                .expect("release gate wait should not poison");
        }
    })
    .expect("subscription should register");

    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "first".to_string())
            .with_ordering_key("account-1"),
    )
    .expect("first publish should occupy the ordered lane");
    wait_for_count(&started, 1);
    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "second".to_string())
            .with_ordering_key("account-1"),
    )
    .expect("second publish should fill the ordered lane queue");
    let error = bus
        .publish_envelope(
            EventEnvelope::create(topic.clone(), "third".to_string())
                .with_ordering_key("account-1"),
        )
        .expect_err(
            "third ordered publish should be rejected by the bounded queue",
        );
    release_gate(&release);
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert!(matches!(error, EventBusError::ExecutionRejected { .. }));
    assert_eq!(*started.0.lock().expect("started count should lock"), 2);
}

#[test]
fn test_ordering_key_yields_between_keys_on_single_worker_pool() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("ordered-key-fairness");
    let sequence = Arc::new(Mutex::new(Vec::<String>::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let first_started = Arc::new((Mutex::new(0_usize), Condvar::new()));
    let captured_sequence = Arc::clone(&sequence);
    let captured_release = Arc::clone(&release);
    let captured_first_started = Arc::clone(&first_started);
    bus.subscribe("sub", &topic, move |event| {
        captured_sequence
            .lock()
            .expect("sequence should lock")
            .push(event.payload().clone());
        if event.payload() == "a1" {
            let (started_lock, started_condvar) = &*captured_first_started;
            let mut started_count =
                started_lock.lock().expect("started count should lock");
            *started_count += 1;
            started_condvar.notify_all();
            drop(started_count);

            let (release_lock, release_condvar) = &*captured_release;
            let mut released =
                release_lock.lock().expect("release gate should lock");
            while !*released {
                released = release_condvar
                    .wait(released)
                    .expect("release gate wait should not poison");
            }
        }
    })
    .expect("subscription should register");

    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "a1".to_string())
            .with_ordering_key("account-a"),
    )
    .expect("first A event should publish");
    wait_for_count(&first_started, 1);
    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "a2".to_string())
            .with_ordering_key("account-a"),
    )
    .expect("second A event should queue");
    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "b1".to_string())
            .with_ordering_key("account-b"),
    )
    .expect("B event should queue behind the running worker");

    release_gate(&release);
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(
        sequence.lock().expect("sequence should lock").as_slice(),
        ["a1", "b1", "a2"]
    );
}

#[test]
fn test_publish_broadcasts_to_multiple_subscribers() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("broadcast");
    let received = Arc::new(Mutex::new(Vec::new()));

    for index in 0..3 {
        let captured = Arc::clone(&received);
        bus.subscribe(format!("sub-{index}"), &topic, move |event| {
            captured
                .lock()
                .expect("received events should lock")
                .push(event);
            Ok(())
        })
        .expect("subscribe should work");
    }

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(
        received_payloads(&received),
        vec![
            "payload".to_string(),
            "payload".to_string(),
            "payload".to_string()
        ]
    );
}

#[test]
fn test_topic_isolation_and_unsubscribe() {
    let bus = LocalEventBus::started().expect("bus should start");
    let target = create_topic("target");
    let other = create_topic("other");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    let subscription = bus
        .subscribe("sub-1", &target, move |event| {
            captured
                .lock()
                .expect("received events should lock")
                .push(event);
            Ok(())
        })
        .expect("subscribe should work");

    bus.publish(&other, "ignored".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&other)
        .expect("other topic should become idle");
    bus.publish(&target, "received".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&target)
        .expect("target topic should become idle");
    subscription.cancel().expect("cancel should work");
    subscription.cancel().expect("cancel should be idempotent");
    assert!(!subscription.is_active());
    bus.publish(&target, "ignored-after-cancel".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&target)
        .expect("target topic should become idle");

    assert_eq!(received_payloads(&received), vec!["received".to_string()]);
}

#[test]
fn test_subscribe_rejects_blank_subscriber_id() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("blank-subscriber");

    let error = match bus.subscribe(" ", &topic, |_| Ok(())) {
        Ok(_) => panic!("blank subscriber ID should be rejected"),
        Err(error) => error,
    };

    assert_eq!(
        error,
        EventBusError::invalid_argument(
            "subscriber_id",
            "subscriber ID must not be blank"
        )
    );
}

#[test]
fn test_subscribe_options_filter_events() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("filter");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);
    let options = SubscribeOptions::<String>::builder()
        .filter(|event: &EventEnvelope<String>| event.payload() == "accepted")
        .build();

    bus.subscribe_with_options(
        "sub-1",
        &topic,
        move |event| {
            captured
                .lock()
                .expect("received events should lock")
                .push(event);
            Ok(())
        },
        options,
    )
    .expect("subscribe should work");

    bus.publish(&topic, "rejected".to_string())
        .expect("publish should work");
    bus.publish(&topic, "accepted".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(received_payloads(&received), vec!["accepted".to_string()]);
}

#[test]
fn test_subscribe_filter_panic_becomes_publish_error() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("filter-panic");
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let captured_handler_calls = Arc::clone(&handler_calls);
    let publish_errors = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_publish_errors = Arc::clone(&publish_errors);
    let options = SubscribeOptions::<String>::builder()
        .filter(|_event| -> bool {
            panic!("filter panic");
        })
        .build();
    bus.subscribe_with_options(
        "sub-1",
        &topic,
        move |_event| {
            captured_handler_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        },
        options,
    )
    .expect("subscribe should work");
    let publish_options = PublishOptions::<String>::builder()
        .error_handler(move |_event, error| {
            captured_publish_errors
                .lock()
                .expect("publish errors should lock")
                .push(error.clone());
            Ok(())
        })
        .build();
    let publish_result = {
        let _panic_hook_guard = PanicHookGuard::suppress();
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            bus.publish_envelope_with_options(
                EventEnvelope::create(topic.clone(), "payload".to_string()),
                publish_options,
            )
        }))
    };

    let error = publish_result
        .expect("filter panic should not unwind")
        .expect_err("filter panic should reject publish");
    assert_eq!(error.kind(), "handler_failed");
    assert!(error.to_string().contains("filter panicked"));
    assert_eq!(handler_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        publish_errors
            .lock()
            .expect("publish errors should lock")
            .as_slice(),
        &[error]
    );
}

#[test]
fn test_publisher_interceptor_can_modify_or_drop_events() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_publisher_interceptor::<String, _>(
            |event: EventEnvelope<String>| {
                if event.topic().name() == "dropped" {
                    None
                } else {
                    Some(event.with_header("intercepted", "true"))
                }
            },
        )
        .expect("interceptor should be registered");
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("intercepted");
    let dropped = create_topic("dropped");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);
    bus.subscribe("sub-1", &topic, move |event| {
        captured
            .lock()
            .expect("received events should lock")
            .push(event);
        Ok(())
    })
    .expect("subscribe should work");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.publish(&dropped, "dropped".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");
    bus.wait_for_idle(&dropped)
        .expect("topic should become idle");

    let events = received.lock().expect("received events should lock");
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].headers().get("intercepted"),
        Some(&"true".to_string())
    );
}

#[test]
fn test_global_publisher_interceptor_applies_to_all_payload_types_and_can_drop()
{
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_global_publisher_interceptor(|metadata: EventEnvelopeMetadata| {
            metadata.with_header("global-bare", "seen")
        })
        .expect("global publisher interceptor should register");
    factory
        .add_global_publisher_interceptor(|metadata: EventEnvelopeMetadata| {
            Ok::<EventEnvelopeMetadata, EventBusError>(
                metadata.with_header("global-result", "seen"),
            )
        })
        .expect("global publisher interceptor should register");
    factory
        .add_global_publisher_interceptor(|metadata: EventEnvelopeMetadata| {
            if metadata.topic_name() == "global-publisher-dropped" {
                Ok::<Option<EventEnvelopeMetadata>, EventBusError>(None)
            } else {
                let payload_type_name = metadata.payload_type_name();
                Ok(Some(
                    metadata.with_header("global-publisher", payload_type_name),
                ))
            }
        })
        .expect("global publisher interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let string_topic = create_topic("global-publisher-string");
    let number_topic = Topic::<i32>::try_new("global-publisher-number")
        .expect("number topic should build");
    let dropped_topic = create_topic("global-publisher-dropped");
    let string_received = Arc::new(Mutex::new(Vec::new()));
    let number_received = Arc::new(Mutex::new(Vec::new()));
    let captured_strings = Arc::clone(&string_received);
    let captured_numbers = Arc::clone(&number_received);
    bus.subscribe("string-sub", &string_topic, move |event| {
        captured_strings
            .lock()
            .expect("string events should lock")
            .push(event);
        Ok(())
    })
    .expect("string subscription should register");
    bus.subscribe("number-sub", &number_topic, move |event| {
        captured_numbers
            .lock()
            .expect("number events should lock")
            .push(event);
        Ok(())
    })
    .expect("number subscription should register");

    bus.publish(&string_topic, "payload".to_string())
        .expect("string publish should work");
    bus.publish(&number_topic, 7)
        .expect("number publish should work");
    bus.publish(&dropped_topic, "dropped".to_string())
        .expect("dropped publish should be accepted");
    bus.wait_for_idle(&string_topic)
        .expect("string topic should become idle");
    bus.wait_for_idle(&number_topic)
        .expect("number topic should become idle");
    bus.wait_for_idle(&dropped_topic)
        .expect("dropped topic should become idle");

    let string_events =
        string_received.lock().expect("string events should lock");
    let number_events =
        number_received.lock().expect("number events should lock");
    assert_eq!(string_events.len(), 1);
    assert_eq!(number_events.len(), 1);
    assert_eq!(
        string_events[0].headers().get("global-publisher"),
        Some(&string_events[0].topic().payload_type_name().to_string())
    );
    assert_eq!(
        string_events[0].headers().get("global-bare"),
        Some(&"seen".to_string())
    );
    assert_eq!(
        string_events[0].headers().get("global-result"),
        Some(&"seen".to_string())
    );
    assert_eq!(
        number_events[0].headers().get("global-publisher"),
        Some(&number_events[0].topic().payload_type_name().to_string())
    );
}

#[test]
fn test_global_publisher_interceptor_error_is_reported_to_publish_error_handling()
 {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_global_publisher_interceptor(|_metadata: EventEnvelopeMetadata| {
            Err::<EventEnvelopeMetadata, EventBusError>(
                EventBusError::handler_failed(
                    "global publisher interceptor failed",
                ),
            )
        })
        .expect("global publisher interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("global-publisher-interceptor-error");
    let publish_errors = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_publish_errors = Arc::clone(&publish_errors);
    let options = PublishOptions::<String>::builder()
        .error_handler(move |_event, error| {
            captured_publish_errors
                .lock()
                .expect("publish errors should lock")
                .push(error.clone());
            Ok(())
        })
        .build();

    let error = bus
        .publish_envelope_with_options(
            EventEnvelope::create(topic, "payload".to_string()),
            options,
        )
        .expect_err("global publisher interceptor error should reject publish");

    assert_eq!(error.kind(), "interceptor_failed");
    assert!(
        error
            .to_string()
            .contains("global publisher interceptor failed")
    );
    let publish_errors =
        publish_errors.lock().expect("publish errors should lock");
    assert_eq!(publish_errors.len(), 1);
    assert_eq!(publish_errors[0].kind(), "interceptor_failed");
}

#[test]
fn test_global_publisher_interceptor_panic_is_reported_to_publish_error_handling()
 {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_global_publisher_interceptor(
            |_metadata: EventEnvelopeMetadata| -> EventEnvelopeMetadata {
                panic!("global publisher interceptor panic");
            },
        )
        .expect("global publisher interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("global-publisher-interceptor-panic");
    let error = {
        let _panic_hook_guard = PanicHookGuard::suppress();
        bus.publish(&topic, "payload".to_string()).expect_err(
            "global publisher interceptor panic should reject publish",
        )
    };

    assert_eq!(error.kind(), "interceptor_failed");
    assert!(
        error
            .to_string()
            .contains("global publisher interceptor panicked")
    );
}

#[test]
fn test_publisher_interceptor_panic_is_reported_to_publish_error_handling() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_publisher_interceptor::<String, _>(
            |_event: EventEnvelope<String>| -> Option<EventEnvelope<String>> {
                panic!("publisher interceptor panic");
            },
        )
        .expect("interceptor should be registered");
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("publisher-interceptor-panic");
    let publish_errors = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let observed_errors = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_publish_errors = Arc::clone(&publish_errors);
    let captured_observed_errors = Arc::clone(&observed_errors);
    bus.add_error_observer(move |error| {
        captured_observed_errors
            .lock()
            .expect("observed errors should lock")
            .push(error.clone());
    })
    .expect("observer should register");
    let options = PublishOptions::<String>::builder()
        .error_handler(move |_event, error| {
            captured_publish_errors
                .lock()
                .expect("publish errors should lock")
                .push(error.clone());
            Err(EventBusError::handler_failed(
                "publish error handler saw interceptor panic",
            ))
        })
        .build();
    let error = {
        let _panic_hook_guard = PanicHookGuard::suppress();
        bus.publish_envelope_with_options(
            EventEnvelope::create(topic, "payload".to_string()),
            options,
        )
        .expect_err("publisher interceptor panic should become publish error")
    };

    assert_eq!(error.kind(), "interceptor_failed");
    assert!(error.to_string().contains("publisher interceptor panicked"));
    let publish_errors =
        publish_errors.lock().expect("publish errors should lock");
    assert_eq!(publish_errors.len(), 1);
    assert_eq!(publish_errors[0].kind(), "interceptor_failed");
    let observed_errors =
        observed_errors.lock().expect("observed errors should lock");
    assert!(
        observed_errors
            .iter()
            .any(|error| error.kind() == "interceptor_failed")
    );
    assert!(observed_errors.iter().any(|error| matches!(
        error,
        EventBusError::ErrorHandlerFailed { phase, message }
            if *phase == "publish" && message.contains("interceptor panic")
    )));
}

#[test]
fn test_publish_retry_retries_publisher_interceptor_failures() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let received = Arc::new(AtomicUsize::new(0));
    let captured_attempts = Arc::clone(&attempts);
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_publisher_interceptor::<String, _>(
            move |event: EventEnvelope<String>| {
                let attempt =
                    captured_attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if attempt == 1 {
                    panic!("transient publisher interceptor failure");
                }
                Some(event.with_header("attempt", attempt.to_string()))
            },
        )
        .expect("interceptor should be registered");
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("publisher-interceptor-retry");
    let captured_received = Arc::clone(&received);
    bus.subscribe("sub-1", &topic, move |event| {
        assert_eq!(event.headers().get("attempt"), Some(&"2".to_string()));
        captured_received.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .expect("subscribe should work");
    let options = PublishOptions::<String>::builder()
        .retry_options(retry_options(2))
        .build();
    let result = {
        let _panic_hook_guard = PanicHookGuard::suppress();
        bus.publish_envelope_with_options(
            EventEnvelope::create(topic.clone(), "payload".to_string()),
            options,
        )
    };

    result.expect("transient interceptor failure should be retried");
    bus.wait_for_idle(&topic).expect("topic should become idle");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(received.load(Ordering::SeqCst), 1);
}

#[test]
fn test_retry_eventually_succeeds() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("retry");
    let attempts = Arc::new(AtomicUsize::new(0));
    let captured_attempts = Arc::clone(&attempts);
    let options = SubscribeOptions::<String>::builder()
        .retry_options(retry_options(3))
        .build();

    bus.subscribe_with_options(
        "sub-1",
        &topic,
        move |_| {
            let attempt = captured_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt < 3 {
                Err(EventBusError::handler_failed(format!(
                    "attempt {attempt} failed"
                )))
            } else {
                Ok(())
            }
        },
        options,
    )
    .expect("subscribe should work");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

#[test]
fn test_retry_success_ignores_nack_from_failed_attempt() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("retry-manual-nack-then-success");
    let dead_letter_topic =
        create_dead_letter_topic("dlq.retry-manual-nack-then-success");
    let attempts = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let dead_letters =
        Arc::new(Mutex::new(Vec::<EventEnvelope<DeadLetterPayload>>::new()));
    let captured_attempts = Arc::clone(&attempts);
    let captured_errors = Arc::clone(&errors);
    let dead_letter_target = dead_letter_topic.clone();
    let options = SubscribeOptions::<String>::builder()
        .ack_mode(AckMode::Manual)
        .retry_options(retry_options(2))
        .error_handler(
            move |_subscriber_id, _envelope, _error, _acknowledgement| {
                captured_errors.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .dead_letter_strategy(move |subscriber_id, failed, error, _options| {
            Ok(Some(EventEnvelope::create(
                dead_letter_target.clone(),
                DeadLetterRecord::from_failure(subscriber_id, failed, error),
            )))
        })
        .build();
    bus.subscribe_with_options(
        "sub-1",
        &topic,
        move |event| {
            let attempt = captured_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == 1 {
                event
                    .acknowledgement()
                    .expect("acknowledgement should be injected")
                    .nack();
                Err(EventBusError::handler_failed("first attempt failed"))
            } else {
                Ok(())
            }
        },
        options,
    )
    .expect("subscribe should work");
    let captured_dead_letters = Arc::clone(&dead_letters);
    bus.subscribe("dlq-sub", &dead_letter_topic, move |event| {
        captured_dead_letters
            .lock()
            .expect("dead letters should lock")
            .push(event);
        Ok(())
    })
    .expect("dead letter subscriber should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");
    bus.wait_for_idle(&dead_letter_topic)
        .expect("dead letter topic should become idle");

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(errors.load(Ordering::SeqCst), 0);
    assert!(
        dead_letters
            .lock()
            .expect("dead letters should lock")
            .is_empty()
    );
}

#[test]
fn test_manual_nack_returning_ok_is_retried() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("manual-nack-ok-retry");
    let attempts = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let captured_attempts = Arc::clone(&attempts);
    let captured_errors = Arc::clone(&errors);
    let options = SubscribeOptions::<String>::builder()
        .ack_mode(AckMode::Manual)
        .retry_options(retry_options(2))
        .error_handler(
            move |_subscriber_id, _envelope, _error, _acknowledgement| {
                captured_errors.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .build();

    bus.subscribe_with_options(
        "sub-1",
        &topic,
        move |event| {
            let attempt = captured_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            let acknowledgement = event
                .acknowledgement()
                .expect("acknowledgement should be injected");
            if attempt == 1 {
                acknowledgement.nack();
            } else {
                acknowledgement.ack();
            }
            Ok(())
        },
        options,
    )
    .expect("subscribe should work");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(errors.load(Ordering::SeqCst), 0);
}

#[test]
fn test_retry_failure_ignores_ack_from_failed_attempt() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("retry-manual-ack-then-failure");
    let dead_letter_topic =
        create_dead_letter_topic("dlq.retry-manual-ack-then-failure");
    let attempts = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let dead_letters =
        Arc::new(Mutex::new(Vec::<EventEnvelope<DeadLetterPayload>>::new()));
    let captured_attempts = Arc::clone(&attempts);
    let captured_errors = Arc::clone(&errors);
    let dead_letter_target = dead_letter_topic.clone();
    let options = SubscribeOptions::<String>::builder()
        .ack_mode(AckMode::Manual)
        .retry_options(retry_options(2))
        .error_handler(
            move |_subscriber_id, _envelope, _error, acknowledgement| {
                assert!(!acknowledgement.is_acked());
                captured_errors.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .dead_letter_strategy(move |subscriber_id, failed, error, _options| {
            Ok(Some(EventEnvelope::create(
                dead_letter_target.clone(),
                DeadLetterRecord::from_failure(subscriber_id, failed, error),
            )))
        })
        .build();
    bus.subscribe_with_options(
        "sub-1",
        &topic,
        move |event| {
            let attempt = captured_attempts.fetch_add(1, Ordering::SeqCst) + 1;
            if attempt == 1 {
                event
                    .acknowledgement()
                    .expect("acknowledgement should be injected")
                    .ack();
            }
            Err(EventBusError::handler_failed(format!(
                "attempt {attempt} failed"
            )))
        },
        options,
    )
    .expect("subscribe should work");
    let captured_dead_letters = Arc::clone(&dead_letters);
    bus.subscribe("dlq-sub", &dead_letter_topic, move |event| {
        captured_dead_letters
            .lock()
            .expect("dead letters should lock")
            .push(event);
        Ok(())
    })
    .expect("dead letter subscriber should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");
    bus.wait_for_idle(&dead_letter_topic)
        .expect("dead letter topic should become idle");

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(errors.load(Ordering::SeqCst), 1);
    assert_eq!(
        dead_letters.lock().expect("dead letters should lock").len(),
        1
    );
}

#[test]
fn test_exhausted_retry_calls_error_handler_and_dead_letter_strategy() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("retry-failed");
    let dead_letter_topic = create_dead_letter_topic("dlq.retry-failed");
    let attempts = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let dead_letters =
        Arc::new(Mutex::new(Vec::<EventEnvelope<DeadLetterPayload>>::new()));

    let captured_attempts = Arc::clone(&attempts);
    let captured_errors = Arc::clone(&errors);
    let dead_letter_target = dead_letter_topic.clone();
    let options = SubscribeOptions::<String>::builder()
        .retry_options(retry_options(2))
        .error_handler(
            move |subscriber_id, envelope, error, acknowledgement| {
                assert_eq!(subscriber_id, "sub-1");
                assert_eq!(envelope.payload(), "payload");
                assert!(error.to_string().contains("failed"));
                assert!(!acknowledgement.is_completed());
                captured_errors.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .dead_letter_strategy(move |subscriber_id, failed, error, _options| {
            Ok(Some(
                EventEnvelope::create(
                    dead_letter_target.clone(),
                    DeadLetterRecord::from_failure(
                        subscriber_id,
                        failed,
                        error,
                    ),
                )
                .with_header("subscriber-id", subscriber_id)
                .with_header("failure", error.to_string())
                .as_dead_letter(),
            ))
        })
        .build();

    bus.subscribe_with_options(
        "sub-1",
        &topic,
        move |_| {
            captured_attempts.fetch_add(1, Ordering::SeqCst);
            Err(EventBusError::handler_failed("handler failed"))
        },
        options,
    )
    .expect("subscribe should work");

    let captured_dead_letters = Arc::clone(&dead_letters);
    bus.subscribe("dlq-sub", &dead_letter_topic, move |event| {
        captured_dead_letters
            .lock()
            .expect("dead letters should lock")
            .push(event);
        Ok(())
    })
    .expect("dead letter subscriber should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");
    bus.wait_for_idle(&dead_letter_topic)
        .expect("dead letter topic should become idle");

    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(errors.load(Ordering::SeqCst), 1);
    let events = dead_letters.lock().expect("dead letters should lock");
    assert_eq!(events.len(), 1);
    assert!(events[0].is_dead_letter());
    assert_eq!(
        events[0].headers().get("subscriber-id"),
        Some(&"sub-1".to_string())
    );
    let payload = events[0]
        .payload()
        .downcast_original_payload_ref::<String>()
        .expect("dead letter payload should preserve original payload");
    assert_eq!(payload, "payload");
    assert_eq!(
        events[0]
            .payload()
            .metadata()
            .get::<String>(DEAD_LETTER_SUBSCRIBER_ID),
        Some("sub-1".to_string())
    );
}

#[test]
fn test_standard_dead_letter_strategy_helper_routes_standard_payload() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("standard-dead-letter-helper");
    let dead_letter_topic =
        create_dead_letter_topic("dlq.standard-dead-letter-helper");
    let dead_letters =
        Arc::new(Mutex::new(Vec::<EventEnvelope<DeadLetterPayload>>::new()));
    let captured_dead_letters = Arc::clone(&dead_letters);
    bus.subscribe("dlq-sub", &dead_letter_topic, move |event| {
        captured_dead_letters
            .lock()
            .expect("dead letters should lock")
            .push(event);
        Ok(())
    })
    .expect("dead letter subscriber should register");
    let options = SubscribeOptions::<String>::builder()
        .dead_letter_strategy(standard_dead_letters_to(
            dead_letter_topic.clone(),
        ))
        .build();
    bus.subscribe_with_options(
        "sub",
        &topic,
        |_event| Err(EventBusError::handler_failed("handler failed")),
        options,
    )
    .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");
    bus.wait_for_idle(&dead_letter_topic)
        .expect("dead letter topic should become idle");

    let events = dead_letters.lock().expect("dead letters should lock");
    assert_eq!(events.len(), 1);
    assert!(events[0].is_dead_letter());
    assert_eq!(
        events[0]
            .payload()
            .metadata()
            .get::<String>(DEAD_LETTER_SUBSCRIBER_ID),
        Some("sub".to_string())
    );
    assert_eq!(
        events[0]
            .payload()
            .downcast_original_payload_ref::<String>(),
        Some(&"payload".to_string())
    );
}

#[test]
fn test_discard_dead_letter_strategy_helper_suppresses_dead_letter_routing() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("discard-dead-letter-helper");
    let dead_letter_topic =
        create_dead_letter_topic("dlq.discard-dead-letter-helper");
    let dead_letters =
        Arc::new(Mutex::new(Vec::<EventEnvelope<DeadLetterPayload>>::new()));
    let captured_dead_letters = Arc::clone(&dead_letters);
    bus.subscribe("dlq-sub", &dead_letter_topic, move |event| {
        captured_dead_letters
            .lock()
            .expect("dead letters should lock")
            .push(event);
        Ok(())
    })
    .expect("dead letter subscriber should register");
    let options = SubscribeOptions::<String>::builder()
        .dead_letter_strategy(discard_dead_letters())
        .build();
    bus.subscribe_with_options(
        "sub",
        &topic,
        |_event| Err(EventBusError::handler_failed("handler failed")),
        options,
    )
    .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");
    bus.wait_for_idle(&dead_letter_topic)
        .expect("dead letter topic should become idle");

    assert!(
        dead_letters
            .lock()
            .expect("dead letters should lock")
            .is_empty()
    );
}

#[test]
fn test_manual_ack_is_exposed_to_handler() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("manual-ack");
    let acknowledgements = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&acknowledgements);
    let options = SubscribeOptions::<String>::builder()
        .ack_mode(AckMode::Manual)
        .build();

    bus.subscribe_with_options(
        "sub-1",
        &topic,
        move |event| {
            let acknowledgement = event
                .acknowledgement()
                .expect("manual ack should be injected")
                .clone();
            acknowledgement.ack();
            captured
                .lock()
                .expect("acknowledgements should lock")
                .push(acknowledgement);
            Ok(())
        },
        options,
    )
    .expect("subscribe should work");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    let acknowledgements = acknowledgements
        .lock()
        .expect("acknowledgements should lock");
    assert_eq!(acknowledgements.len(), 1);
    assert!(acknowledgements[0].is_acked());
    assert!(!acknowledgements[0].is_nacked());
}

#[test]
fn test_manual_nack_is_treated_as_subscription_failure() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("manual-nack");
    let dead_letter_topic = create_dead_letter_topic("dlq.manual-nack");
    let errors = Arc::new(AtomicUsize::new(0));
    let dead_letters =
        Arc::new(Mutex::new(Vec::<EventEnvelope<DeadLetterPayload>>::new()));
    let captured_errors = Arc::clone(&errors);
    let dead_letter_target = dead_letter_topic.clone();
    let options = SubscribeOptions::<String>::builder()
        .ack_mode(AckMode::Manual)
        .error_handler(
            move |subscriber_id, envelope, error, acknowledgement| {
                assert_eq!(subscriber_id, "sub-1");
                assert_eq!(envelope.payload(), "payload");
                assert!(error.to_string().contains("nack"));
                assert!(acknowledgement.is_nacked());
                captured_errors.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .dead_letter_strategy(
            move |_subscriber_id, failed, _error, _options| {
                Ok(Some(EventEnvelope::create(
                    dead_letter_target.clone(),
                    DeadLetterRecord::from_failure(
                        _subscriber_id,
                        failed,
                        _error,
                    ),
                )))
            },
        )
        .build();

    bus.subscribe_with_options(
        "sub-1",
        &topic,
        move |event| {
            event
                .acknowledgement()
                .expect("manual ack should be injected")
                .nack();
            Ok(())
        },
        options,
    )
    .expect("subscribe should work");

    let captured_dead_letters = Arc::clone(&dead_letters);
    bus.subscribe("dlq-sub", &dead_letter_topic, move |event| {
        captured_dead_letters
            .lock()
            .expect("dead letters should lock")
            .push(event);
        Ok(())
    })
    .expect("dead letter subscriber should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");
    bus.wait_for_idle(&dead_letter_topic)
        .expect("dead letter topic should become idle");

    assert_eq!(errors.load(Ordering::SeqCst), 1);
    let events = dead_letters.lock().expect("dead letters should lock");
    assert_eq!(events.len(), 1);
    assert!(events[0].is_dead_letter());
}

#[test]
fn test_subscribe_error_handler_ack_short_circuits_failure_handling() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("error-handler-ack");
    let dead_letter_topic = create_dead_letter_topic("dlq.error-handler-ack");
    let first_errors = Arc::new(AtomicUsize::new(0));
    let second_errors = Arc::new(AtomicUsize::new(0));
    let dead_letters =
        Arc::new(Mutex::new(Vec::<EventEnvelope<DeadLetterPayload>>::new()));
    let captured_first = Arc::clone(&first_errors);
    let captured_second = Arc::clone(&second_errors);
    let dead_letter_target = dead_letter_topic.clone();
    let options = SubscribeOptions::<String>::builder()
        .error_handler(
            move |_subscriber_id, _envelope, _error, acknowledgement| {
                acknowledgement.ack();
                captured_first.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .error_handler(
            move |_subscriber_id, _envelope, _error, _acknowledgement| {
                captured_second.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .dead_letter_strategy(
            move |_subscriber_id, failed, _error, _options| {
                Ok(Some(EventEnvelope::create(
                    dead_letter_target.clone(),
                    DeadLetterRecord::from_failure(
                        _subscriber_id,
                        failed,
                        _error,
                    ),
                )))
            },
        )
        .build();

    bus.subscribe_with_options(
        "sub-1",
        &topic,
        |_| Err(EventBusError::handler_failed("boom")),
        options,
    )
    .expect("subscribe should work");

    let captured_dead_letters = Arc::clone(&dead_letters);
    bus.subscribe("dlq-sub", &dead_letter_topic, move |event| {
        captured_dead_letters
            .lock()
            .expect("dead letters should lock")
            .push(event);
        Ok(())
    })
    .expect("dead letter subscriber should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");
    bus.wait_for_idle(&dead_letter_topic)
        .expect("dead letter topic should become idle");

    assert_eq!(first_errors.load(Ordering::SeqCst), 1);
    assert_eq!(second_errors.load(Ordering::SeqCst), 0);
    assert!(
        dead_letters
            .lock()
            .expect("dead letters should lock")
            .is_empty()
    );
}

#[test]
fn test_manual_nack_notifies_all_error_handlers_until_acknowledged() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("manual-nack-error-handler-chain");
    let first_errors = Arc::new(AtomicUsize::new(0));
    let second_errors = Arc::new(AtomicUsize::new(0));
    let captured_first = Arc::clone(&first_errors);
    let captured_second = Arc::clone(&second_errors);
    let options = SubscribeOptions::<String>::builder()
        .ack_mode(AckMode::Manual)
        .error_handler(
            move |_subscriber_id, _envelope, _error, acknowledgement| {
                assert!(acknowledgement.is_nacked());
                captured_first.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .error_handler(
            move |_subscriber_id, _envelope, _error, acknowledgement| {
                assert!(acknowledgement.is_nacked());
                captured_second.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .build();

    bus.subscribe_with_options(
        "sub-1",
        &topic,
        |event| {
            event
                .acknowledgement()
                .expect("manual ack should be injected")
                .nack();
            Ok(())
        },
        options,
    )
    .expect("subscribe should work");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(first_errors.load(Ordering::SeqCst), 1);
    assert_eq!(second_errors.load(Ordering::SeqCst), 1);
}

#[test]
fn test_handler_panic_is_reported_and_does_not_block_idle_wait() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("panic-handler");
    let errors = Arc::new(AtomicUsize::new(0));
    let captured_errors = Arc::clone(&errors);
    let options = SubscribeOptions::builder()
        .error_handler(
            move |_subscriber_id, _envelope, error, acknowledgement| {
                assert!(error.to_string().contains("panicked"));
                assert!(!acknowledgement.is_completed());
                captured_errors.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .build();
    bus.subscribe_with_options(
        "sub-1",
        &topic,
        |_| -> qubit_event_bus::EventBusResult<()> {
            panic!("handler panic");
        },
        options,
    )
    .expect("subscribe should work");

    {
        let _panic_hook_guard = PanicHookGuard::suppress();
        bus.publish(&topic, "payload".to_string())
            .expect("publish should work");
        bus.wait_for_idle(&topic)
            .expect("topic should become idle after panic");
    }

    assert_eq!(errors.load(Ordering::SeqCst), 1);
}

#[test]
fn test_subscriber_interceptor_wraps_handler_and_can_short_circuit() {
    let sequence = Arc::new(Mutex::new(Vec::<String>::new()));
    let captured_before_after = Arc::clone(&sequence);
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_subscriber_interceptor::<String, _>(
            move |event: EventEnvelope<String>,
                  chain: SubscriberInterceptorChain<String>| {
                captured_before_after
                    .lock()
                    .expect("sequence should lock")
                    .push(format!("before:{}", event.payload()));
                let result =
                    chain.proceed(event.with_header("intercepted", "true"));
                captured_before_after
                    .lock()
                    .expect("sequence should lock")
                    .push("after".to_string());
                result
            },
        )
        .expect("subscriber interceptor should register");
    let captured_short_circuit = Arc::clone(&sequence);
    factory
        .add_subscriber_interceptor::<String, _>(
            move |event: EventEnvelope<String>,
                  chain: SubscriberInterceptorChain<String>| {
                if event.topic().name() == "subscriber-interceptor-dropped" {
                    captured_short_circuit
                        .lock()
                        .expect("sequence should lock")
                        .push("dropped".to_string());
                    Ok(())
                } else {
                    chain.proceed(event)
                }
            },
        )
        .expect("subscriber interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let handled_topic = create_topic("subscriber-interceptor");
    let dropped_topic = create_topic("subscriber-interceptor-dropped");
    let captured_handler = Arc::clone(&sequence);

    bus.subscribe("sub-1", &handled_topic, move |event| {
        assert_eq!(
            event.headers().get("intercepted"),
            Some(&"true".to_string())
        );
        captured_handler
            .lock()
            .expect("sequence should lock")
            .push(format!("handler:{}", event.payload()));
        Ok(())
    })
    .expect("subscribe should work");
    let dropped_called = Arc::new(AtomicBool::new(false));
    let captured_dropped_called = Arc::clone(&dropped_called);
    bus.subscribe("sub-2", &dropped_topic, move |_| {
        captured_dropped_called.store(true, Ordering::SeqCst);
        Ok(())
    })
    .expect("subscribe should work");

    bus.publish(&handled_topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&handled_topic)
        .expect("handled topic should become idle");
    bus.publish(&dropped_topic, "ignored".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&dropped_topic)
        .expect("dropped topic should become idle");

    assert!(!dropped_called.load(Ordering::SeqCst));
    assert_eq!(
        *sequence.lock().expect("sequence should lock"),
        vec![
            "before:payload".to_string(),
            "handler:payload".to_string(),
            "after".to_string(),
            "before:ignored".to_string(),
            "dropped".to_string(),
            "after".to_string(),
        ]
    );
}

#[test]
fn test_configured_handler_pool_limits_concurrent_subscriber_work() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("pool size should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("single-worker-pool");
    let current = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let captured_current = Arc::clone(&current);
    let captured_max_seen = Arc::clone(&max_seen);
    let captured_release_rx = Arc::clone(&release_rx);

    bus.subscribe("sub-1", &topic, move |event| {
        let running = captured_current.fetch_add(1, Ordering::SeqCst) + 1;
        captured_max_seen.fetch_max(running, Ordering::SeqCst);
        if event.payload() == "first" {
            started_tx
                .send(())
                .expect("started signal should be received");
            captured_release_rx
                .lock()
                .expect("release receiver should lock")
                .recv()
                .expect("release signal should arrive");
        }
        captured_current.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    })
    .expect("subscribe should work");

    bus.publish(&topic, "first".to_string())
        .expect("first publish should work");
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first event should start");
    bus.publish(&topic, "second".to_string())
        .expect("second publish should queue");
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(max_seen.load(Ordering::SeqCst), 1);

    release_tx.send(()).expect("release should send");
    bus.wait_for_idle(&topic).expect("topic should become idle");
    assert_eq!(max_seen.load(Ordering::SeqCst), 1);
}

#[test]
fn test_global_subscriber_interceptor_wraps_all_payload_types_and_can_short_circuit()
 {
    let sequence = Arc::new(Mutex::new(Vec::<String>::new()));
    let captured_sequence = Arc::clone(&sequence);
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_global_subscriber_interceptor(
            move |metadata: EventEnvelopeMetadata,
                  chain: SubscriberInterceptorAnyChain| {
                captured_sequence
                    .lock()
                    .expect("sequence should lock")
                    .push(format!("before:{}", metadata.topic_name()));
                if metadata.topic_name() == "global-subscriber-dropped" {
                    captured_sequence
                        .lock()
                        .expect("sequence should lock")
                        .push("dropped".to_string());
                    Ok(())
                } else {
                    let result = chain.proceed();
                    captured_sequence
                        .lock()
                        .expect("sequence should lock")
                        .push("after".to_string());
                    result
                }
            },
        )
        .expect("global subscriber interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let string_topic = create_topic("global-subscriber-string");
    let number_topic = Topic::<i32>::try_new("global-subscriber-number")
        .expect("number topic should build");
    let dropped_topic = create_topic("global-subscriber-dropped");
    let captured_string_sequence = Arc::clone(&sequence);
    bus.subscribe("string-sub", &string_topic, move |event| {
        captured_string_sequence
            .lock()
            .expect("sequence should lock")
            .push(format!("handler:{}", event.payload()));
        Ok(())
    })
    .expect("string subscription should register");
    let captured_number_sequence = Arc::clone(&sequence);
    bus.subscribe("number-sub", &number_topic, move |event| {
        captured_number_sequence
            .lock()
            .expect("sequence should lock")
            .push(format!("handler:{}", event.payload()));
        Ok(())
    })
    .expect("number subscription should register");
    let dropped_called = Arc::new(AtomicBool::new(false));
    let captured_dropped_called = Arc::clone(&dropped_called);
    bus.subscribe("dropped-sub", &dropped_topic, move |_| {
        captured_dropped_called.store(true, Ordering::SeqCst);
        Ok(())
    })
    .expect("dropped subscription should register");

    bus.publish(&string_topic, "payload".to_string())
        .expect("string publish should work");
    bus.publish(&number_topic, 9)
        .expect("number publish should work");
    bus.publish(&dropped_topic, "ignored".to_string())
        .expect("dropped publish should work");
    bus.wait_for_idle(&string_topic)
        .expect("string topic should become idle");
    bus.wait_for_idle(&number_topic)
        .expect("number topic should become idle");
    bus.wait_for_idle(&dropped_topic)
        .expect("dropped topic should become idle");

    let mut observed = sequence.lock().expect("sequence should lock").clone();
    observed.sort();
    assert_eq!(
        observed,
        vec![
            "after".to_string(),
            "after".to_string(),
            "before:global-subscriber-dropped".to_string(),
            "before:global-subscriber-number".to_string(),
            "before:global-subscriber-string".to_string(),
            "dropped".to_string(),
            "handler:9".to_string(),
            "handler:payload".to_string(),
        ]
    );
    assert!(!dropped_called.load(Ordering::SeqCst));
}

#[test]
fn test_global_subscriber_interceptor_error_is_observed() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_global_subscriber_interceptor(
            |_metadata: EventEnvelopeMetadata,
             _chain: SubscriberInterceptorAnyChain| {
                Err(EventBusError::handler_failed(
                    "global subscriber interceptor failed",
                ))
            },
        )
        .expect("global subscriber interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("global-subscriber-interceptor-error");
    let subscribe_errors = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_subscribe_errors = Arc::clone(&subscribe_errors);
    let options = SubscribeOptions::<String>::builder()
        .error_handler(
            move |_subscriber_id, _envelope, error, acknowledgement| {
                captured_subscribe_errors
                    .lock()
                    .expect("subscribe errors should lock")
                    .push(error.clone());
                acknowledgement.ack();
                Ok(())
            },
        )
        .build();
    bus.subscribe_with_options("sub", &topic, |_event| Ok(()), options)
        .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    let subscribe_errors = subscribe_errors
        .lock()
        .expect("subscribe errors should lock");
    assert!(subscribe_errors.iter().any(|error| matches!(
        error,
        EventBusError::InterceptorFailed { phase, message }
            if *phase == "subscribe" && message.contains("global subscriber interceptor failed")
    )));
}

#[test]
fn test_global_subscriber_interceptor_preserves_downstream_handler_error() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_global_subscriber_interceptor(
            |_metadata: EventEnvelopeMetadata,
             chain: SubscriberInterceptorAnyChain| chain.proceed(),
        )
        .expect("global subscriber interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("global-subscriber-preserves-handler-error");
    let subscribe_errors = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_subscribe_errors = Arc::clone(&subscribe_errors);
    let options = SubscribeOptions::<String>::builder()
        .error_handler(
            move |_subscriber_id, _envelope, error, acknowledgement| {
                captured_subscribe_errors
                    .lock()
                    .expect("subscribe errors should lock")
                    .push(error.clone());
                acknowledgement.ack();
                Ok(())
            },
        )
        .build();
    bus.subscribe_with_options(
        "sub",
        &topic,
        |_event| {
            Err(EventBusError::handler_failed(
                "handler failed behind global interceptor",
            ))
        },
        options,
    )
    .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    let subscribe_errors = subscribe_errors
        .lock()
        .expect("subscribe errors should lock");
    assert_eq!(
        subscribe_errors.as_slice(),
        [EventBusError::handler_failed(
            "handler failed behind global interceptor"
        )]
    );
}

#[test]
fn test_global_subscriber_interceptor_preserves_downstream_handler_panic() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_global_subscriber_interceptor(
            |_metadata: EventEnvelopeMetadata,
             chain: SubscriberInterceptorAnyChain| chain.proceed(),
        )
        .expect("global subscriber interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("global-subscriber-preserves-handler-panic");
    let subscribe_errors = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_subscribe_errors = Arc::clone(&subscribe_errors);
    let options = SubscribeOptions::<String>::builder()
        .error_handler(
            move |_subscriber_id, _envelope, error, acknowledgement| {
                captured_subscribe_errors
                    .lock()
                    .expect("subscribe errors should lock")
                    .push(error.clone());
                acknowledgement.ack();
                Ok(())
            },
        )
        .build();
    bus.subscribe_with_options(
        "sub",
        &topic,
        |_event| -> EventBusResult<()> {
            panic!("handler panic behind global interceptor");
        },
        options,
    )
    .expect("subscription should register");
    {
        let _panic_hook_guard = PanicHookGuard::suppress();
        bus.publish(&topic, "payload".to_string())
            .expect("publish should succeed");
        bus.wait_for_idle(&topic).expect("topic should become idle");
    }

    let subscribe_errors = subscribe_errors
        .lock()
        .expect("subscribe errors should lock");
    assert_eq!(
        subscribe_errors.as_slice(),
        [EventBusError::handler_panicked()]
    );
}

#[test]
fn test_subscriber_interceptor_owned_equal_error_is_reported_as_interceptor_failure()
 {
    let mut factory = LocalEventBusFactory::new();
    let downstream_keepalive = Arc::new(Mutex::new(None::<EventBusError>));
    let captured_downstream_keepalive = Arc::clone(&downstream_keepalive);
    factory
        .add_subscriber_interceptor::<String, _>(
            move |event: EventEnvelope<String>,
                  chain: SubscriberInterceptorChain<String>| {
                if let Err(error) = chain.proceed(event) {
                    captured_downstream_keepalive
                        .lock()
                        .expect("downstream error should lock")
                        .replace(error);
                }
                Err(EventBusError::handler_failed("ambiguous subscriber error"))
            },
        )
        .expect("subscriber interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("subscriber-interceptor-equal-owned-error");
    let subscribe_errors = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_subscribe_errors = Arc::clone(&subscribe_errors);
    let options = SubscribeOptions::<String>::builder()
        .error_handler(
            move |_subscriber_id, _envelope, error, acknowledgement| {
                captured_subscribe_errors
                    .lock()
                    .expect("subscribe errors should lock")
                    .push(error.clone());
                acknowledgement.ack();
                Ok(())
            },
        )
        .build();
    bus.subscribe_with_options(
        "sub",
        &topic,
        |_event| {
            Err(EventBusError::handler_failed("ambiguous subscriber error"))
        },
        options,
    )
    .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    let subscribe_errors = subscribe_errors
        .lock()
        .expect("subscribe errors should lock");
    assert!(matches!(
        subscribe_errors.as_slice(),
        [EventBusError::InterceptorFailed { phase, message }]
            if *phase == "subscribe" && message.contains("ambiguous subscriber error")
    ));
}

#[test]
fn test_global_subscriber_interceptor_panic_is_observed() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_global_subscriber_interceptor(
            |_metadata: EventEnvelopeMetadata,
             _chain: SubscriberInterceptorAnyChain|
             -> EventBusResult<()> {
                panic!("global subscriber interceptor panic");
            },
        )
        .expect("global subscriber interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("global-subscriber-interceptor-panic");
    let subscribe_errors = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_subscribe_errors = Arc::clone(&subscribe_errors);
    let options = SubscribeOptions::<String>::builder()
        .error_handler(
            move |_subscriber_id, _envelope, error, acknowledgement| {
                captured_subscribe_errors
                    .lock()
                    .expect("subscribe errors should lock")
                    .push(error.clone());
                acknowledgement.ack();
                Ok(())
            },
        )
        .build();
    bus.subscribe_with_options("sub", &topic, |_event| Ok(()), options)
        .expect("subscription should register");
    {
        let _panic_hook_guard = PanicHookGuard::suppress();
        bus.publish(&topic, "payload".to_string())
            .expect("publish should succeed");
        bus.wait_for_idle(&topic).expect("topic should become idle");
    }

    let subscribe_errors = subscribe_errors
        .lock()
        .expect("subscribe errors should lock");
    assert!(subscribe_errors.iter().any(|error| matches!(
        error,
        EventBusError::InterceptorFailed { phase, message }
            if *phase == "subscribe" && message.contains("global subscriber interceptor panicked")
    )));
}

#[test]
fn test_subscriber_interceptor_error_is_reported_as_interceptor_failure() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_subscriber_interceptor::<String, _>(
            |_event: EventEnvelope<String>,
             _chain: SubscriberInterceptorChain<String>| {
                Err(EventBusError::handler_failed(
                    "typed subscriber interceptor failed",
                ))
            },
        )
        .expect("subscriber interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("subscriber-interceptor-error");
    let subscribe_errors = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_subscribe_errors = Arc::clone(&subscribe_errors);
    let handler_called = Arc::new(AtomicBool::new(false));
    let captured_handler_called = Arc::clone(&handler_called);
    let options = SubscribeOptions::<String>::builder()
        .error_handler(
            move |_subscriber_id, _envelope, error, acknowledgement| {
                captured_subscribe_errors
                    .lock()
                    .expect("subscribe errors should lock")
                    .push(error.clone());
                acknowledgement.ack();
                Ok(())
            },
        )
        .build();
    bus.subscribe_with_options(
        "sub",
        &topic,
        move |_event| {
            captured_handler_called.store(true, Ordering::SeqCst);
            Ok(())
        },
        options,
    )
    .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should succeed");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert!(!handler_called.load(Ordering::SeqCst));
    let subscribe_errors = subscribe_errors
        .lock()
        .expect("subscribe errors should lock");
    assert!(matches!(
        subscribe_errors.as_slice(),
        [EventBusError::InterceptorFailed { phase, message }]
            if *phase == "subscribe" && message.contains("typed subscriber interceptor failed")
    ));
}

#[test]
fn test_subscriber_interceptor_panic_is_reported_as_interceptor_failure() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_subscriber_interceptor::<String, _>(
            |_event: EventEnvelope<String>,
             _chain: SubscriberInterceptorChain<String>|
             -> EventBusResult<()> {
                panic!("typed subscriber interceptor panic");
            },
        )
        .expect("subscriber interceptor should register");
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("subscriber-interceptor-panic");
    let subscribe_errors = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_subscribe_errors = Arc::clone(&subscribe_errors);
    let options = SubscribeOptions::<String>::builder()
        .error_handler(
            move |_subscriber_id, _envelope, error, acknowledgement| {
                captured_subscribe_errors
                    .lock()
                    .expect("subscribe errors should lock")
                    .push(error.clone());
                acknowledgement.ack();
                Ok(())
            },
        )
        .build();
    bus.subscribe_with_options("sub", &topic, |_event| Ok(()), options)
        .expect("subscription should register");
    {
        let _panic_hook_guard = PanicHookGuard::suppress();
        bus.publish(&topic, "payload".to_string())
            .expect("publish should succeed");
        bus.wait_for_idle(&topic).expect("topic should become idle");
    }

    let subscribe_errors = subscribe_errors
        .lock()
        .expect("subscribe errors should lock");
    assert!(matches!(
        subscribe_errors.as_slice(),
        [EventBusError::InterceptorFailed { phase, message }]
            if *phase == "subscribe" && message.contains("subscriber interceptor panicked")
    ));
}

#[test]
fn test_publish_all_delivers_each_envelope() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("pool size should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("batch");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    bus.subscribe("sub-1", &topic, move |event| {
        captured
            .lock()
            .expect("received events should lock")
            .push(event);
        Ok(())
    })
    .expect("subscribe should work");

    let envelopes = ["batch-2", "batch-1"]
        .into_iter()
        .map(|payload| {
            EventEnvelope::create(topic.clone(), payload.to_string())
        })
        .collect::<Vec<_>>();
    let batch_result = bus
        .publish_all(envelopes)
        .expect("batch publish should work");
    assert_eq!(batch_result.total_count(), 2);
    assert_eq!(batch_result.accepted_count(), 2);
    assert_eq!(batch_result.dropped_count(), 0);
    assert!(batch_result.is_success());
    bus.wait_for_idle(&topic).expect("topic should become idle");

    let payloads = received
        .lock()
        .expect("received events should lock")
        .iter()
        .map(|event| event.payload().clone())
        .collect::<Vec<_>>();
    assert_eq!(payloads, vec!["batch-2".to_string(), "batch-1".to_string()]);
}

#[test]
fn test_publish_all_reports_dropped_envelopes_separately_from_accepted() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_publisher_interceptor::<String, _>(
            |event: EventEnvelope<String>| {
                if event.payload() == "drop" {
                    None
                } else {
                    Some(event)
                }
            },
        )
        .expect("interceptor should register");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("batch-dropped");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);
    bus.subscribe("sub", &topic, move |event| {
        captured
            .lock()
            .expect("received events should lock")
            .push(event.payload().clone());
        Ok(())
    })
    .expect("subscription should register");

    let envelopes = ["keep", "drop"]
        .into_iter()
        .map(|payload| {
            EventEnvelope::create(topic.clone(), payload.to_string())
        })
        .collect::<Vec<_>>();
    let batch_result = bus
        .publish_all(envelopes)
        .expect("batch with dropped envelope should return summary");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(batch_result.total_count(), 2);
    assert_eq!(batch_result.accepted_count(), 1);
    assert_eq!(batch_result.dropped_count(), 1);
    assert_eq!(batch_result.failure_count(), 0);
    assert!(batch_result.is_success());
    assert_eq!(
        received
            .lock()
            .expect("received events should lock")
            .as_slice(),
        ["keep".to_string()]
    );
}

#[test]
fn test_publish_all_reports_failures_and_continues_remaining_envelopes() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .add_publisher_interceptor::<String, _>(
            |event: EventEnvelope<String>| {
                if event.payload() == "bad" {
                    Err(EventBusError::interceptor_failed(
                        "publish",
                        "bad payload rejected",
                    ))
                } else {
                    Ok(Some(event))
                }
            },
        )
        .expect("interceptor should register");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("batch-best-effort");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);
    bus.subscribe("sub", &topic, move |event| {
        captured
            .lock()
            .expect("received events should lock")
            .push(event.payload().clone());
        Ok(())
    })
    .expect("subscription should register");

    let envelopes = ["ok-1", "bad", "ok-2"]
        .into_iter()
        .map(|payload| {
            EventEnvelope::create(topic.clone(), payload.to_string())
        })
        .collect::<Vec<_>>();
    let failed_event_id = envelopes[1].id().to_string();

    let batch_result = bus
        .publish_all(envelopes)
        .expect("best-effort batch should return a result summary");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(batch_result.total_count(), 3);
    assert_eq!(batch_result.accepted_count(), 2);
    assert_eq!(batch_result.dropped_count(), 0);
    assert_eq!(batch_result.failure_count(), 1);
    assert!(!batch_result.is_success());
    assert_eq!(batch_result.failures()[0].index(), 1);
    assert_eq!(batch_result.failures()[0].event_id(), failed_event_id);
    assert_eq!(
        batch_result.failures()[0].error().kind(),
        "interceptor_failed"
    );
    let mut received = received
        .lock()
        .expect("received events should lock")
        .clone();
    received.sort();
    assert_eq!(
        received.as_slice(),
        ["ok-1".to_string(), "ok-2".to_string()]
    );
}

#[test]
fn test_publish_accepts_policy_without_execution_timeout() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("publish-attempt-timeout");
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let captured_handler_calls = Arc::clone(&handler_calls);
    bus.subscribe("sub-1", &topic, move |_event| {
        captured_handler_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    })
    .expect("subscribe should work");
    let options = PublishOptions::<String>::builder()
        .retry_options(retry_options_with_attempt_timeout())
        .build();

    bus.publish_envelope_with_options(
        EventEnvelope::create(topic, "payload".to_string()),
        options,
    )
    .expect("policy should be accepted");
    let _ = handler_calls;
}

#[test]
fn test_publish_all_accepts_merged_default_publish_policy() {
    let mut factory = LocalEventBusFactory::new();
    factory.set_default_publish_options::<String>(
        PublishOptions::builder()
            .retry_options(retry_options_with_attempt_timeout())
            .build(),
    );
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("publish-all-default-attempt-timeout");
    let envelopes = ["first", "second"]
        .into_iter()
        .map(|payload| {
            EventEnvelope::create(topic.clone(), payload.to_string())
        })
        .collect::<Vec<_>>();

    let result = bus
        .publish_all(envelopes)
        .expect("policy should be accepted");
    assert_eq!(result.accepted_count(), 2);
}

#[test]
fn test_subscribe_accepts_policy_without_execution_timeout() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("subscribe-attempt-timeout");
    let options = SubscribeOptions::<String>::builder()
        .retry_options(retry_options_with_attempt_timeout())
        .build();

    let subscription = bus
        .subscribe_with_options("sub-1", &topic, |_event| Ok(()), options)
        .expect("policy should be accepted");
    drop(subscription);
}

#[test]
fn test_factory_creates_started_bus_with_default_options() {
    let mut factory = LocalEventBusFactory::new();
    factory.set_default_subscribe_options(
        SubscribeOptions::<String>::builder().priority(5).build(),
    );

    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("factory");
    let subscription = bus
        .subscribe("sub-1", &topic, |_| Ok(()))
        .expect("factory bus should accept subscriptions");

    assert_eq!(subscription.options().priority(), 5);
}

#[test]
fn test_subscriber_priority_controls_delivery_order() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("pool size should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("priority-order");
    let blocker_topic = create_topic("priority-order-blocker");
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let (started_tx, started_rx) = mpsc::channel();
    let captured_release = Arc::clone(&release);
    bus.subscribe("blocker", &blocker_topic, move |_| {
        started_tx
            .send(())
            .expect("blocker start should be observed");
        wait_for_gate(&captured_release);
    })
    .expect("blocker subscriber should register");
    let sequence = Arc::new(Mutex::new(Vec::<String>::new()));
    let low_sequence = Arc::clone(&sequence);
    bus.subscribe_with_options(
        "low",
        &topic,
        move |_| {
            low_sequence
                .lock()
                .expect("sequence should lock")
                .push("low".to_string());
        },
        SubscribeOptions::<String>::builder().priority(1).build(),
    )
    .expect("low priority subscriber should register");
    let high_sequence = Arc::clone(&sequence);
    bus.subscribe_with_options(
        "high",
        &topic,
        move |_| {
            high_sequence
                .lock()
                .expect("sequence should lock")
                .push("high".to_string());
        },
        SubscribeOptions::<String>::builder().priority(10).build(),
    )
    .expect("high priority subscriber should register");

    bus.publish(&blocker_topic, "blocked".to_string())
        .expect("blocker publish should work");
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("blocker should start");
    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    release_gate(&release);
    bus.wait_for_idle(&blocker_topic)
        .expect("blocker topic should become idle");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(
        sequence.lock().expect("sequence should lock").as_slice(),
        ["high", "low"]
    );
}

#[test]
fn test_subscriber_equal_priority_preserves_registration_order() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("pool size should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("equal-priority-order");
    let blocker_topic = create_topic("equal-priority-order-blocker");
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let (started_tx, started_rx) = mpsc::channel();
    let captured_release = Arc::clone(&release);
    bus.subscribe("blocker", &blocker_topic, move |_| {
        started_tx
            .send(())
            .expect("blocker start should be observed");
        wait_for_gate(&captured_release);
    })
    .expect("blocker subscriber should register");
    let sequence = Arc::new(Mutex::new(Vec::<String>::new()));
    let first_sequence = Arc::clone(&sequence);
    bus.subscribe_with_options(
        "first",
        &topic,
        move |_| {
            first_sequence
                .lock()
                .expect("sequence should lock")
                .push("first".to_string());
        },
        SubscribeOptions::<String>::builder().priority(5).build(),
    )
    .expect("first subscriber should register");
    let second_sequence = Arc::clone(&sequence);
    bus.subscribe_with_options(
        "second",
        &topic,
        move |_| {
            second_sequence
                .lock()
                .expect("sequence should lock")
                .push("second".to_string());
        },
        SubscribeOptions::<String>::builder().priority(5).build(),
    )
    .expect("second subscriber should register");

    bus.publish(&blocker_topic, "blocked".to_string())
        .expect("blocker publish should work");
    started_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("blocker should start");
    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    release_gate(&release);
    bus.wait_for_idle(&blocker_topic)
        .expect("blocker topic should become idle");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(
        sequence.lock().expect("sequence should lock").as_slice(),
        ["first", "second"]
    );
}

#[test]
fn test_factory_applies_default_publish_options_and_interceptors() {
    let mut factory = LocalEventBusFactory::new();
    let publish_errors = Arc::new(AtomicUsize::new(0));
    let captured_publish_errors = Arc::clone(&publish_errors);
    factory.set_default_publish_options::<String>(
        PublishOptions::builder()
            .error_handler(move |_event, error| {
                assert_eq!(error, &EventBusError::not_started());
                captured_publish_errors.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .build(),
    );
    factory
        .add_publisher_interceptor::<String, _>(
            |event: EventEnvelope<String>| {
                Some(event.with_header("factory", "true"))
            },
        )
        .expect("factory publisher interceptor should register");
    factory
        .add_subscriber_interceptor::<String, _>(
            |event: EventEnvelope<String>,
             chain: SubscriberInterceptorChain<String>| {
                chain.proceed(event.with_header("subscriber-factory", "true"))
            },
        )
        .expect("factory subscriber interceptor should register");

    let stopped_bus = factory.create();
    let topic = create_topic("factory-defaults");
    assert_eq!(
        stopped_bus
            .publish(&topic, "stopped".to_string())
            .expect_err("stopped publish should fail"),
        EventBusError::not_started()
    );
    assert_eq!(publish_errors.load(Ordering::SeqCst), 1);

    let bus = factory.create_started().expect("factory should start bus");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);
    bus.subscribe("sub-1", &topic, move |event| {
        captured
            .lock()
            .expect("received events should lock")
            .push(event);
        Ok(())
    })
    .expect("subscribe should work");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    let events = received.lock().expect("received events should lock");
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].headers().get("factory"),
        Some(&"true".to_string())
    );
    assert_eq!(
        events[0].headers().get("subscriber-factory"),
        Some(&"true".to_string())
    );
}

#[test]
fn test_publish_with_options_merges_factory_default_publish_error_handlers() {
    let mut factory = LocalEventBusFactory::new();
    let default_errors = Arc::new(AtomicUsize::new(0));
    let explicit_errors = Arc::new(AtomicUsize::new(0));
    let captured_default_errors = Arc::clone(&default_errors);
    let captured_explicit_errors = Arc::clone(&explicit_errors);
    factory.set_default_publish_options::<String>(
        PublishOptions::builder()
            .error_handler(move |_event, error| {
                assert_eq!(error, &EventBusError::not_started());
                captured_default_errors.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .build(),
    );
    let bus = factory.create();
    let topic = create_topic("factory-default-publish-merge");
    let options = PublishOptions::<String>::builder()
        .error_handler(move |_event, error| {
            assert_eq!(error, &EventBusError::not_started());
            captured_explicit_errors.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .build();

    let error = bus
        .publish_envelope_with_options(
            EventEnvelope::create(topic, "payload".to_string()),
            options,
        )
        .expect_err("stopped publish should fail");

    assert_eq!(error, EventBusError::not_started());
    assert_eq!(default_errors.load(Ordering::SeqCst), 1);
    assert_eq!(explicit_errors.load(Ordering::SeqCst), 1);
}

#[test]
fn test_subscribe_with_options_merges_factory_default_subscribe_options() {
    let mut factory = LocalEventBusFactory::new();
    let default_errors = Arc::new(AtomicUsize::new(0));
    let explicit_errors = Arc::new(AtomicUsize::new(0));
    let captured_default_errors = Arc::clone(&default_errors);
    let captured_explicit_errors = Arc::clone(&explicit_errors);
    factory.set_default_subscribe_options::<String>(
        SubscribeOptions::builder()
            .priority(7)
            .error_handler(move |_subscriber, _event, _error, _ack| {
                captured_default_errors.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .build(),
    );
    let bus = factory.create_started().expect("bus should start");
    let topic = create_topic("factory-default-subscribe-merge");
    let subscription = bus
        .subscribe_with_options(
            "sub",
            &topic,
            |_event| Err(EventBusError::handler_failed("expected failure")),
            SubscribeOptions::<String>::builder()
                .error_handler(move |_subscriber, _event, _error, _ack| {
                    captured_explicit_errors.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .build(),
        )
        .expect("subscription should merge defaults");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(subscription.options().priority(), 7);
    assert_eq!(default_errors.load(Ordering::SeqCst), 1);
    assert_eq!(explicit_errors.load(Ordering::SeqCst), 1);
}

#[test]
fn test_publish_error_handler_failure_is_observed_and_isolated() {
    let bus = LocalEventBus::new();
    let topic = create_topic("publish-error-observed");
    let observed = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_observed = Arc::clone(&observed);
    bus.add_error_observer(move |error| {
        captured_observed
            .lock()
            .expect("observed errors should lock")
            .push(error.clone());
    })
    .expect("error observer should register");
    let second_handlers = Arc::new(AtomicUsize::new(0));
    let captured_second_handlers = Arc::clone(&second_handlers);
    let options = PublishOptions::<String>::builder()
        .error_handler(|_event, _error| {
            Err(EventBusError::handler_failed(
                "publish error handler failed",
            ))
        })
        .error_handler(move |_event, error| {
            assert_eq!(error, &EventBusError::not_started());
            captured_second_handlers.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .build();

    assert_eq!(
        bus.publish_envelope_with_options(
            EventEnvelope::create(topic, "payload".to_string()),
            options,
        )
        .expect_err("stopped bus should reject publish"),
        EventBusError::not_started()
    );

    assert_eq!(second_handlers.load(Ordering::SeqCst), 1);
    let observed = observed.lock().expect("observed errors should lock");
    assert!(observed.iter().any(|error| matches!(
        error,
        EventBusError::ErrorHandlerFailed { phase, message }
            if *phase == "publish" && message.contains("publish error handler failed")
    )));
}

#[test]
fn test_subscribe_error_handler_failure_is_observed() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("observed-error-handler");
    let observed = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_observed = Arc::clone(&observed);
    bus.add_error_observer(move |error| {
        captured_observed
            .lock()
            .expect("observed errors should lock")
            .push(error.clone());
    })
    .expect("error observer should register");
    let options = SubscribeOptions::<String>::builder()
        .error_handler(|_subscriber_id, _envelope, _error, _acknowledgement| {
            Err(EventBusError::handler_failed("custom error handler failed"))
        })
        .build();

    bus.subscribe_with_options(
        "sub-1",
        &topic,
        |_| Err(EventBusError::handler_failed("handler failed")),
        options,
    )
    .expect("subscribe should work");
    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    let observed = observed.lock().expect("observed errors should lock");
    assert!(observed.iter().any(|error| matches!(
        error,
        EventBusError::ErrorHandlerFailed { phase, message }
            if *phase == "subscribe" && message.contains("custom error handler failed")
    )));
}

#[test]
fn test_subscribe_error_handler_panic_does_not_block_later_ack() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("subscribe-error-handler-panic");
    let dead_letter_topic =
        create_dead_letter_topic("dlq.subscribe-error-handler-panic");
    let observed = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_observed = Arc::clone(&observed);
    bus.add_error_observer(move |error| {
        captured_observed
            .lock()
            .expect("observed errors should lock")
            .push(error.clone());
    })
    .expect("error observer should register");
    let second_handlers = Arc::new(AtomicUsize::new(0));
    let captured_second_handlers = Arc::clone(&second_handlers);
    let dead_letters =
        Arc::new(Mutex::new(Vec::<EventEnvelope<DeadLetterPayload>>::new()));
    let dead_letter_target = dead_letter_topic.clone();
    let options = SubscribeOptions::<String>::builder()
        .error_handler(
            |_subscriber_id,
             _envelope,
             _error,
             _acknowledgement|
             -> qubit_event_bus::EventBusResult<()> {
                panic!("subscribe error handler panic");
            },
        )
        .error_handler(
            move |_subscriber_id, _envelope, _error, acknowledgement| {
                acknowledgement.ack();
                captured_second_handlers.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .dead_letter_strategy(move |subscriber_id, failed, error, _options| {
            Ok(Some(EventEnvelope::create(
                dead_letter_target.clone(),
                DeadLetterRecord::from_failure(subscriber_id, failed, error),
            )))
        })
        .build();

    bus.subscribe_with_options(
        "sub-1",
        &topic,
        |_| Err(EventBusError::handler_failed("handler failed")),
        options,
    )
    .expect("subscribe should work");
    let captured_dead_letters = Arc::clone(&dead_letters);
    bus.subscribe("dlq-sub", &dead_letter_topic, move |event| {
        captured_dead_letters
            .lock()
            .expect("dead letters should lock")
            .push(event);
        Ok(())
    })
    .expect("dead letter subscriber should register");
    {
        let _panic_hook_guard = PanicHookGuard::suppress();
        bus.publish(&topic, "payload".to_string())
            .expect("publish should work");
        bus.wait_for_idle(&topic).expect("topic should become idle");
        bus.wait_for_idle(&dead_letter_topic)
            .expect("dead letter topic should become idle");
    }

    assert_eq!(second_handlers.load(Ordering::SeqCst), 1);
    assert!(
        dead_letters
            .lock()
            .expect("dead letters should lock")
            .is_empty()
    );
    let observed = observed.lock().expect("observed errors should lock");
    assert!(observed.iter().any(|error| matches!(
        error,
        EventBusError::ErrorHandlerFailed { phase, message }
            if *phase == "subscribe" && message.contains("panicked")
    )));
}

#[test]
fn test_dead_letter_strategy_error_is_observed() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("dead-letter-strategy-error");
    let observed = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_observed = Arc::clone(&observed);
    bus.add_error_observer(move |error| {
        captured_observed
            .lock()
            .expect("observed errors should lock")
            .push(error.clone());
    })
    .expect("error observer should register");
    let options = SubscribeOptions::<String>::builder()
        .dead_letter_strategy(|_subscriber_id, _failed, _error, _options| {
            Err(EventBusError::handler_failed("dead-letter strategy failed"))
        })
        .build();

    bus.subscribe_with_options(
        "sub-1",
        &topic,
        |_| Err(EventBusError::handler_failed("handler failed")),
        options,
    )
    .expect("subscribe should work");
    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    let observed = observed.lock().expect("observed errors should lock");
    assert!(observed.iter().any(|error| matches!(
        error,
        EventBusError::DeadLetterFailed { message }
            if message.contains("dead-letter strategy failed")
    )));
}

#[test]
fn test_dead_letter_strategy_dead_letter_error_is_preserved() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("dead-letter-strategy-preserve-error");
    let observed = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_observed = Arc::clone(&observed);
    bus.add_error_observer(move |error| {
        captured_observed
            .lock()
            .expect("observed errors should lock")
            .push(error.clone());
    })
    .expect("error observer should register");
    let options = SubscribeOptions::<String>::builder()
        .dead_letter_strategy(|_subscriber_id, _failed, _error, _options| {
            Err(EventBusError::dead_letter_failed(
                "already dead-letter failed",
            ))
        })
        .build();

    bus.subscribe_with_options(
        "sub-1",
        &topic,
        |_| Err(EventBusError::handler_failed("handler failed")),
        options,
    )
    .expect("subscribe should work");
    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    let observed = observed.lock().expect("observed errors should lock");
    assert!(observed.iter().any(|error| matches!(
        error,
        EventBusError::DeadLetterFailed { message }
            if message == "already dead-letter failed"
    )));
}

#[test]
fn test_dead_letter_strategy_panic_is_observed() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("dead-letter-strategy-panic");
    let observed = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_observed = Arc::clone(&observed);
    bus.add_error_observer(move |error| {
        captured_observed
            .lock()
            .expect("observed errors should lock")
            .push(error.clone());
    })
    .expect("error observer should register");
    let options = SubscribeOptions::<String>::builder()
        .dead_letter_strategy(|_subscriber_id, _failed, _error, _options| {
            panic!("dead-letter strategy panic");
        })
        .build();

    bus.subscribe_with_options(
        "sub-1",
        &topic,
        |_| Err(EventBusError::handler_failed("handler failed")),
        options,
    )
    .expect("subscribe should work");
    {
        let _panic_hook_guard = PanicHookGuard::suppress();
        bus.publish(&topic, "payload".to_string())
            .expect("publish should work");
        bus.wait_for_idle(&topic).expect("topic should become idle");
    }

    let observed = observed.lock().expect("observed errors should lock");
    assert!(observed.iter().any(|error| matches!(
        error,
        EventBusError::DeadLetterFailed { message } if message.contains("panicked")
    )));
}

#[test]
fn test_bounded_handler_queue_rejects_when_saturated() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    factory
        .set_subscription_handler_queue_capacity(Some(1))
        .expect("bounded queue should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("bounded-handler-queue");
    let started = Arc::new((Mutex::new(0_usize), Condvar::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let captured_started = Arc::clone(&started);
    let captured_release = Arc::clone(&release);
    bus.subscribe("sub", &topic, move |_event| {
        let (started_lock, started_condvar) = &*captured_started;
        let mut started_count =
            started_lock.lock().expect("started count should lock");
        *started_count += 1;
        started_condvar.notify_all();
        drop(started_count);

        let (release_lock, release_condvar) = &*captured_release;
        let mut released =
            release_lock.lock().expect("release gate should lock");
        while !*released {
            released = release_condvar
                .wait(released)
                .expect("release gate wait should not poison");
        }
    })
    .expect("subscription should register");

    bus.publish(&topic, "first".to_string())
        .expect("first publish should occupy the worker");
    wait_for_count(&started, 1);
    bus.publish(&topic, "second".to_string())
        .expect("second publish should fill the queue");
    let error = bus
        .publish(&topic, "third".to_string())
        .expect_err("third publish should be rejected by the bounded queue");
    release_gate(&release);
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert!(matches!(error, EventBusError::ExecutionRejected { .. }));
    assert_eq!(*started.0.lock().expect("started count should lock"), 2);
}

#[test]
fn test_cancelled_queued_delivery_skips_handler() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    factory
        .set_subscription_handler_queue_capacity(Some(2))
        .expect("bounded queue should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("cancelled-queued-delivery");
    let started = Arc::new((Mutex::new(0_usize), Condvar::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let captured_started = Arc::clone(&started);
    let captured_release = Arc::clone(&release);
    let subscription = bus
        .subscribe("sub", &topic, move |_event| {
            let (started_lock, started_condvar) = &*captured_started;
            let mut started_count =
                started_lock.lock().expect("started count should lock");
            *started_count += 1;
            started_condvar.notify_all();
            drop(started_count);

            let (release_lock, release_condvar) = &*captured_release;
            let mut released =
                release_lock.lock().expect("release gate should lock");
            while !*released {
                released = release_condvar
                    .wait(released)
                    .expect("release gate wait should not poison");
            }
        })
        .expect("subscription should register");

    bus.publish(&topic, "first".to_string())
        .expect("first publish should occupy the worker");
    wait_for_count(&started, 1);
    bus.publish(&topic, "second".to_string())
        .expect("second publish should queue behind the worker");
    subscription
        .cancel()
        .expect("subscription cancellation should succeed");
    release_gate(&release);
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(*started.0.lock().expect("started count should lock"), 1);
}

#[test]
fn test_cancelled_queued_delayed_delivery_skips_delay_wait() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    factory
        .set_subscription_handler_queue_capacity(Some(2))
        .expect("bounded queue should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("cancelled-queued-delayed-delivery");
    let started = Arc::new((Mutex::new(0_usize), Condvar::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let captured_started = Arc::clone(&started);
    let captured_release = Arc::clone(&release);
    let subscription = bus
        .subscribe("sub", &topic, move |_event| {
            let (started_lock, started_condvar) = &*captured_started;
            let mut started_count =
                started_lock.lock().expect("started count should lock");
            *started_count += 1;
            started_condvar.notify_all();
            drop(started_count);

            let (release_lock, release_condvar) = &*captured_release;
            let mut released =
                release_lock.lock().expect("release gate should lock");
            while !*released {
                released = release_condvar
                    .wait(released)
                    .expect("release gate wait should not poison");
            }
        })
        .expect("subscription should register");

    bus.publish(&topic, "first".to_string())
        .expect("first publish should occupy the worker");
    wait_for_count(&started, 1);
    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "second".to_string())
            .with_delay(Duration::from_millis(500)),
    )
    .expect("delayed publish should queue behind the worker");
    subscription
        .cancel()
        .expect("subscription cancellation should succeed");

    let wait_started_at = Instant::now();
    release_gate(&release);
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert!(
        wait_started_at.elapsed() < Duration::from_millis(150),
        "cancelled delayed delivery should not sleep for the configured delay"
    );
    assert_eq!(*started.0.lock().expect("started count should lock"), 1);
}

#[test]
fn test_cancelled_ordered_delayed_delivery_skips_delay_wait() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("cancelled-ordered-delayed-delivery");
    let (received_tx, received_rx) = mpsc::channel::<String>();
    let subscription = bus
        .subscribe("sub", &topic, move |event| {
            received_tx
                .send(event.payload().clone())
                .expect("received payload should send");
            Ok(())
        })
        .expect("subscription should register");

    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "delayed".to_string())
            .with_ordering_key("same-key")
            .with_delay(Duration::from_millis(500)),
    )
    .expect("ordered delayed publish should queue");
    thread::sleep(Duration::from_millis(30));
    let wait_started_at = Instant::now();
    subscription
        .cancel()
        .expect("subscription cancellation should succeed");

    assert!(
        bus.wait_for_idle_timeout(&topic, Duration::from_millis(150))
            .expect("cancelled delayed ordered delivery should become idle"),
        "cancelled ordered delayed delivery should not wait for the configured delay"
    );
    assert!(
        wait_started_at.elapsed() < Duration::from_millis(150),
        "cancelled ordered delayed delivery should wake promptly"
    );
    assert!(
        received_rx.recv_timeout(Duration::from_millis(50)).is_err(),
        "cancelled ordered delayed delivery should not invoke the handler"
    );
}

#[test]
fn test_shutdown_waits_for_active_handler() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("shutdown-waits-active-handler");
    let started = Arc::new((Mutex::new(0_usize), Condvar::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let captured_started = Arc::clone(&started);
    let captured_release = Arc::clone(&release);
    bus.subscribe("sub", &topic, move |_event| {
        let (started_lock, started_condvar) = &*captured_started;
        let mut started_count =
            started_lock.lock().expect("started count should lock");
        *started_count += 1;
        started_condvar.notify_all();
        drop(started_count);

        let (release_lock, release_condvar) = &*captured_release;
        let mut released =
            release_lock.lock().expect("release gate should lock");
        while !*released {
            released = release_condvar
                .wait(released)
                .expect("release gate wait should not poison");
        }
    })
    .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should start handler work");
    wait_for_count(&started, 1);
    let release_for_thread = Arc::clone(&release);
    let releaser = thread::spawn(move || {
        thread::sleep(Duration::from_millis(20));
        release_gate(&release_for_thread);
    });

    assert!(bus.shutdown());
    releaser.join().expect("release thread should finish");
    assert_eq!(*started.0.lock().expect("started count should lock"), 1);
}

#[test]
fn test_shutdown_panics_when_called_from_own_subscription_worker() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("shutdown-from-own-handler");
    let handler_bus = bus.clone();
    let (result_tx, result_rx) = mpsc::channel();
    bus.subscribe("sub", &topic, move |_event| {
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                handler_bus.shutdown()
            }));
        result_tx
            .send(result.is_err())
            .expect("shutdown result should send");
        Ok(())
    })
    .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should start handler work");

    assert!(
        result_rx
            .recv_timeout(Duration::from_millis(200))
            .expect("handler should report shutdown panic")
    );
    bus.wait_for_idle(&topic).expect("topic should become idle");
    assert!(bus.shutdown());
}

#[test]
fn test_shutdown_nonblocking_returns_while_handler_is_active() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("shutdown-nonblocking-active-handler");
    let started = Arc::new((Mutex::new(0_usize), Condvar::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let captured_started = Arc::clone(&started);
    let captured_release = Arc::clone(&release);
    bus.subscribe("sub", &topic, move |_event| {
        let (started_lock, started_condvar) = &*captured_started;
        let mut started_count =
            started_lock.lock().expect("started count should lock");
        *started_count += 1;
        started_condvar.notify_all();
        drop(started_count);

        let (release_lock, release_condvar) = &*captured_release;
        let mut released =
            release_lock.lock().expect("release gate should lock");
        while !*released {
            released = release_condvar
                .wait(released)
                .expect("release gate wait should not poison");
        }
    })
    .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should start handler work");
    wait_for_count(&started, 1);

    assert!(bus.shutdown_nonblocking());
    assert!(!bus.shutdown_nonblocking());
    assert_eq!(
        bus.publish(&topic, "after-shutdown".to_string())
            .expect_err("nonblocking shutdown should stop publishing"),
        EventBusError::not_started()
    );

    release_gate(&release);
    bus.wait_for_idle(&topic).expect("topic should become idle");
    assert_eq!(*started.0.lock().expect("started count should lock"), 1);
}

#[test]
fn test_publish_racing_shutdown_rejects_existing_ordering_lane_submission() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    let interceptor_ready = Arc::new((Mutex::new(false), Condvar::new()));
    let release_interceptor = Arc::new((Mutex::new(false), Condvar::new()));
    let captured_ready = Arc::clone(&interceptor_ready);
    let captured_release_interceptor = Arc::clone(&release_interceptor);
    factory
        .add_publisher_interceptor::<String, _>(
            move |event: EventEnvelope<String>| {
                if event.payload() == "after-shutdown" {
                    release_gate(&captured_ready);
                    let (release_lock, release_condvar) =
                        &*captured_release_interceptor;
                    let mut released = release_lock
                        .lock()
                        .expect("interceptor gate should lock");
                    while !*released {
                        released = release_condvar
                            .wait(released)
                            .expect("interceptor wait should not poison");
                    }
                }
                Some(event)
            },
        )
        .expect("interceptor should register");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("ordered-shutdown-race");
    let started = Arc::new((Mutex::new(0_usize), Condvar::new()));
    let release_handler = Arc::new((Mutex::new(false), Condvar::new()));
    let captured_started = Arc::clone(&started);
    let captured_release_handler = Arc::clone(&release_handler);
    bus.subscribe("sub", &topic, move |_event| {
        let (started_lock, started_condvar) = &*captured_started;
        let mut started_count =
            started_lock.lock().expect("started count should lock");
        *started_count += 1;
        started_condvar.notify_all();
        drop(started_count);

        let (release_lock, release_condvar) = &*captured_release_handler;
        let mut released =
            release_lock.lock().expect("release gate should lock");
        while !*released {
            released = release_condvar
                .wait(released)
                .expect("release gate wait should not poison");
        }
    })
    .expect("subscription should register");

    bus.publish_envelope(
        EventEnvelope::create(topic.clone(), "first".to_string())
            .with_ordering_key("account-1"),
    )
    .expect("first publish should occupy the ordered lane");
    wait_for_count(&started, 1);

    let publisher_bus = bus.clone();
    let publish_topic = topic.clone();
    let publisher = thread::spawn(move || {
        publisher_bus.publish_envelope(
            EventEnvelope::create(publish_topic, "after-shutdown".to_string())
                .with_ordering_key("account-1"),
        )
    });
    wait_for_gate(&interceptor_ready);
    assert!(bus.shutdown_nonblocking());
    release_gate(&release_interceptor);
    let error = publisher
        .join()
        .expect("publisher thread should finish")
        .expect_err("publish should be rejected after shutdown wins admission");
    release_gate(&release_handler);
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(error, EventBusError::not_started());
}

#[test]
fn test_shutdown_with_timeout_reports_active_handler_timeout() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("shutdown-timeout-active-handler");
    let started = Arc::new((Mutex::new(0_usize), Condvar::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let captured_started = Arc::clone(&started);
    let captured_release = Arc::clone(&release);
    bus.subscribe("sub", &topic, move |_event| {
        let (started_lock, started_condvar) = &*captured_started;
        let mut started_count =
            started_lock.lock().expect("started count should lock");
        *started_count += 1;
        started_condvar.notify_all();
        drop(started_count);

        let (release_lock, release_condvar) = &*captured_release;
        let mut released =
            release_lock.lock().expect("release gate should lock");
        while !*released {
            released = release_condvar
                .wait(released)
                .expect("release gate wait should not poison");
        }
    })
    .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should start handler work");
    wait_for_count(&started, 1);

    let error = bus
        .shutdown_with_timeout(Duration::from_millis(10))
        .expect_err("active handler should time out shutdown");
    assert!(matches!(error, EventBusError::ShutdownTimedOut { .. }));
    assert_eq!(error.kind(), "shutdown_timed_out");

    release_gate(&release);
    bus.wait_for_idle(&topic).expect("topic should become idle");
    assert!(!bus.shutdown());
}

#[test]
fn test_shutdown_with_timeout_completes_without_active_work() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("shutdown-timeout-completes");

    assert!(
        bus.shutdown_with_timeout(Duration::from_secs(1))
            .expect("idle shutdown should complete")
    );
    assert_eq!(
        bus.publish(&topic, "after-shutdown".to_string())
            .expect_err("stopped bus should reject publish"),
        EventBusError::not_started()
    );
}

#[test]
fn test_shutdown_with_timeout_returns_false_when_already_stopped() {
    let bus = LocalEventBus::new();

    assert!(
        !bus.shutdown_with_timeout(Duration::from_millis(10))
            .expect("stopped shutdown should be idempotent")
    );
}

#[test]
fn test_start_rejects_restart_after_shutdown_timeout_until_handlers_finish() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("restart-after-shutdown-timeout");
    let started = Arc::new((Mutex::new(0_usize), Condvar::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let captured_started = Arc::clone(&started);
    let captured_release = Arc::clone(&release);
    bus.subscribe("sub", &topic, move |_event| {
        let (started_lock, started_condvar) = &*captured_started;
        let mut started_count =
            started_lock.lock().expect("started count should lock");
        *started_count += 1;
        started_condvar.notify_all();
        drop(started_count);

        let (release_lock, release_condvar) = &*captured_release;
        let mut released =
            release_lock.lock().expect("release gate should lock");
        while !*released {
            released = release_condvar
                .wait(released)
                .expect("release gate wait should not poison");
        }
    })
    .expect("subscription should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should start handler work");
    wait_for_count(&started, 1);
    let error = bus
        .shutdown_with_timeout(Duration::from_millis(10))
        .expect_err("active handler should time out shutdown");
    assert!(matches!(error, EventBusError::ShutdownTimedOut { .. }));

    let restart_error = bus
        .start()
        .expect_err("restart should wait until old handler work is finished");
    assert!(matches!(restart_error, EventBusError::StartFailed { .. }));

    release_gate(&release);
    bus.wait_for_idle(&topic).expect("topic should become idle");
    assert!(
        bus.start()
            .expect("restart should work after old handlers finish")
    );
    assert!(bus.shutdown());
}

#[test]
fn test_shutdown_routes_dead_letter_for_handler_failure_during_graceful_shutdown()
 {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("shutdown-routes-dead-letter");
    let probe_topic = create_topic("shutdown-routes-dead-letter-probe");
    let dead_letter_topic =
        create_dead_letter_topic("dlq.shutdown-routes-dead-letter");
    let dead_letters =
        Arc::new(Mutex::new(Vec::<EventEnvelope<DeadLetterPayload>>::new()));
    let captured_dead_letters = Arc::clone(&dead_letters);
    bus.subscribe("dlq-sub", &dead_letter_topic, move |event| {
        captured_dead_letters
            .lock()
            .expect("dead letters should lock")
            .push(event);
        Ok(())
    })
    .expect("dead letter subscriber should register");

    let started = Arc::new((Mutex::new(0_usize), Condvar::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let captured_started = Arc::clone(&started);
    let captured_release = Arc::clone(&release);
    let dead_letter_target = dead_letter_topic.clone();
    let options = SubscribeOptions::<String>::builder()
        .dead_letter_strategy(move |subscriber_id, failed, error, _options| {
            Ok(Some(EventEnvelope::create(
                dead_letter_target.clone(),
                DeadLetterRecord::from_failure(subscriber_id, failed, error),
            )))
        })
        .build();
    bus.subscribe_with_options(
        "sub",
        &topic,
        move |_event| {
            let (started_lock, started_condvar) = &*captured_started;
            let mut started_count =
                started_lock.lock().expect("started count should lock");
            *started_count += 1;
            started_condvar.notify_all();
            drop(started_count);

            let (release_lock, release_condvar) = &*captured_release;
            let mut released =
                release_lock.lock().expect("release gate should lock");
            while !*released {
                released = release_condvar
                    .wait(released)
                    .expect("release gate wait should not poison");
            }
            Err(EventBusError::handler_failed(
                "handler failed during shutdown",
            ))
        },
        options,
    )
    .expect("failing subscriber should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should start handler work");
    wait_for_count(&started, 1);

    let shutdown_bus = bus.clone();
    let shutdown_thread = thread::spawn(move || shutdown_bus.shutdown());
    let stopped_before_release = (0..50).any(|_| {
        if bus.publish(&probe_topic, "probe".to_string()).is_err() {
            true
        } else {
            thread::sleep(Duration::from_millis(5));
            false
        }
    });
    assert!(
        stopped_before_release,
        "shutdown should stop public publishing before the handler completes"
    );

    release_gate(&release);
    assert!(
        shutdown_thread
            .join()
            .expect("shutdown thread should finish")
    );

    let events = dead_letters.lock().expect("dead letters should lock");
    assert_eq!(events.len(), 1);
    assert!(events[0].is_dead_letter());
    assert_eq!(
        events[0]
            .payload()
            .downcast_original_payload_ref::<String>()
            .expect("dead letter payload should preserve original payload"),
        "payload"
    );
}

#[test]
fn test_shutdown_routes_delayed_dead_letter_during_graceful_shutdown() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_subscription_handler_pool_size(1)
        .expect("single worker should be accepted");
    let bus = factory.create_started().expect("factory should start bus");
    let topic = create_topic("shutdown-routes-delayed-dead-letter");
    let probe_topic = create_topic("shutdown-routes-delayed-dead-letter-probe");
    let dead_letter_topic =
        create_dead_letter_topic("dlq.shutdown-routes-delayed-dead-letter");
    let (dead_letter_tx, dead_letter_rx) =
        mpsc::channel::<EventEnvelope<DeadLetterPayload>>();
    bus.subscribe("dlq-sub", &dead_letter_topic, move |event| {
        dead_letter_tx.send(event).expect("dead letter should send");
        Ok(())
    })
    .expect("dead letter subscriber should register");

    let started = Arc::new((Mutex::new(0_usize), Condvar::new()));
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let captured_started = Arc::clone(&started);
    let captured_release = Arc::clone(&release);
    let dead_letter_target = dead_letter_topic.clone();
    let options = SubscribeOptions::<String>::builder()
        .dead_letter_strategy(move |subscriber_id, failed, error, _options| {
            Ok(Some(
                EventEnvelope::create(
                    dead_letter_target.clone(),
                    DeadLetterRecord::from_failure(
                        subscriber_id,
                        failed,
                        error,
                    ),
                )
                .with_delay(Duration::from_millis(30)),
            ))
        })
        .build();
    bus.subscribe_with_options(
        "sub",
        &topic,
        move |_event| {
            let (started_lock, started_condvar) = &*captured_started;
            let mut started_count =
                started_lock.lock().expect("started count should lock");
            *started_count += 1;
            started_condvar.notify_all();
            drop(started_count);

            wait_for_gate(&captured_release);
            Err(EventBusError::handler_failed(
                "handler failed during shutdown",
            ))
        },
        options,
    )
    .expect("failing subscriber should register");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should start handler work");
    wait_for_count(&started, 1);

    let shutdown_bus = bus.clone();
    let shutdown_thread = thread::spawn(move || shutdown_bus.shutdown());
    let stopped_before_release = (0..50).any(|_| {
        if bus.publish(&probe_topic, "probe".to_string()).is_err() {
            true
        } else {
            thread::sleep(Duration::from_millis(5));
            false
        }
    });
    assert!(
        stopped_before_release,
        "shutdown should stop public publishing before the handler completes"
    );

    release_gate(&release);
    assert!(
        shutdown_thread
            .join()
            .expect("shutdown thread should finish")
    );

    let dead_letter =
        dead_letter_rx.recv_timeout(Duration::from_secs(1)).expect(
            "delayed dead letter should be delivered while shutdown drains",
        );
    assert!(dead_letter.is_dead_letter());
    assert_eq!(
        dead_letter
            .payload()
            .downcast_original_payload_ref::<String>()
            .expect("dead letter payload should preserve original payload"),
        "payload"
    );
}

#[test]
fn test_dead_letter_publish_failure_is_observed() {
    let bus = LocalEventBus::started().expect("bus should start");
    let topic = create_topic("dead-letter-publish-failure");
    let dead_letter_topic =
        create_dead_letter_topic("dlq.dead-letter-publish-failure");
    let observed = Arc::new(Mutex::new(Vec::<EventBusError>::new()));
    let captured_observed = Arc::clone(&observed);
    bus.add_error_observer(move |error| {
        captured_observed
            .lock()
            .expect("observed errors should lock")
            .push(error.clone());
    })
    .expect("error observer should register");
    let dead_letter_target = dead_letter_topic.clone();
    let options = SubscribeOptions::<String>::builder()
        .dead_letter_strategy(move |subscriber_id, failed, error, _options| {
            Ok(Some(EventEnvelope::create(
                dead_letter_target.clone(),
                DeadLetterRecord::from_failure(subscriber_id, failed, error),
            )))
        })
        .build();
    let dead_letter_options = SubscribeOptions::<DeadLetterPayload>::builder()
        .filter(|_event| -> bool {
            panic!("dead-letter filter panic");
        })
        .build();
    bus.subscribe_with_options(
        "dlq-sub",
        &dead_letter_topic,
        |_event| Ok(()),
        dead_letter_options,
    )
    .expect("dead letter subscriber should register");
    bus.subscribe_with_options(
        "sub",
        &topic,
        |_event| Err(EventBusError::handler_failed("handler failed")),
        options,
    )
    .expect("failing subscriber should register");
    let result = {
        let _panic_hook_guard = PanicHookGuard::suppress();
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            bus.publish(&topic, "payload".to_string())
                .expect("publish should schedule failing handler");
            bus.wait_for_idle(&topic).expect("topic should become idle");
        }))
    };
    result.expect("dead-letter filter panic should be isolated");

    let observed = observed.lock().expect("observed errors should lock");
    assert!(observed.iter().any(|error| matches!(
        error,
        EventBusError::DeadLetterFailed { message } if message.contains("filter panicked")
    )));
}
