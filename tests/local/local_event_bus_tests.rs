/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for the in-process event bus implementation.

use std::sync::atomic::{
    AtomicUsize,
    Ordering,
};
use std::sync::{
    Arc,
    Mutex,
};
use std::time::Duration;

use qubit_event_bus::{
    AckMode,
    EventBusError,
    EventEnvelope,
    LocalEventBus,
    LocalEventBusFactory,
    RetryOptions,
    SubscribeOptions,
    Topic,
};

fn create_topic(name: &str) -> Topic<String> {
    Topic::try_new(name).expect("topic should build")
}

fn received_payloads(events: &Arc<Mutex<Vec<EventEnvelope<String>>>>) -> Vec<String> {
    let mut payloads = events
        .lock()
        .expect("received events should lock")
        .iter()
        .map(|event| event.payload().clone())
        .collect::<Vec<_>>();
    payloads.sort();
    payloads
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
    assert!(bus.start());
    assert!(!bus.start());
    let subscription = bus
        .subscribe("sub-1", &topic, |_| Ok(()))
        .expect("subscribe should work after start");
    assert!(subscription.is_active());
    assert_eq!(subscription.subscriber_id(), "sub-1");
    assert_eq!(subscription.topic(), &topic);

    assert!(bus.shutdown());
    assert!(!bus.shutdown());
    assert_eq!(
        bus.publish(&topic, "payload".to_string())
            .expect_err("bus should reject publish"),
        EventBusError::not_started()
    );
}

#[test]
fn test_publish_delivers_event_to_single_subscriber() {
    let bus = LocalEventBus::started();
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
fn test_publish_broadcasts_to_multiple_subscribers() {
    let bus = LocalEventBus::started();
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
    let bus = LocalEventBus::started();
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
    let bus = LocalEventBus::started();
    let topic = create_topic("blank-subscriber");

    let error = match bus.subscribe(" ", &topic, |_| Ok(())) {
        Ok(_) => panic!("blank subscriber ID should be rejected"),
        Err(error) => error,
    };

    assert_eq!(
        error,
        EventBusError::invalid_argument("subscriber_id", "subscriber ID must not be blank")
    );
}

#[test]
fn test_default_and_stopped_async_publish() {
    let bus = LocalEventBus::default();
    let topic = create_topic("default-bus");

    let error = bus
        .publish_async(&topic, "payload".to_string())
        .expect_err("stopped bus should reject async publish");

    assert_eq!(error, EventBusError::not_started());
}

#[test]
fn test_subscribe_options_filter_events() {
    let bus = LocalEventBus::started();
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
fn test_publisher_interceptor_can_modify_or_drop_events() {
    let bus = LocalEventBus::started();
    let topic = create_topic("intercepted");
    let dropped = create_topic("dropped");
    let received = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&received);

    bus.add_publisher_interceptor::<String, _>(|event| {
        if event.topic().name() == "dropped" {
            None
        } else {
            Some(event.with_header("intercepted", "true"))
        }
    })
    .expect("interceptor should be registered");
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
fn test_retry_eventually_succeeds() {
    let bus = LocalEventBus::started();
    let topic = create_topic("retry");
    let attempts = Arc::new(AtomicUsize::new(0));
    let captured_attempts = Arc::clone(&attempts);
    let options = SubscribeOptions::<String>::builder()
        .retry_options(RetryOptions::new(3, Duration::ZERO).expect("retry should build"))
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
fn test_exhausted_retry_calls_error_handler_and_dead_letter_strategy() {
    let bus = LocalEventBus::started();
    let topic = create_topic("retry-failed");
    let dead_letter_topic = create_topic("dlq.retry-failed");
    let attempts = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::new()));

    let captured_attempts = Arc::clone(&attempts);
    let captured_errors = Arc::clone(&errors);
    let dead_letter_target = dead_letter_topic.clone();
    let options = SubscribeOptions::<String>::builder()
        .retry_options(RetryOptions::new(2, Duration::ZERO).expect("retry should build"))
        .error_handler(move |subscriber_id, envelope, error, acknowledgement| {
            assert_eq!(subscriber_id, "sub-1");
            assert_eq!(envelope.payload(), "payload");
            assert!(error.to_string().contains("failed"));
            assert!(!acknowledgement.is_completed());
            captured_errors.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .dead_letter_strategy(move |subscriber_id, failed, error, _options| {
            Some(
                EventEnvelope::create(dead_letter_target.clone(), failed.payload().clone())
                    .with_header("subscriber-id", subscriber_id)
                    .with_header("failure", error.to_string())
                    .as_dead_letter(),
            )
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
}

#[test]
fn test_manual_ack_is_exposed_to_handler() {
    let bus = LocalEventBus::started();
    let topic = create_topic("manual-ack");
    let acknowledgements = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&acknowledgements);
    let options = SubscribeOptions::builder()
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
fn test_publish_all_and_publish_async() {
    let bus = LocalEventBus::started();
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
        .map(|payload| EventEnvelope::create(topic.clone(), payload.to_string()))
        .collect::<Vec<_>>();
    bus.publish_all(envelopes)
        .expect("batch publish should work");
    let handle = bus
        .publish_async(&topic, "async".to_string())
        .expect("async publish should start");
    handle
        .join()
        .expect("publish thread should join")
        .expect("async publish should work");
    bus.wait_for_idle(&topic).expect("topic should become idle");

    assert_eq!(
        received_payloads(&received),
        vec![
            "async".to_string(),
            "batch-1".to_string(),
            "batch-2".to_string()
        ]
    );
}

#[test]
fn test_factory_creates_started_bus_with_default_options() {
    let mut factory = LocalEventBusFactory::new();
    factory
        .set_default_subscribe_options(SubscribeOptions::<String>::builder().priority(5).build());

    let bus = factory.create_started();
    let topic = create_topic("factory");
    let subscription = bus
        .subscribe("sub-1", &topic, |_| Ok(()))
        .expect("factory bus should accept subscriptions");

    assert_eq!(subscription.options().priority(), 5);
}
