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

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::time::Duration;

use qubit_event_bus::{
    AckMode, DeadLetterPayload, EventBusError, EventEnvelope, LocalEventBus, LocalEventBusFactory,
    RetryOptions, SubscribeOptions, Topic,
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

fn create_dead_letter_topic(name: &str) -> Topic<DeadLetterPayload> {
    Topic::try_new(name).expect("dead letter topic should build")
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
    let dead_letter_topic = create_dead_letter_topic("dlq.retry-failed");
    let attempts = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::<EventEnvelope<DeadLetterPayload>>::new()));

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
                EventEnvelope::create(
                    dead_letter_target.clone(),
                    Arc::new(failed.payload().clone()) as DeadLetterPayload,
                )
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
    let payload = events[0]
        .payload()
        .downcast_ref::<String>()
        .expect("dead letter payload should preserve original payload");
    assert_eq!(payload, "payload");
}

#[test]
fn test_manual_ack_is_exposed_to_handler() {
    let bus = LocalEventBus::started();
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
    let bus = LocalEventBus::started();
    let topic = create_topic("manual-nack");
    let dead_letter_topic = create_dead_letter_topic("dlq.manual-nack");
    let errors = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::<EventEnvelope<DeadLetterPayload>>::new()));
    let captured_errors = Arc::clone(&errors);
    let dead_letter_target = dead_letter_topic.clone();
    let options = SubscribeOptions::<String>::builder()
        .ack_mode(AckMode::Manual)
        .error_handler(move |subscriber_id, envelope, error, acknowledgement| {
            assert_eq!(subscriber_id, "sub-1");
            assert_eq!(envelope.payload(), "payload");
            assert!(error.to_string().contains("nack"));
            assert!(acknowledgement.is_nacked());
            captured_errors.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .dead_letter_strategy(move |_subscriber_id, failed, _error, _options| {
            Some(EventEnvelope::create(
                dead_letter_target.clone(),
                Arc::new(failed.payload().clone()) as DeadLetterPayload,
            ))
        })
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
    let bus = LocalEventBus::started();
    let topic = create_topic("error-handler-ack");
    let dead_letter_topic = create_dead_letter_topic("dlq.error-handler-ack");
    let first_errors = Arc::new(AtomicUsize::new(0));
    let second_errors = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::<EventEnvelope<DeadLetterPayload>>::new()));
    let captured_first = Arc::clone(&first_errors);
    let captured_second = Arc::clone(&second_errors);
    let dead_letter_target = dead_letter_topic.clone();
    let options = SubscribeOptions::<String>::builder()
        .error_handler(move |_subscriber_id, _envelope, _error, acknowledgement| {
            acknowledgement.ack();
            captured_first.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .error_handler(move |_subscriber_id, _envelope, _error, _acknowledgement| {
            captured_second.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .dead_letter_strategy(move |_subscriber_id, failed, _error, _options| {
            Some(EventEnvelope::create(
                dead_letter_target.clone(),
                Arc::new(failed.payload().clone()) as DeadLetterPayload,
            ))
        })
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
fn test_handler_panic_is_reported_and_does_not_block_idle_wait() {
    let bus = LocalEventBus::started();
    let topic = create_topic("panic-handler");
    let errors = Arc::new(AtomicUsize::new(0));
    let captured_errors = Arc::clone(&errors);
    let options = SubscribeOptions::builder()
        .error_handler(move |_subscriber_id, _envelope, error, acknowledgement| {
            assert!(error.to_string().contains("panicked"));
            assert!(!acknowledgement.is_completed());
            captured_errors.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .build();
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    bus.subscribe_with_options(
        "sub-1",
        &topic,
        |_| -> qubit_event_bus::EventBusResult<()> {
            panic!("handler panic");
        },
        options,
    )
    .expect("subscribe should work");

    bus.publish(&topic, "payload".to_string())
        .expect("publish should work");
    bus.wait_for_idle(&topic)
        .expect("topic should become idle after panic");
    std::panic::set_hook(previous_hook);

    assert_eq!(errors.load(Ordering::SeqCst), 1);
}

#[test]
fn test_subscriber_interceptor_wraps_handler_and_can_short_circuit() {
    let bus = LocalEventBus::started();
    let handled_topic = create_topic("subscriber-interceptor");
    let dropped_topic = create_topic("subscriber-interceptor-dropped");
    let sequence = Arc::new(Mutex::new(Vec::<String>::new()));
    let captured_before_after = Arc::clone(&sequence);
    bus.add_subscriber_interceptor::<String, _, _>(move |event, chain| {
        captured_before_after
            .lock()
            .expect("sequence should lock")
            .push(format!("before:{}", event.payload()));
        let result = chain.proceed(event.with_header("intercepted", "true"));
        captured_before_after
            .lock()
            .expect("sequence should lock")
            .push("after".to_string());
        result
    })
    .expect("subscriber interceptor should register");
    let captured_short_circuit = Arc::clone(&sequence);
    bus.add_subscriber_interceptor::<String, _, _>(move |event, chain| {
        if event.topic().name() == "subscriber-interceptor-dropped" {
            captured_short_circuit
                .lock()
                .expect("sequence should lock")
                .push("dropped".to_string());
            Ok(())
        } else {
            chain.proceed(event)
        }
    })
    .expect("subscriber interceptor should register");
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
    let bus = factory.create_started();
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
