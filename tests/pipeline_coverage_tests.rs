// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Publicly observable subscriber pipeline branch tests.

mod support;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::DeliveryError;
use qubit_event_bus::EventBus;
use qubit_event_bus::error::DeliveryAttemptError;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::SyncDeliverySchedulerConfig;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::DeadLetterEvent;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::pipeline::Diagnostic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryPolicy;

fn topic<T: 'static>(name: &str) -> Topic<T> {
    Topic::new(name).unwrap()
}

fn handler_error(message: &'static str) -> DeliveryError {
    DeliveryError::Handler {
        source: Box::new(std::io::Error::other(message)),
    }
}

#[test]
fn retry_runs_interceptor_and_handler_again() {
    let backend = Arc::new(support::fake_spi::FakeEventBusSpi::new());
    let bus = EventBus::from_spi(ProviderId::new("pipeline-retry").unwrap(), backend.clone());
    let attempts = Arc::new(AtomicUsize::new(0));
    let intercepted = Arc::new(AtomicUsize::new(0));
    let (completed_tx, completed_rx) = mpsc::channel();

    let interceptor_attempts = intercepted.clone();
    let options = SubscribeOptions::<u32>::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .retry_rule(|_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .error_handler(|_, _| FailureDirective::Retry)
        .interceptor(move |delivery, next| {
            interceptor_attempts.fetch_add(1, Ordering::AcqRel);
            next(delivery)
        })
        .build();
    let request = SubscribeRequest::new("retry-pipeline", topic("pipeline.retry"))
        .expect("valid subscriber ID")
        .with_options(options);
    let handler_attempts = attempts.clone();
    let subscription = bus
        .subscribe(request, move |_: Delivery<u32>| {
            if handler_attempts.fetch_add(1, Ordering::AcqRel) == 0 {
                Err(handler_error("retry once"))
            } else {
                completed_tx.send(()).unwrap();
                Ok(())
            }
        })
        .unwrap();

    bus.publish(PublishRequest::new(topic("pipeline.retry"), 7_u32).unwrap())
        .unwrap();
    completed_rx.recv_timeout(Duration::from_secs(2)).unwrap();

    assert_eq!(attempts.load(Ordering::Acquire), 2);
    assert_eq!(intercepted.load(Ordering::Acquire), 2);
    subscription.cancel().unwrap();
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn exhausted_retry_publishes_typed_dead_letter_and_emits_one_terminal_diagnostic() {
    let bus = EventBus::local(LocalEventBusConfig::new().queue_capacity(8)).unwrap();
    let dead_letter_topic = topic::<DeadLetterEvent<String>>("pipeline.dead");
    let (dead_letter_tx, dead_letter_rx) = mpsc::channel();
    let dead_letter_subscription = bus
        .subscribe(
            SubscribeRequest::new("dead-letter-reader", dead_letter_topic).expect("valid subscriber ID"),
            move |delivery: Delivery<DeadLetterEvent<String>>| {
                dead_letter_tx
                    .send((
                        delivery.payload().original_event().payload().clone(),
                        delivery.payload().subscriber_id().as_str().to_owned(),
                        delivery.payload().reason().to_owned(),
                    ))
                    .unwrap();
            },
        )
        .unwrap();

    let (diagnostic_tx, diagnostic_rx) = mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::DeliveryFailed { attempts, .. } = diagnostic {
            let _ = diagnostic_tx.send(*attempts);
        }
    });

    let options = SubscribeOptions::<String>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(1).build().unwrap())
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(DeadLetterPolicy::topic("pipeline.dead").unwrap())
        .build();
    let source_topic = topic("pipeline.source");
    let source_subscription = bus
        .subscribe(
            SubscribeRequest::new("dead-letter-source", source_topic.clone())
                .expect("valid subscriber ID")
                .with_options(options),
            |_: Delivery<String>| Err(handler_error("terminal failure")),
        )
        .unwrap();

    bus.publish(PublishRequest::new(source_topic.clone(), "original".to_owned()).unwrap())
        .unwrap();

    let (original_payload, subscriber_id, reason) = dead_letter_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let attempts = diagnostic_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(original_payload, "original");
    assert_eq!(subscriber_id, "dead-letter-source");
    assert!(!reason.is_empty());
    assert_eq!(attempts, 1);

    source_subscription.cancel().unwrap();
    dead_letter_subscription.cancel().unwrap();
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn interceptor_error_retries_and_scheduler_backpressure_preserves_pending_deliveries() {
    let facade_config =
        EventBusFacadeConfig::new().with_sync_delivery_scheduler(SyncDeliverySchedulerConfig::new(1, 0).unwrap());
    let backend = Arc::new(support::fake_spi::FakeEventBusSpi::new());
    let bus = EventBus::with_config(ProviderId::new("pipeline-admission").unwrap(), backend, facade_config);
    let interceptor_calls = Arc::new(AtomicUsize::new(0));
    let handler_calls = Arc::new(AtomicUsize::new(0));

    let interceptor_count = interceptor_calls.clone();
    let options = SubscribeOptions::<u32>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(2).build().unwrap())
        .retry_rule(|_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .error_handler(|_, _| FailureDirective::Retry)
        .interceptor(move |delivery, next| {
            if interceptor_count.fetch_add(1, Ordering::AcqRel) == 0 {
                Err(handler_error("interceptor transient failure"))
            } else {
                next(delivery)
            }
        })
        .build();

    let (started_tx, started_rx) = mpsc::channel();
    let (completed_tx, completed_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let handler_count = handler_calls.clone();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("admission-and-interceptor", topic("pipeline.admission"))
                .expect("valid subscriber ID")
                .with_options(options),
            move |_: Delivery<u32>| {
                if handler_count.fetch_add(1, Ordering::AcqRel) == 0 {
                    started_tx.send(()).unwrap();
                    release_rx.lock().unwrap().recv().unwrap();
                }
                completed_tx.send(()).unwrap();
                Ok(())
            },
        )
        .unwrap();

    let event_topic = topic("pipeline.admission");
    bus.publish(PublishRequest::new(event_topic.clone(), 1_u32).unwrap())
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    bus.publish(PublishRequest::new(event_topic.clone(), 2_u32).unwrap())
        .unwrap();
    bus.publish(PublishRequest::new(event_topic.clone(), 3_u32).unwrap())
        .unwrap();
    release_tx.send(()).unwrap();
    for _ in 0..3 {
        completed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }

    assert_eq!(interceptor_calls.load(Ordering::Acquire), 4);
    assert_eq!(handler_calls.load(Ordering::Acquire), 3);

    subscription.cancel().unwrap();
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}
