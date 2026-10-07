// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public asynchronous event-bus publishing contracts.

use std::error::Error;
use std::sync::Arc;
use std::sync::Mutex;

use qubit_event_bus::error::PublishAttemptError;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::facade::AsyncEventBus;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishMetadata;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_retry::AttemptFailure;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryErrorReason;
use qubit_retry::RetryPolicy;

use crate::support::fake_spi::FakeAsyncEventBusSpi;
use crate::support::manual_async::block_on;

/// Creates the common typed topic used by publishing contract tests.
fn topic() -> Topic<u32> {
    Topic::new("test.topic").unwrap()
}

#[test]
fn test_async_facade_publishes_single_and_ordered_batch_without_runtime_dependency() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");

    let unpolled = bus.publish(PublishRequest::new(topic(), 99).unwrap());
    assert_eq!(bus.publish_metrics().attempts, 0);
    drop(unpolled);

    block_on(async {
        let request = PublishRequest::new(topic(), 7).unwrap();
        let input_event_id = request.envelope().id().clone();
        let receipt = bus.publish(request).await.unwrap();
        assert_eq!(receipt.input_event_id(), &input_event_id);

        let batch = bus
            .publish_all([
                PublishRequest::new(topic(), 11).unwrap(),
                PublishRequest::new(topic(), 12).unwrap(),
            ])
            .await;
        assert_eq!(batch.total_count(), 2);
        assert_eq!(batch.accepted_count(), 2);

        let string_topic = Topic::<String>::new("test.string.topic").unwrap();
        let string_batch = bus
            .publish_all([
                PublishRequest::new(string_topic.clone(), String::from("first")).unwrap(),
                PublishRequest::new(string_topic.clone(), String::from("second")).unwrap(),
            ])
            .await;
        assert_eq!(string_batch.total_count(), 2);
        assert_eq!(string_batch.accepted_count(), 2);

        spi.fail_next_publish();
        let failed_options = PublishOptions::<String>::builder()
            .retry_policy(RetryPolicy::builder().max_attempts(1).build().unwrap())
            .build();
        let mixed_batch = bus
            .publish_all([
                PublishRequest::new(string_topic.clone(), String::from("rejected"))
                    .unwrap()
                    .with_options(failed_options),
                PublishRequest::new(string_topic, String::from("accepted")).unwrap(),
            ])
            .await;
        assert_eq!(mixed_batch.total_count(), 2);
        assert_eq!(mixed_batch.accepted_count(), 1);
        assert_eq!(mixed_batch.failure_count(), 1);
        assert!(mixed_batch.items()[0].is_err());
        assert!(mixed_batch.items()[1].is_ok());

        let outcome = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
        assert_eq!(outcome.outcome, ShutdownOutcome::Complete);
    });

    let metrics = bus.publish_metrics();
    assert_eq!(metrics.attempts, 7);
    assert_eq!(metrics.errors, 1);
    assert_eq!(metrics.opaque_accepted, 6);
    assert_eq!(metrics.dropped, 0);
    assert_eq!(spi.shutdown_transition_count(), 1);
    assert_eq!(spi.operation_log().iter().filter(|op| **op == "publish").count(), 7);
}

#[test]
fn test_async_facade_publisher_interceptor_can_drop_without_spi_publish() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let typed_order = order.clone();
    let global_order = order.clone();
    let config = EventBusFacadeConfig::new().publisher_interceptor(move |metadata: &mut PublishMetadata| {
        global_order.lock().unwrap().push("global");
        assert_eq!(metadata.header("origin"), Some("typed"));
        metadata.set_header("trace", "global")?;
        Ok(false)
    });
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    let bus = AsyncEventBus::with_config(ProviderId::new("fake").unwrap(), spi.clone(), config)
        .expect("valid provider capabilities");
    let options = PublishOptions::<u32>::builder()
        .interceptor(move |mut envelope| {
            typed_order.lock().unwrap().push("typed");
            envelope.set_header("origin", "typed").expect("valid header");
            Ok(Some(envelope))
        })
        .build();

    block_on(async {
        let receipt = bus
            .publish(PublishRequest::new(topic(), 17).unwrap().with_options(options))
            .await
            .unwrap();
        assert!(receipt.acknowledgement().is_dropped());
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });

    assert_eq!(*order.lock().unwrap(), ["typed", "global"]);
    assert_eq!(bus.publish_metrics().attempts, 1);
    assert_eq!(bus.publish_metrics().dropped, 1);
    assert!(!spi.operation_log().contains(&"publish"));
}

#[test]
fn test_async_terminal_publish_retry_error_retains_reason_attempt_and_spi_source() {
    let spi = Arc::new(FakeAsyncEventBusSpi::new());
    spi.fail_next_publish();
    let bus =
        AsyncEventBus::from_spi(ProviderId::new("fake").unwrap(), spi.clone()).expect("valid provider capabilities");
    let options = PublishOptions::<u32>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(1).build().unwrap())
        .retry_rule(|_: &AttemptFailure<PublishAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .build();

    block_on(async {
        let error = bus
            .publish(PublishRequest::new(topic(), 91).unwrap().with_options(options))
            .await
            .unwrap_err();
        let PublishError::Retry(retry) = error.into_cause() else {
            panic!("typed RetryError must reach async facade caller");
        };
        assert!(matches!(retry.reason(), RetryErrorReason::Exhausted { .. }));
        assert_eq!(retry.context().attempts(), 1);
        let attempt_error = retry.last_error().expect("attempt error retained");
        assert_eq!(attempt_error.kind(), "fake_failure");
        let mut source = Error::source(attempt_error);
        let mut found = false;
        while let Some(error) = source {
            if error.to_string().contains("fake") {
                found = true;
                break;
            }
            source = error.source();
        }
        assert!(found, "provider source chain must survive terminal retry mapping");
        let _ = bus.shutdown(ShutdownMode::Immediate).await.unwrap();
    });
}
