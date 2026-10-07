// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Subscriber-pipeline unit contracts that require crate-private access.

use std::error::Error;
use std::future::Future;
use std::io::Error as IoError;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;

use qubit_id::Id;

use crate::error::DeliveryAttemptError;
use crate::error::DeliveryError;
use crate::model::AckMode;
use crate::model::AcknowledgementState;
use crate::model::AsyncSubscriberInterceptor;
use crate::model::Delivery;
use crate::model::DeliveryContext;
use crate::model::EventEnvelope;
use crate::model::FailureDirective;
use crate::model::ProviderId;
use crate::model::SubscriberId;
use crate::model::SubscriberInterceptor;
use crate::model::Topic;
use crate::pipeline::DeliveryFailureAction;
use crate::pipeline::DeliveryOutcome;
use crate::pipeline::SubscriberPipeline;
use crate::pipeline::dead_letter_envelope;
use crate::spi::DeliveryDisposition;
use crate::spi::SettlementCapabilities;
use crate::spi::SpiFuture;

/// Builds a delivery with stable test identities and an empty string payload.
fn delivery() -> Delivery<String> {
    let topic = Topic::<String>::new("test.events").unwrap();
    let event = Arc::new(EventEnvelope::new(topic, String::new()).unwrap());
    let context = DeliveryContext::new(
        ProviderId::new("test.provider").unwrap(),
        Id::new(1),
        SubscriberId::new("test.subscriber").unwrap(),
    );
    Delivery::new(event, context)
}

#[test]
fn test_manual_ack_matrix_and_handler_error_precedence_are_enforced() {
    let acked = delivery();
    acked.acknowledgement().ack().unwrap();
    let outcome = SubscriberPipeline::finish_attempt(AckMode::Manual, &acked, Ok(()));
    assert!(matches!(outcome, DeliveryOutcome::Success));

    let pending = delivery();
    assert!(matches!(
        SubscriberPipeline::finish_attempt(AckMode::Manual, &pending, Ok(())),
        DeliveryOutcome::Failure(_)
    ));

    let acknowledged_then_error = delivery();
    acknowledged_then_error.acknowledgement().ack().unwrap();
    let outcome = SubscriberPipeline::finish_attempt(
        AckMode::Manual,
        &acknowledged_then_error,
        Err(DeliveryError::Handler {
            source: Box::new(IoError::other("handler failed")),
        }),
    );
    assert!(matches!(outcome, DeliveryOutcome::Failure(error) if error.to_string().contains("handler failed")));

    let auto = delivery();
    assert!(matches!(
        SubscriberPipeline::finish_attempt(AckMode::Auto, &auto, Ok(())),
        DeliveryOutcome::Success
    ));
    assert!(matches!(
        SubscriberPipeline::finish_attempt(
            AckMode::Auto,
            &auto,
            Err(DeliveryError::Handler {
                source: Box::new(IoError::other("auto failure"))
            })
        ),
        DeliveryOutcome::Failure(_)
    ));
    let nacked = delivery();
    nacked.acknowledgement().nack().unwrap();
    assert!(matches!(
        SubscriberPipeline::finish_attempt(AckMode::Manual, &nacked, Ok(())),
        DeliveryOutcome::Failure(_)
    ));
    assert!(nacked.acknowledgement().nack().is_ok());
    assert!(nacked.acknowledgement().ack().is_err());
    assert!(SubscriberPipeline::validate_ack_capability(AckMode::Manual, SettlementCapabilities::None).is_err());
    assert!(SubscriberPipeline::validate_ack_capability(AckMode::Manual, SettlementCapabilities::AcceptOnly).is_err());
    assert!(
        SubscriberPipeline::validate_ack_capability(AckMode::Manual, SettlementCapabilities::AcceptRetryReject).is_ok()
    );

    let retry_delivery = delivery();
    retry_delivery.acknowledgement().ack().unwrap();
    let next_attempt = retry_delivery.next_attempt(2);
    assert_eq!(next_attempt.context().retry_attempt(), 2);
    assert_eq!(next_attempt.acknowledgement().state(), AcknowledgementState::Pending);
    assert_eq!(next_attempt.event().id(), retry_delivery.event().id());
}

#[test]
fn test_subscriber_middleware_unwinds_in_reverse_registration_layers() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let global_order = order.clone();
    let typed_order = order.clone();
    let handler_order = order.clone();
    let global: Vec<Arc<SubscriberInterceptor<String>>> = vec![Arc::new(move |delivery, next| {
        global_order.lock().unwrap().push("outer-before");
        let result = next(delivery);
        global_order.lock().unwrap().push("outer-after");
        result
    })];
    let typed: Vec<Arc<SubscriberInterceptor<String>>> = vec![Arc::new(move |delivery, next| {
        typed_order.lock().unwrap().push("inner-before");
        let result = next(delivery);
        typed_order.lock().unwrap().push("inner-after");
        result
    })];
    let outcome = SubscriberPipeline::attempt_sync(AckMode::Auto, delivery(), &global, &typed, move |_| {
        handler_order.lock().unwrap().push("handler");
        Ok(())
    });
    assert!(matches!(outcome, DeliveryOutcome::Success));
    assert_eq!(
        *order.lock().unwrap(),
        ["outer-before", "inner-before", "handler", "inner-after", "outer-after"]
    );
}

#[test]
fn test_async_subscriber_middleware_uses_runtime_neutral_continuations() {
    let order = Arc::new(Mutex::new(Vec::new()));
    let global_order = order.clone();
    let typed_order = order.clone();
    let handler_order = order.clone();
    let global: Vec<Arc<AsyncSubscriberInterceptor<String>>> = vec![Arc::new(move |delivery, next| {
        global_order.lock().unwrap().push("outer-before");
        let order = global_order.clone();
        Box::pin(async move {
            let result = next(delivery).await;
            order.lock().unwrap().push("outer-after");
            result
        }) as SpiFuture<'static, Result<(), DeliveryError>>
    })];
    let typed: Vec<Arc<AsyncSubscriberInterceptor<String>>> = vec![Arc::new(move |delivery, next| {
        typed_order.lock().unwrap().push("inner-before");
        let order = typed_order.clone();
        Box::pin(async move {
            let result = next(delivery).await;
            order.lock().unwrap().push("inner-after");
            result
        }) as SpiFuture<'static, Result<(), DeliveryError>>
    })];
    let outcome = block_on(SubscriberPipeline::attempt_async(
        AckMode::Auto,
        delivery(),
        &global,
        &typed,
        move |_| {
            handler_order.lock().unwrap().push("handler");
            Box::pin(async { Ok(()) })
        },
    ));
    assert!(matches!(outcome, DeliveryOutcome::Success));
    assert_eq!(
        *order.lock().unwrap(),
        ["outer-before", "inner-before", "handler", "inner-after", "outer-after"]
    );
}

#[test]
fn test_failure_directive_maps_to_terminal_spi_disposition() {
    assert_eq!(
        SubscriberPipeline::failure_action(FailureDirective::Retry),
        DeliveryFailureAction::RetryLocally
    );
    assert_eq!(
        SubscriberPipeline::failure_action(FailureDirective::Requeue),
        DeliveryFailureAction::Requeue
    );
    assert_eq!(
        SubscriberPipeline::failure_action(FailureDirective::DeadLetter),
        DeliveryFailureAction::DeadLetter
    );
    assert_eq!(
        SubscriberPipeline::failure_action(FailureDirective::Discard),
        DeliveryFailureAction::Discard
    );
    assert_eq!(
        SubscriberPipeline::failure_disposition(
            DeliveryFailureAction::Requeue,
            SettlementCapabilities::AcceptRetryReject
        ),
        Some(DeliveryDisposition::Retry)
    );
    assert_eq!(
        SubscriberPipeline::failure_disposition(
            DeliveryFailureAction::RetryLocally,
            SettlementCapabilities::AcceptRetryReject
        ),
        None
    );
    assert_eq!(
        SubscriberPipeline::failure_disposition(DeliveryFailureAction::Discard, SettlementCapabilities::None),
        None
    );
}

#[test]
fn test_subscriber_middleware_panic_is_contained_as_delivery_failure() {
    let panic_layer: Arc<SubscriberInterceptor<String>> = Arc::new(|_, _| panic!("middleware panic"));
    let called = Arc::new(Mutex::new(false));
    let called_handler = called.clone();
    let result = SubscriberPipeline::run_sync(delivery(), &[panic_layer], &[], move |_| {
        *called_handler.lock().unwrap() = true;
        Ok(())
    });
    assert!(result.unwrap_err().to_string().contains("panicked"));
    assert!(!*called.lock().unwrap());
}

#[test]
fn test_delivery_attempt_error_preserves_delivery_error_as_source() {
    let original = DeliveryError::Handler {
        source: Box::new(IoError::other("original")),
    };
    let attempt = DeliveryAttemptError::new("delivery", None, original);
    assert_eq!(attempt.kind(), "delivery");
    assert!(Error::source(&attempt).unwrap().to_string().contains("original"));
}

#[test]
fn test_dead_letter_record_preserves_original_and_prevents_recursive_dead_lettering() {
    struct NonClonePayload;
    let topic = Topic::<NonClonePayload>::new("test.events").unwrap();
    let envelope = Arc::new(EventEnvelope::new(topic, NonClonePayload).unwrap());
    let normal_context = DeliveryContext::new(
        ProviderId::new("test.provider").unwrap(),
        Id::new(2),
        SubscriberId::new("test.subscriber").unwrap(),
    );
    let delivery = Delivery::new(envelope.clone(), normal_context);
    let error = DeliveryError::Handler {
        source: Box::new(IoError::other("failed")),
    };
    let dead_letter = dead_letter_envelope(&delivery, &error, "test.dead-letter")
        .unwrap()
        .unwrap();
    assert_eq!(dead_letter.topic().name(), "test.dead-letter");
    assert_eq!(dead_letter.header("x-qubit-event-bus-dead-letter"), Some("v1"));
    let record = dead_letter.payload();
    assert!(Arc::ptr_eq(&record.original_event_arc(), &envelope));
    assert_eq!(record.subscriber_id().as_str(), "test.subscriber");
    assert_eq!(record.reason(), "event handler failed: failed");

    let dead_letter_context = DeliveryContext::new(
        ProviderId::new("test.provider").unwrap(),
        Id::new(2),
        SubscriberId::new("test.subscriber").unwrap(),
    )
    .as_dead_letter();
    let dead_letter_delivery = Delivery::new(envelope, dead_letter_context);
    assert!(
        dead_letter_envelope(&dead_letter_delivery, &error, "test.dead-letter")
            .unwrap()
            .is_none()
    );
}

/// Drives a runtime-neutral test future with a thread-backed waker.
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        thread::park();
    }
}

/// Wakes the parked test thread when its future becomes ready to poll.
struct ThreadWake(thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}
