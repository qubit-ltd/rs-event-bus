// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use qubit_id::Id;

use crate::error::DeliveryError;
use crate::model::AckMode;
use crate::model::AsyncSubscriberInterceptor;
use crate::model::Delivery;
use crate::model::DeliveryContext;
use crate::model::EventEnvelope;
use crate::model::FailureDirective;
use crate::model::ProviderId;
use crate::model::SubscriberId;
use crate::model::SubscriberInterceptor;
use crate::model::Topic;
use crate::pipeline::AdmissionTracker;
use crate::pipeline::AsyncOrderingLanes;
use crate::pipeline::DeliveryOutcome;
use crate::pipeline::OrderingLaneKey;
use crate::pipeline::OrderingLanes;
use crate::pipeline::SubscriberPipeline;
use crate::pipeline::dead_letter::dead_letter_envelope;
use crate::spi::DeliveryDisposition;

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
fn manual_ack_matrix_and_handler_error_precedence_are_enforced() {
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
            source: Box::new(std::io::Error::other("handler failed")),
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
                source: Box::new(std::io::Error::other("auto failure"))
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
    assert!(
        SubscriberPipeline::validate_ack_capability(AckMode::Manual, crate::spi::SettlementCapabilities::None).is_err()
    );
    assert!(
        SubscriberPipeline::validate_ack_capability(AckMode::Manual, crate::spi::SettlementCapabilities::AcceptOnly)
            .is_err()
    );
    assert!(
        SubscriberPipeline::validate_ack_capability(
            AckMode::Manual,
            crate::spi::SettlementCapabilities::AcceptRetryReject
        )
        .is_ok()
    );

    let retry_delivery = delivery();
    retry_delivery.acknowledgement().ack().unwrap();
    let next_attempt = retry_delivery.next_attempt(2);
    assert_eq!(next_attempt.context().retry_attempt(), 2);
    assert_eq!(
        next_attempt.acknowledgement().state(),
        crate::model::AcknowledgementState::Pending
    );
    assert_eq!(next_attempt.event().id(), retry_delivery.event().id());
}

#[test]
fn subscriber_middleware_unwinds_in_reverse_registration_layers() {
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
fn async_subscriber_middleware_uses_runtime_neutral_continuations() {
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
        }) as crate::spi::SpiFuture<'static, Result<(), DeliveryError>>
    })];
    let typed: Vec<Arc<AsyncSubscriberInterceptor<String>>> = vec![Arc::new(move |delivery, next| {
        typed_order.lock().unwrap().push("inner-before");
        let order = typed_order.clone();
        Box::pin(async move {
            let result = next(delivery).await;
            order.lock().unwrap().push("inner-after");
            result
        }) as crate::spi::SpiFuture<'static, Result<(), DeliveryError>>
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
fn admission_permit_is_released_once_on_drop() {
    let tracker = AdmissionTracker::new(1).unwrap();
    let permit = tracker.try_acquire().unwrap();
    assert!(tracker.try_acquire().is_none());
    assert_eq!(tracker.in_flight(), 1);
    drop(permit);
    assert_eq!(tracker.in_flight(), 0);
    assert!(tracker.try_acquire().is_some());
}

#[test]
fn ordering_lanes_preserve_fifo_per_key_and_allow_other_keys() {
    let lanes = OrderingLanes::new();
    let first = OrderingLaneKey::new("orders", Some("a"), Id::new(1));
    let second = OrderingLaneKey::new("orders", Some("b"), Id::new(1));
    let first_turn = lanes.enqueue(first.clone(), 1);
    assert!(first_turn.is_leader());
    let queued_turn = lanes.enqueue(first.clone(), 2);
    assert!(!queued_turn.is_leader());
    let other = lanes.enqueue(second, 3).take().unwrap();
    assert_eq!(*other.value(), 3);
    let first_guard = first_turn.take().unwrap();
    assert_eq!(*first_guard.value(), 1);
    let waiting = std::thread::spawn(move || queued_turn.take().map(|guard| *guard.value()));
    drop(first_guard);
    assert_eq!(waiting.join().unwrap(), Some(2));
}

#[test]
fn failure_directive_maps_to_terminal_spi_disposition() {
    use crate::pipeline::subscriber::DeliveryFailureAction;
    use crate::spi::SettlementCapabilities;
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
    assert_eq!(
        SubscriberPipeline::success_disposition(SettlementCapabilities::AcceptOnly),
        Some(DeliveryDisposition::Accept)
    );
}

#[test]
fn subscriber_middleware_panic_is_contained_as_delivery_failure() {
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
fn attempt_failure_converts_without_losing_delivery_error_as_source() {
    let original = DeliveryError::Handler {
        source: Box::new(std::io::Error::other("original")),
    };
    let attempt = SubscriberPipeline::attempt_error(original);
    assert_eq!(attempt.kind(), "delivery");
    assert!(
        std::error::Error::source(&attempt)
            .unwrap()
            .to_string()
            .contains("original")
    );
}

#[test]
fn dead_letter_record_preserves_original_and_prevents_recursive_dead_lettering() {
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
        source: Box::new(std::io::Error::other("failed")),
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

#[test]
fn async_ordering_waits_without_blocking_and_holds_turn_through_handler_scope() {
    let lanes = AsyncOrderingLanes::new();
    let key = OrderingLaneKey::new("orders", Some("customer-1"), Id::new(9));
    let first = block_on(lanes.enqueue(key.clone(), 1)).unwrap();
    assert_eq!(*first.value(), 1);

    let mut waiting = Box::pin(lanes.enqueue(key, 2));
    let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = std::task::Context::from_waker(&waker);
    assert!(matches!(waiting.as_mut().poll(&mut context), std::task::Poll::Pending));
    drop(first);
    let second = block_on(waiting).unwrap();
    assert_eq!(*second.value(), 2);
}

#[test]
fn async_ordering_invokes_custom_waker_after_releasing_lane_lock() {
    use std::future::Future;
    use std::task::Context;
    use std::task::Poll;

    let lanes = AsyncOrderingLanes::new();
    let key = OrderingLaneKey::new("orders", Some("customer-2"), Id::new(10));
    let first = block_on(lanes.enqueue(key.clone(), 1)).unwrap();
    let waiting = Arc::new(Mutex::new(Some(Box::pin(lanes.enqueue(key, 2)))));
    let completed_guard = Arc::new(Mutex::new(None));
    let completed = Arc::new(AtomicBool::new(false));
    let waker = std::task::Waker::from(Arc::new(ReentrantWake {
        waiting: waiting.clone(),
        completed_guard: completed_guard.clone(),
        completed: completed.clone(),
    }));
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        waiting.lock().unwrap().as_mut().unwrap().as_mut().poll(&mut context),
        Poll::Pending
    ));

    // This Waker immediately polls the waiting future again. The lane's
    // internal mutex must therefore already be unlocked when it is called.
    drop(first);
    assert!(completed.load(Ordering::Acquire));
    assert_eq!(*completed_guard.lock().unwrap().as_ref().unwrap().value(), 2);
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        std::thread::park();
    }
}

struct ThreadWake(std::thread::Thread);
impl std::task::Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

type WaitingOrderingTurn = Arc<Mutex<Option<std::pin::Pin<Box<crate::pipeline::AsyncOrderingTurn<usize>>>>>>;
type CompletedOrderingGuard = Arc<Mutex<Option<crate::pipeline::AsyncOrderingGuard<usize>>>>;

struct ReentrantWake {
    waiting: WaitingOrderingTurn,
    completed_guard: CompletedOrderingGuard,
    completed: Arc<AtomicBool>,
}

impl std::task::Wake for ReentrantWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        use std::future::Future;
        let mut waiting = self.waiting.lock().unwrap();
        let Some(future) = waiting.as_mut() else {
            return;
        };
        let fallback = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
        let mut context = std::task::Context::from_waker(&fallback);
        if let std::task::Poll::Ready(guard) = future.as_mut().poll(&mut context) {
            *waiting = None;
            *self.completed_guard.lock().unwrap() = guard;
            self.completed.store(true, Ordering::Release);
        }
    }
}
