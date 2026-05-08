/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Coverage-only helpers for [`super::LocalEventBusInner`] internals.

use std::any::{
    Any,
    TypeId,
};
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{
    Arc,
    Mutex,
};
use std::thread;
use std::time::Duration;

use qubit_thread_pool::ExecutorService;

use crate::core::SubscriptionState;
use crate::core::subscribe_options::wrap_dead_letter_strategy;
use crate::{
    DeadLetterPayload,
    DeadLetterRecord,
    EventBusError,
    EventBusResult,
    EventEnvelope,
    EventEnvelopeMetadata,
    PublishOptions,
    SubscribeOptions,
    SubscriberInterceptorAnyChain,
    Topic,
    TopicKey,
};

use super::{
    LocalEventBusInner,
    LocalEventBusRuntimeOptions,
    OrderedLaneRunnerGuard,
    OrderedLaneTask,
    OrderedLaneTurn,
    OrderedProcessingLane,
    ProcessingTracker,
    take_subscription_task,
};
use crate::local::erased_subscription::ErasedSubscription;
use crate::local::ordering_lane_key::OrderingLaneKey;
use crate::local::processing_task::ProcessingTask;
use crate::local::publisher_interceptor_entry::PublisherInterceptorEntry;
use crate::local::subscriber_interceptor_entry::SubscriberInterceptorEntry;

struct CoveragePublisherInterceptor;

impl PublisherInterceptorEntry for CoveragePublisherInterceptor {
    fn payload_type_id(&self) -> TypeId {
        TypeId::of::<String>()
    }

    fn intercept(
        &self,
        envelope: Box<dyn Any + Send>,
    ) -> EventBusResult<Option<Box<dyn Any + Send>>> {
        Ok(Some(envelope))
    }
}

struct CoverageSubscriberInterceptor;

impl SubscriberInterceptorEntry for CoverageSubscriberInterceptor {
    fn payload_type_id(&self) -> TypeId {
        TypeId::of::<String>()
    }

    fn wrap_handler(
        &self,
        handler: Box<dyn Any + Send + Sync>,
    ) -> EventBusResult<Box<dyn Any + Send + Sync>> {
        Ok(handler)
    }
}

struct CoverageSubscription;

impl ErasedSubscription for CoverageSubscription {
    fn id(&self) -> usize {
        1
    }

    fn priority(&self) -> i32 {
        0
    }

    fn deactivate(&self) {}

    fn dispatch(
        &self,
        _envelope: Box<dyn Any + Send>,
        _bus: Arc<LocalEventBusInner>,
        _allow_stopping: bool,
    ) -> EventBusResult<()> {
        Ok(())
    }
}

fn coverage_noop_task() {}

fn coverage_ignore_error(_error: &EventBusError) {}

fn coverage_global_publisher(metadata: EventEnvelopeMetadata) -> Option<EventEnvelopeMetadata> {
    Some(metadata)
}

fn coverage_global_subscriber(
    _metadata: EventEnvelopeMetadata,
    chain: SubscriberInterceptorAnyChain,
) -> EventBusResult<()> {
    chain.proceed()
}

fn coverage_global_subscriber_next() -> EventBusResult<()> {
    Ok(())
}

fn coverage_dead_letter_record_strategy(
    _subscriber_id: &str,
    _event: &EventEnvelope<DeadLetterRecord>,
    _error: &EventBusError,
    _options: &SubscribeOptions<DeadLetterRecord>,
) -> EventBusResult<Option<EventEnvelope<DeadLetterPayload>>> {
    Ok(None)
}

fn inactive_subscription_state() -> Arc<SubscriptionState> {
    let state = Arc::new(SubscriptionState::active());
    state.deactivate();
    state
}

/// Exercises internal defensive branches that require poisoned private locks.
///
/// # Returns
/// Errors produced by intentionally poisoning internal locks and task state.
pub fn coverage_exercise_local_event_bus_inner_defensive_paths() -> Vec<EventBusError> {
    fn empty_inner() -> LocalEventBusInner {
        LocalEventBusInner::new(LocalEventBusRuntimeOptions {
            default_publish_options: HashMap::new(),
            default_subscribe_options: HashMap::new(),
            default_dead_letter_strategies: HashMap::new(),
            global_default_dead_letter_strategy: None,
            global_publisher_interceptors: Vec::new(),
            global_subscriber_interceptors: Vec::new(),
            publisher_interceptors: Vec::new(),
            subscriber_interceptors: Vec::new(),
            subscription_handler_pool_size: 1,
            subscription_handler_queue_capacity: None,
        })
    }

    fn poison_mutex<T>(mutex: &Mutex<T>) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = mutex.lock().expect("coverage mutex should lock");
            panic!("coverage poison");
        }));
    }

    fn push_error<T>(errors: &mut Vec<EventBusError>, result: EventBusResult<T>) {
        errors.extend(result.err());
    }

    fn poison_tracker_counts_and_notify(tracker: &Arc<ProcessingTracker>) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut counts = tracker.counts.lock().expect("coverage tracker should lock");
            counts.clear();
            tracker.condvar.notify_all();
            panic!("coverage tracker poison");
        }));
    }

    let mut errors = Vec::new();
    let topic_key = TopicKey::new(
        "coverage-inner-defensive".to_string(),
        TypeId::of::<String>(),
    );

    coverage_noop_task();
    coverage_ignore_error(&EventBusError::handler_failed("coverage"));

    let mut default_publish_options = HashMap::new();
    default_publish_options.insert(
        TypeId::of::<DeadLetterRecord>(),
        Arc::new(PublishOptions::<DeadLetterRecord>::empty()) as Arc<dyn Any + Send + Sync>,
    );
    let mut default_subscribe_options = HashMap::new();
    default_subscribe_options.insert(
        TypeId::of::<DeadLetterRecord>(),
        Arc::new(SubscribeOptions::<DeadLetterRecord>::empty()) as Arc<dyn Any + Send + Sync>,
    );
    let mut default_dead_letter_strategies = HashMap::new();
    let default_dead_letter_strategy: Arc<
        crate::core::subscribe_options::DeadLetterStrategyFn<DeadLetterRecord>,
    > = wrap_dead_letter_strategy(coverage_dead_letter_record_strategy);
    default_dead_letter_strategies.insert(
        TypeId::of::<DeadLetterRecord>(),
        Arc::new(default_dead_letter_strategy) as Arc<dyn Any + Send + Sync>,
    );
    let default_options_inner = LocalEventBusInner::new(LocalEventBusRuntimeOptions {
        default_publish_options,
        default_subscribe_options,
        default_dead_letter_strategies,
        global_default_dead_letter_strategy: None,
        global_publisher_interceptors: Vec::new(),
        global_subscriber_interceptors: Vec::new(),
        publisher_interceptors: Vec::new(),
        subscriber_interceptors: Vec::new(),
        subscription_handler_pool_size: 1,
        subscription_handler_queue_capacity: None,
    });
    assert!(
        default_options_inner
            .default_publish_options::<DeadLetterRecord>()
            .is_some()
    );
    assert!(
        default_options_inner
            .default_subscribe_options::<DeadLetterRecord>()
            .is_some()
    );
    let default_dead_letter_strategy = default_options_inner
        .default_dead_letter_strategy::<DeadLetterRecord>()
        .expect("default dead-letter strategy should be configured");
    let dead_letter_topic = Topic::<DeadLetterRecord>::try_new("coverage-dead-letter-record")
        .expect("topic should build");
    let dead_letter_record = DeadLetterRecord::new(
        qubit_metadata::Metadata::new(),
        Arc::new("payload".to_string()),
    );
    default_dead_letter_strategy
        .create_dead_letter(
            "coverage-sub",
            &EventEnvelope::create(dead_letter_topic, dead_letter_record),
            &EventBusError::handler_failed("coverage"),
            &SubscribeOptions::empty(),
        )
        .expect("coverage dead-letter strategy should run");

    let publisher_interceptor = CoveragePublisherInterceptor;
    assert_eq!(
        publisher_interceptor.payload_type_id(),
        TypeId::of::<String>(),
    );
    let publisher_output = publisher_interceptor
        .intercept(Box::new("payload".to_string()))
        .expect("coverage publisher interceptor should pass payload");
    assert!(publisher_output.is_some());
    let global_metadata = EventEnvelope::create(
        Topic::<String>::try_new("coverage-global-interceptor").expect("topic should build"),
        "payload".to_string(),
    )
    .metadata();
    assert!(coverage_global_publisher(global_metadata.clone()).is_some());
    coverage_global_subscriber(
        global_metadata,
        SubscriberInterceptorAnyChain::new(Arc::new(coverage_global_subscriber_next)),
    )
    .expect("coverage global subscriber should proceed");

    let subscriber_interceptor = CoverageSubscriberInterceptor;
    assert_eq!(
        subscriber_interceptor.payload_type_id(),
        TypeId::of::<String>(),
    );
    let subscriber_output = subscriber_interceptor
        .wrap_handler(Box::new("handler".to_string()))
        .expect("coverage subscriber interceptor should pass handler");
    assert!(subscriber_output.downcast::<String>().is_ok());

    let registration_inner = empty_inner();
    registration_inner
        .add_publisher_interceptor(Arc::new(CoveragePublisherInterceptor))
        .expect("coverage publisher interceptor should register");
    assert_eq!(
        registration_inner
            .publisher_interceptors()
            .expect("coverage publisher interceptors should load")
            .len(),
        1,
    );
    registration_inner
        .add_global_publisher_interceptor(Arc::new(coverage_global_publisher))
        .expect("coverage global publisher interceptor should register");
    assert_eq!(
        registration_inner
            .global_publisher_interceptors()
            .expect("coverage global publisher interceptors should load")
            .len(),
        1,
    );
    registration_inner
        .add_subscriber_interceptor(Arc::new(CoverageSubscriberInterceptor))
        .expect("coverage subscriber interceptor should register");
    assert_eq!(
        registration_inner
            .subscriber_interceptors()
            .expect("coverage subscriber interceptors should load")
            .len(),
        1,
    );
    registration_inner
        .add_global_subscriber_interceptor(Arc::new(coverage_global_subscriber))
        .expect("coverage global subscriber interceptor should register");
    assert_eq!(
        registration_inner
            .global_subscriber_interceptors()
            .expect("coverage global subscriber interceptors should load")
            .len(),
        1,
    );
    registration_inner
        .add_error_observer(Arc::new(coverage_ignore_error))
        .expect("coverage error observer should register");

    let subscription = CoverageSubscription;
    assert_eq!(subscription.id(), 1);
    assert_eq!(subscription.priority(), 0);
    subscription.deactivate();
    subscription
        .dispatch(
            Box::new("payload".to_string()),
            Arc::new(empty_inner()),
            false,
        )
        .expect("coverage subscription dispatch should succeed");

    let invalid_executor_inner = LocalEventBusInner::new(LocalEventBusRuntimeOptions {
        default_publish_options: HashMap::new(),
        default_subscribe_options: HashMap::new(),
        default_dead_letter_strategies: HashMap::new(),
        global_default_dead_letter_strategy: None,
        global_publisher_interceptors: Vec::new(),
        global_subscriber_interceptors: Vec::new(),
        publisher_interceptors: Vec::new(),
        subscriber_interceptors: Vec::new(),
        subscription_handler_pool_size: 0,
        subscription_handler_queue_capacity: None,
    });
    errors.push(
        invalid_executor_inner
            .mark_started()
            .expect_err("invalid executor should fail startup"),
    );

    let direct_task_inner = Arc::new(empty_inner());
    direct_task_inner
        .mark_started()
        .expect("coverage direct task inner should start");
    direct_task_inner
        .submit_processing_task(coverage_noop_task, false)
        .expect("coverage direct task should submit");
    if let Some(executor) = direct_task_inner.take_executor() {
        executor.shutdown();
        while !executor.is_terminated() {
            thread::sleep(Duration::from_millis(1));
        }
    }
    if let Some(delay_scheduler) = direct_task_inner.take_delay_scheduler() {
        delay_scheduler.shutdown();
    }

    let mut one_shot = Some(coverage_noop_task);
    let task = take_subscription_task(&mut one_shot).expect("first task take should succeed");
    task();
    errors.extend(take_subscription_task(&mut one_shot).err());

    let lane_bus = Arc::new(empty_inner());
    let lane_key = OrderingLaneKey::new(topic_key.clone(), "coverage-order", 1);
    let mut lane = OrderedProcessingLane::new();
    assert_eq!(lane.release_front_queue_slot(), 0);
    lane.push(
        ProcessingTask::new(Arc::clone(&lane_bus), topic_key.clone(), coverage_noop_task),
        false,
    );
    assert_eq!(lane.release_front_queue_slot(), 0);
    lane.push(
        ProcessingTask::new(Arc::clone(&lane_bus), topic_key.clone(), coverage_noop_task),
        true,
    );
    assert_eq!(lane.release_all_queue_slots(), 1);
    drop(lane);
    lane_bus.release_ordered_queue_slots(0);

    let cancel_bus = Arc::new(empty_inner());
    let mut cancel_lane = OrderedProcessingLane::new();
    cancel_lane.push(
        ProcessingTask::new(
            Arc::clone(&cancel_bus),
            topic_key.clone(),
            coverage_noop_task,
        ),
        true,
    );
    cancel_bus
        .ordered_queued_task_count
        .store(1, Ordering::SeqCst);
    cancel_bus
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(lane_key.clone(), cancel_lane);
    {
        let _guard = OrderedLaneRunnerGuard::new(Arc::clone(&cancel_bus), lane_key.clone());
    }
    assert!(
        cancel_bus
            .ordering_lanes
            .lock()
            .expect("coverage lanes should lock")
            .get(&lane_key)
            .is_none()
    );
    assert_eq!(
        cancel_bus.ordered_queued_task_count.load(Ordering::SeqCst),
        0
    );

    let pop_bus = Arc::new(empty_inner());
    let missing_lane_key = OrderingLaneKey::new(topic_key.clone(), "missing-order", 1);
    let mut missing_guard =
        OrderedLaneRunnerGuard::new(Arc::clone(&pop_bus), missing_lane_key.clone());
    assert!(
        pop_bus
            .pop_ordered_lane_task(&missing_lane_key, &mut missing_guard)
            .is_none()
    );

    let empty_lane_key = OrderingLaneKey::new(topic_key.clone(), "empty-order", 1);
    pop_bus
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(empty_lane_key.clone(), OrderedProcessingLane::new());
    let mut empty_guard = OrderedLaneRunnerGuard::new(Arc::clone(&pop_bus), empty_lane_key.clone());
    assert!(
        pop_bus
            .pop_ordered_lane_task(&empty_lane_key, &mut empty_guard)
            .is_none()
    );

    let reserved_lane_key = OrderingLaneKey::new(topic_key.clone(), "reserved-order", 1);
    let mut reserved_lane = OrderedProcessingLane::new();
    reserved_lane.push(
        ProcessingTask::new(Arc::clone(&pop_bus), topic_key.clone(), coverage_noop_task),
        true,
    );
    pop_bus.ordered_queued_task_count.store(1, Ordering::SeqCst);
    pop_bus
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(reserved_lane_key.clone(), reserved_lane);
    let mut reserved_guard =
        OrderedLaneRunnerGuard::new(Arc::clone(&pop_bus), reserved_lane_key.clone());
    match pop_bus
        .pop_ordered_lane_task(&reserved_lane_key, &mut reserved_guard)
        .expect("reserved lane should yield a task")
    {
        OrderedLaneTask::Ready(task) => task.run(),
        OrderedLaneTask::Delayed(..) => panic!("reserved ready lane should not be delayed"),
    }

    let delayed_lane_key = OrderingLaneKey::new(topic_key.clone(), "delayed-order", 1);
    let delayed_state = Arc::new(SubscriptionState::active());
    let mut delayed_lane = OrderedProcessingLane::new();
    delayed_lane.push_delayed(
        ProcessingTask::new(Arc::clone(&pop_bus), topic_key.clone(), coverage_noop_task),
        false,
        Duration::from_millis(50),
        Arc::clone(&delayed_state),
    );
    pop_bus
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(delayed_lane_key.clone(), delayed_lane);
    let mut delayed_guard =
        OrderedLaneRunnerGuard::new(Arc::clone(&pop_bus), delayed_lane_key.clone());
    match pop_bus
        .pop_ordered_lane_task(&delayed_lane_key, &mut delayed_guard)
        .expect("delayed lane should yield delayed work")
    {
        OrderedLaneTask::Delayed(remaining, state) => {
            assert!(remaining <= Duration::from_millis(50));
            assert!(Arc::ptr_eq(&state, &delayed_state));
        }
        OrderedLaneTask::Ready(_) => panic!("delayed lane should not be ready"),
    }
    pop_bus.cancel_ordered_lane(&delayed_lane_key);

    Arc::clone(&pop_bus).run_ordered_lane(missing_lane_key.clone());

    let inactive_lane_key = OrderingLaneKey::new(topic_key.clone(), "inactive-delayed-order", 1);
    let inactive_state = inactive_subscription_state();
    let mut inactive_lane = OrderedProcessingLane::new();
    inactive_lane.push_delayed(
        ProcessingTask::new(Arc::clone(&pop_bus), topic_key.clone(), coverage_noop_task),
        true,
        Duration::from_millis(50),
        inactive_state,
    );
    pop_bus.ordered_queued_task_count.store(1, Ordering::SeqCst);
    pop_bus
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(inactive_lane_key.clone(), inactive_lane);
    let mut inactive_guard =
        OrderedLaneRunnerGuard::new(Arc::clone(&pop_bus), inactive_lane_key.clone());
    assert!(
        pop_bus
            .pop_ordered_lane_task(&inactive_lane_key, &mut inactive_guard)
            .is_none()
    );
    assert_eq!(pop_bus.ordered_queued_task_count.load(Ordering::SeqCst), 0);

    let poisoned_pop_inner = Arc::new(empty_inner());
    let poisoned_pop_lane_key = OrderingLaneKey::new(topic_key.clone(), "poisoned-pop-order", 1);
    poison_mutex(&poisoned_pop_inner.ordering_lanes);
    let mut poisoned_pop_guard = OrderedLaneRunnerGuard::new(
        Arc::clone(&poisoned_pop_inner),
        poisoned_pop_lane_key.clone(),
    );
    assert!(
        poisoned_pop_inner
            .pop_ordered_lane_task(&poisoned_pop_lane_key, &mut poisoned_pop_guard)
            .is_none()
    );

    let mut finish_guard =
        OrderedLaneRunnerGuard::new(Arc::clone(&pop_bus), missing_lane_key.clone());
    assert!(matches!(
        pop_bus.finish_ordered_lane_turn(&missing_lane_key, &mut finish_guard),
        OrderedLaneTurn::Drained
    ));

    let rejected_order_inner = Arc::new(empty_inner());
    rejected_order_inner
        .mark_started()
        .expect("coverage ordered inner should start");
    {
        let lifecycle = rejected_order_inner
            .lifecycle
            .lock()
            .expect("coverage lifecycle should lock");
        lifecycle
            .executor
            .as_ref()
            .expect("coverage executor should exist")
            .shutdown();
    }
    errors.push(
        rejected_order_inner
            .submit_ordered_processing_task(
                OrderingLaneKey::new(topic_key.clone(), "rejected-order", 1),
                ProcessingTask::new(
                    Arc::clone(&rejected_order_inner),
                    topic_key.clone(),
                    coverage_noop_task,
                ),
                false,
            )
            .expect_err("shutdown executor should reject ordered runner"),
    );

    let poisoned_lifecycle_order_inner = Arc::new(empty_inner());
    poison_mutex(&poisoned_lifecycle_order_inner.lifecycle);
    errors.push(
        poisoned_lifecycle_order_inner
            .submit_ordered_processing_task(
                OrderingLaneKey::new(topic_key.clone(), "poisoned-lifecycle-order", 1),
                ProcessingTask::new(
                    Arc::clone(&poisoned_lifecycle_order_inner),
                    topic_key.clone(),
                    coverage_noop_task,
                ),
                false,
            )
            .expect_err("poisoned lifecycle should reject ordered submission"),
    );

    let poisoned_lanes_order_inner = Arc::new(empty_inner());
    poisoned_lanes_order_inner
        .mark_started()
        .expect("coverage poisoned lanes inner should start");
    poison_mutex(&poisoned_lanes_order_inner.ordering_lanes);
    errors.push(
        poisoned_lanes_order_inner
            .submit_ordered_processing_task(
                OrderingLaneKey::new(topic_key.clone(), "poisoned-lanes-order", 1),
                ProcessingTask::new(
                    Arc::clone(&poisoned_lanes_order_inner),
                    topic_key.clone(),
                    coverage_noop_task,
                ),
                false,
            )
            .expect_err("poisoned lanes should reject ordered submission"),
    );
    if let Some(executor) = poisoned_lanes_order_inner.take_executor() {
        executor.shutdown();
    }

    let no_executor_order_inner = Arc::new(empty_inner());
    let no_executor_lane_key = OrderingLaneKey::new(topic_key.clone(), "no-executor-order", 1);
    let mut no_executor_lane = OrderedProcessingLane::new();
    no_executor_lane.push(
        ProcessingTask::new(
            Arc::clone(&no_executor_order_inner),
            topic_key.clone(),
            coverage_noop_task,
        ),
        false,
    );
    no_executor_order_inner
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(no_executor_lane_key.clone(), no_executor_lane);
    let mut no_executor_guard = OrderedLaneRunnerGuard::new(
        Arc::clone(&no_executor_order_inner),
        no_executor_lane_key.clone(),
    );
    assert!(matches!(
        no_executor_order_inner
            .finish_ordered_lane_turn(&no_executor_lane_key, &mut no_executor_guard,),
        OrderedLaneTurn::Cancelled
    ));

    let inline_inner = Arc::new(empty_inner());
    inline_inner
        .mark_started()
        .expect("coverage inline inner should start");
    {
        let lifecycle = inline_inner
            .lifecycle
            .lock()
            .expect("coverage lifecycle should lock");
        lifecycle
            .executor
            .as_ref()
            .expect("coverage executor should exist")
            .shutdown();
    }
    let inline_lane_key = OrderingLaneKey::new(topic_key.clone(), "inline-order", 1);
    let mut inline_lane = OrderedProcessingLane::new();
    inline_lane.push(
        ProcessingTask::new(
            Arc::clone(&inline_inner),
            topic_key.clone(),
            coverage_noop_task,
        ),
        false,
    );
    inline_lane.push(
        ProcessingTask::new(
            Arc::clone(&inline_inner),
            topic_key.clone(),
            coverage_noop_task,
        ),
        true,
    );
    inline_inner
        .ordered_queued_task_count
        .store(1, Ordering::SeqCst);
    inline_inner
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(inline_lane_key.clone(), inline_lane);
    Arc::clone(&inline_inner).run_ordered_lane(inline_lane_key.clone());
    assert_eq!(
        inline_inner
            .ordered_queued_task_count
            .load(Ordering::SeqCst),
        0
    );

    let delayed_rejected_inner = Arc::new(empty_inner());
    delayed_rejected_inner
        .mark_started()
        .expect("coverage delayed inner should start");
    delayed_rejected_inner
        .submit_delayed_processing_task(
            ProcessingTask::new(
                Arc::clone(&delayed_rejected_inner),
                topic_key.clone(),
                coverage_noop_task,
            ),
            Duration::from_millis(20),
            Arc::new(SubscriptionState::active()),
            false,
        )
        .expect("coverage delayed task should schedule");
    {
        let lifecycle = delayed_rejected_inner
            .lifecycle
            .lock()
            .expect("coverage lifecycle should lock");
        lifecycle
            .executor
            .as_ref()
            .expect("coverage executor should exist")
            .shutdown();
    }
    thread::sleep(Duration::from_millis(60));

    let delayed_order_existing_inner = Arc::new(empty_inner());
    delayed_order_existing_inner
        .mark_started()
        .expect("coverage delayed order inner should start");
    let delayed_order_existing_key =
        OrderingLaneKey::new(topic_key.clone(), "existing-delayed-order", 1);
    delayed_order_existing_inner
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(
            delayed_order_existing_key.clone(),
            OrderedProcessingLane::new(),
        );
    delayed_order_existing_inner
        .submit_delayed_ordered_processing_task(
            delayed_order_existing_key.clone(),
            ProcessingTask::new(
                Arc::clone(&delayed_order_existing_inner),
                topic_key.clone(),
                coverage_noop_task,
            ),
            Duration::from_millis(1),
            Arc::new(SubscriptionState::active()),
            false,
        )
        .expect("existing delayed lane should accept queued work");
    delayed_order_existing_inner.cancel_ordered_lane(&delayed_order_existing_key);

    let delayed_order_rejected_inner = Arc::new(empty_inner());
    delayed_order_rejected_inner
        .mark_started()
        .expect("coverage rejected delayed order inner should start");
    {
        let lifecycle = delayed_order_rejected_inner
            .lifecycle
            .lock()
            .expect("coverage lifecycle should lock");
        lifecycle
            .executor
            .as_ref()
            .expect("coverage executor should exist")
            .shutdown();
    }
    errors.push(
        delayed_order_rejected_inner
            .submit_delayed_ordered_processing_task(
                OrderingLaneKey::new(topic_key.clone(), "rejected-delayed-order", 1),
                ProcessingTask::new(
                    Arc::clone(&delayed_order_rejected_inner),
                    topic_key.clone(),
                    coverage_noop_task,
                ),
                Duration::from_millis(1),
                Arc::new(SubscriptionState::active()),
                false,
            )
            .expect_err("shutdown executor should reject delayed ordered runner"),
    );

    let delayed_order_no_delay_executor_inner = Arc::new(empty_inner());
    let delayed_order_no_delay_executor_key =
        OrderingLaneKey::new(topic_key.clone(), "no-delay-executor-order", 1);
    let mut delayed_order_no_delay_executor_lane = OrderedProcessingLane::new();
    delayed_order_no_delay_executor_lane.push_delayed(
        ProcessingTask::new(
            Arc::clone(&delayed_order_no_delay_executor_inner),
            topic_key.clone(),
            coverage_noop_task,
        ),
        false,
        Duration::from_millis(1),
        Arc::new(SubscriptionState::active()),
    );
    delayed_order_no_delay_executor_inner
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(
            delayed_order_no_delay_executor_key.clone(),
            delayed_order_no_delay_executor_lane,
        );
    Arc::clone(&delayed_order_no_delay_executor_inner)
        .run_ordered_lane(delayed_order_no_delay_executor_key);

    let delayed_order_inline_inner = Arc::new(empty_inner());
    delayed_order_inline_inner
        .mark_started()
        .expect("coverage delayed order inline inner should start");
    let delayed_order_inline_key =
        OrderingLaneKey::new(topic_key.clone(), "inline-delayed-order", 1);
    let delayed_order_inline_state = Arc::new(SubscriptionState::active());
    let mut delayed_order_inline_lane = OrderedProcessingLane::new();
    delayed_order_inline_lane.push_delayed(
        ProcessingTask::new(
            Arc::clone(&delayed_order_inline_inner),
            topic_key.clone(),
            coverage_noop_task,
        ),
        false,
        Duration::from_millis(20),
        Arc::clone(&delayed_order_inline_state),
    );
    delayed_order_inline_inner
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(delayed_order_inline_key.clone(), delayed_order_inline_lane);
    Arc::clone(&delayed_order_inline_inner).run_ordered_lane(delayed_order_inline_key);
    {
        let lifecycle = delayed_order_inline_inner
            .lifecycle
            .lock()
            .expect("coverage lifecycle should lock");
        lifecycle
            .executor
            .as_ref()
            .expect("coverage executor should exist")
            .shutdown();
    }
    thread::sleep(Duration::from_millis(60));

    let delayed_order_cancel_inner = Arc::new(empty_inner());
    delayed_order_cancel_inner
        .mark_started()
        .expect("coverage delayed order cancel inner should start");
    let delayed_order_cancel_key =
        OrderingLaneKey::new(topic_key.clone(), "cancel-delayed-order", 1);
    let delayed_order_cancel_state = Arc::new(SubscriptionState::active());
    let mut delayed_order_cancel_lane = OrderedProcessingLane::new();
    delayed_order_cancel_lane.push_delayed(
        ProcessingTask::new(
            Arc::clone(&delayed_order_cancel_inner),
            topic_key.clone(),
            coverage_noop_task,
        ),
        false,
        Duration::from_millis(100),
        Arc::clone(&delayed_order_cancel_state),
    );
    delayed_order_cancel_inner
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(delayed_order_cancel_key.clone(), delayed_order_cancel_lane);
    Arc::clone(&delayed_order_cancel_inner).run_ordered_lane(delayed_order_cancel_key);
    delayed_order_cancel_state.deactivate();
    thread::sleep(Duration::from_millis(20));

    let delayed_order_poisoned_inner = Arc::new(empty_inner());
    delayed_order_poisoned_inner
        .mark_started()
        .expect("coverage delayed order poisoned inner should start");
    let delayed_order_poisoned_key =
        OrderingLaneKey::new(topic_key.clone(), "poisoned-delayed-order", 1);
    let delayed_order_poisoned_state = Arc::new(SubscriptionState::active());
    let mut delayed_order_poisoned_lane = OrderedProcessingLane::new();
    delayed_order_poisoned_lane.push_delayed(
        ProcessingTask::new(
            Arc::clone(&delayed_order_poisoned_inner),
            topic_key.clone(),
            coverage_noop_task,
        ),
        false,
        Duration::from_millis(20),
        delayed_order_poisoned_state,
    );
    delayed_order_poisoned_inner
        .ordering_lanes
        .lock()
        .expect("coverage lanes should lock")
        .insert(delayed_order_poisoned_key, delayed_order_poisoned_lane);
    Arc::clone(&delayed_order_poisoned_inner).run_ordered_lane(OrderingLaneKey::new(
        topic_key.clone(),
        "poisoned-delayed-order",
        1,
    ));
    poison_mutex(&delayed_order_poisoned_inner.lifecycle);
    thread::sleep(Duration::from_millis(60));

    let draining_inner = empty_inner();
    assert!(draining_inner.mark_started().expect("inner should start"));
    assert!(draining_inner.mark_stopping());
    errors.push(
        draining_inner
            .mark_started()
            .expect_err("draining executor should block restart"),
    );
    assert!(!draining_inner.mark_stopping());
    if let Some(executor) = draining_inner.take_executor() {
        executor.shutdown();
    }

    let lifecycle_inner = empty_inner();
    errors.push(
        lifecycle_inner
            .submit_processing_task(coverage_noop_task, false)
            .expect_err("stopped executor should reject tasks"),
    );
    poison_mutex(&lifecycle_inner.lifecycle);
    assert!(lifecycle_inner.mark_stopped().is_none());
    assert!(!lifecycle_inner.mark_stopping());
    assert!(lifecycle_inner.take_executor().is_none());
    assert!(lifecycle_inner.take_delay_scheduler().is_none());
    assert!(!lifecycle_inner.is_started());
    errors.push(
        lifecycle_inner
            .mark_started()
            .expect_err("poisoned lifecycle should reject start"),
    );
    errors.push(
        lifecycle_inner
            .submit_processing_task(coverage_noop_task, false)
            .expect_err("poisoned lifecycle should reject task submission"),
    );

    let stopped_delayed_inner = Arc::new(empty_inner());
    errors.push(
        stopped_delayed_inner
            .submit_delayed_processing_task(
                ProcessingTask::new(
                    Arc::clone(&stopped_delayed_inner),
                    topic_key.clone(),
                    coverage_noop_task,
                ),
                Duration::from_millis(1),
                Arc::new(SubscriptionState::active()),
                false,
            )
            .expect_err("stopped delayed executor should reject submission"),
    );

    let poisoned_delayed_inner = Arc::new(empty_inner());
    poison_mutex(&poisoned_delayed_inner.lifecycle);
    errors.push(
        poisoned_delayed_inner
            .submit_delayed_processing_task(
                ProcessingTask::new(
                    Arc::clone(&poisoned_delayed_inner),
                    topic_key.clone(),
                    coverage_noop_task,
                ),
                Duration::from_millis(1),
                Arc::new(SubscriptionState::active()),
                false,
            )
            .expect_err("poisoned lifecycle should reject delayed submission"),
    );

    let delayed_observe_error_inner = Arc::new(empty_inner());
    delayed_observe_error_inner
        .mark_started()
        .expect("coverage delayed observe inner should start");
    delayed_observe_error_inner
        .submit_delayed_processing_task(
            ProcessingTask::new(
                Arc::clone(&delayed_observe_error_inner),
                topic_key.clone(),
                coverage_noop_task,
            ),
            Duration::from_millis(20),
            Arc::new(SubscriptionState::active()),
            false,
        )
        .expect("coverage delayed observe task should schedule");
    poison_mutex(&delayed_observe_error_inner.lifecycle);
    thread::sleep(Duration::from_millis(60));

    let poisoned_lifecycle_delayed_order_inner = Arc::new(empty_inner());
    poison_mutex(&poisoned_lifecycle_delayed_order_inner.lifecycle);
    errors.push(
        poisoned_lifecycle_delayed_order_inner
            .submit_delayed_ordered_processing_task(
                OrderingLaneKey::new(topic_key.clone(), "poisoned-lifecycle-delayed-order", 1),
                ProcessingTask::new(
                    Arc::clone(&poisoned_lifecycle_delayed_order_inner),
                    topic_key.clone(),
                    coverage_noop_task,
                ),
                Duration::from_millis(1),
                Arc::new(SubscriptionState::active()),
                false,
            )
            .expect_err("poisoned lifecycle should reject delayed ordered submission"),
    );

    let poisoned_lanes_delayed_order_inner = Arc::new(empty_inner());
    poisoned_lanes_delayed_order_inner
        .mark_started()
        .expect("coverage poisoned delayed lanes inner should start");
    poison_mutex(&poisoned_lanes_delayed_order_inner.ordering_lanes);
    errors.push(
        poisoned_lanes_delayed_order_inner
            .submit_delayed_ordered_processing_task(
                OrderingLaneKey::new(topic_key.clone(), "poisoned-lanes-delayed-order", 1),
                ProcessingTask::new(
                    Arc::clone(&poisoned_lanes_delayed_order_inner),
                    topic_key.clone(),
                    coverage_noop_task,
                ),
                Duration::from_millis(1),
                Arc::new(SubscriptionState::active()),
                false,
            )
            .expect_err("poisoned lanes should reject delayed ordered submission"),
    );
    if let Some(executor) = poisoned_lanes_delayed_order_inner.take_executor() {
        executor.shutdown();
    }

    let poisoned_reschedule_inner = Arc::new(empty_inner());
    poison_mutex(&poisoned_reschedule_inner.lifecycle);
    errors.push(
        poisoned_reschedule_inner
            .submit_ordered_lane_runner_after_delay(
                OrderingLaneKey::new(topic_key.clone(), "poisoned-delay-reschedule", 1),
                Duration::from_millis(1),
                Arc::new(SubscriptionState::active()),
            )
            .expect_err("poisoned lifecycle should reject ordered delay reschedule"),
    );

    let publisher_inner = empty_inner();
    poison_mutex(&publisher_inner.publisher_interceptors);
    errors.push(
        publisher_inner
            .add_publisher_interceptor(Arc::new(CoveragePublisherInterceptor))
            .expect_err("poisoned publisher interceptors should reject add"),
    );
    push_error(&mut errors, publisher_inner.publisher_interceptors());

    let global_publisher_inner = empty_inner();
    poison_mutex(&global_publisher_inner.global_publisher_interceptors);
    errors.push(
        global_publisher_inner
            .add_global_publisher_interceptor(Arc::new(coverage_global_publisher))
            .expect_err("poisoned global publisher interceptors should reject add"),
    );
    push_error(
        &mut errors,
        global_publisher_inner.global_publisher_interceptors(),
    );

    let subscriber_inner = empty_inner();
    poison_mutex(&subscriber_inner.subscriber_interceptors);
    errors.push(
        subscriber_inner
            .add_subscriber_interceptor(Arc::new(CoverageSubscriberInterceptor))
            .expect_err("poisoned subscriber interceptors should reject add"),
    );
    push_error(&mut errors, subscriber_inner.subscriber_interceptors());

    let global_subscriber_inner = empty_inner();
    poison_mutex(&global_subscriber_inner.global_subscriber_interceptors);
    errors.push(
        global_subscriber_inner
            .add_global_subscriber_interceptor(Arc::new(coverage_global_subscriber))
            .expect_err("poisoned global subscriber interceptors should reject add"),
    );
    push_error(
        &mut errors,
        global_subscriber_inner.global_subscriber_interceptors(),
    );

    let observer_inner = empty_inner();
    poison_mutex(&observer_inner.error_observers);
    observer_inner.observe_error(&EventBusError::handler_failed("coverage"));
    errors.push(
        observer_inner
            .add_error_observer(Arc::new(coverage_ignore_error))
            .expect_err("poisoned error observers should reject add"),
    );

    let subscriptions_inner = empty_inner();
    subscriptions_inner
        .unsubscribe(&topic_key, 1)
        .expect("missing subscription should be a no-op");
    poison_mutex(&subscriptions_inner.subscriptions);
    errors.push(
        subscriptions_inner
            .add_subscription(topic_key.clone(), Arc::new(CoverageSubscription))
            .expect_err("poisoned subscriptions should reject add"),
    );
    push_error(
        &mut errors,
        subscriptions_inner.subscriptions_for(&topic_key),
    );
    errors.push(
        subscriptions_inner
            .unsubscribe(&topic_key, 1)
            .expect_err("poisoned subscriptions should reject unsubscribe"),
    );
    subscriptions_inner.clear_subscriptions();

    let tracker_inner = empty_inner();
    tracker_inner.finish_processing(&topic_key);
    poison_mutex(&tracker_inner.processing_tracker.counts);
    errors.push(
        tracker_inner
            .start_processing(&topic_key)
            .expect_err("poisoned tracker should reject start"),
    );
    tracker_inner.finish_processing(&topic_key);
    errors.push(
        tracker_inner
            .wait_for_idle(&topic_key)
            .expect_err("poisoned tracker should reject topic wait"),
    );
    errors.push(
        tracker_inner
            .wait_for_idle_timeout(&topic_key, Duration::from_millis(1))
            .expect_err("poisoned tracker should reject topic timeout wait"),
    );
    errors.push(
        tracker_inner
            .wait_for_all_idle()
            .expect_err("poisoned tracker should reject global wait"),
    );
    errors.push(
        tracker_inner
            .wait_for_all_idle_timeout(Duration::from_millis(1))
            .expect_err("poisoned tracker should reject timeout wait"),
    );
    push_error(&mut errors, tracker_inner.processing_tracker.has_active());

    let wait_poison_tracker = Arc::new(ProcessingTracker::new());
    wait_poison_tracker
        .start(&topic_key)
        .expect("coverage tracker should start");
    let wait_poison_key = topic_key.clone();
    let wait_poison_thread = {
        let tracker = Arc::clone(&wait_poison_tracker);
        thread::spawn(move || tracker.wait_for_idle(&wait_poison_key))
    };
    thread::sleep(Duration::from_millis(10));
    poison_tracker_counts_and_notify(&wait_poison_tracker);
    let _ = wait_poison_thread
        .join()
        .expect("coverage wait thread should not panic");

    let wait_timeout_poison_tracker = Arc::new(ProcessingTracker::new());
    wait_timeout_poison_tracker
        .start(&topic_key)
        .expect("coverage tracker should start");
    let wait_timeout_poison_key = topic_key.clone();
    let wait_timeout_poison_thread = {
        let tracker = Arc::clone(&wait_timeout_poison_tracker);
        thread::spawn(move || {
            tracker.wait_for_idle_timeout(&wait_timeout_poison_key, Duration::from_secs(1))
        })
    };
    thread::sleep(Duration::from_millis(10));
    poison_tracker_counts_and_notify(&wait_timeout_poison_tracker);
    let _ = wait_timeout_poison_thread
        .join()
        .expect("coverage wait timeout thread should not panic");

    let all_wait_poison_tracker = Arc::new(ProcessingTracker::new());
    all_wait_poison_tracker
        .start(&topic_key)
        .expect("coverage tracker should start");
    let all_wait_poison_thread = {
        let tracker = Arc::clone(&all_wait_poison_tracker);
        thread::spawn(move || tracker.wait_for_all_idle())
    };
    thread::sleep(Duration::from_millis(10));
    poison_tracker_counts_and_notify(&all_wait_poison_tracker);
    let _ = all_wait_poison_thread
        .join()
        .expect("coverage global wait thread should not panic");

    let all_wait_timeout_poison_tracker = Arc::new(ProcessingTracker::new());
    all_wait_timeout_poison_tracker
        .start(&topic_key)
        .expect("coverage tracker should start");
    let all_wait_timeout_poison_thread = {
        let tracker = Arc::clone(&all_wait_timeout_poison_tracker);
        thread::spawn(move || tracker.wait_for_all_idle_timeout(Duration::from_secs(1)))
    };
    thread::sleep(Duration::from_millis(10));
    poison_tracker_counts_and_notify(&all_wait_timeout_poison_tracker);
    let _ = all_wait_timeout_poison_thread
        .join()
        .expect("coverage global wait timeout thread should not panic");

    let tracker = ProcessingTracker::new();
    tracker
        .start(&topic_key)
        .expect("coverage tracker should start");
    assert!(
        !tracker
            .wait_for_idle_timeout(&topic_key, Duration::ZERO)
            .expect("coverage topic tracker timeout should return")
    );
    assert!(
        !tracker
            .wait_for_all_idle_timeout(Duration::ZERO)
            .expect("coverage tracker timeout should return")
    );
    assert!(
        !tracker
            .wait_for_all_idle_timeout(Duration::from_millis(20))
            .expect("coverage tracker positive timeout should return")
    );
    tracker.finish(&topic_key);

    let poisoned_ordering_inner = Arc::new(empty_inner());
    let poisoned_lane_key = OrderingLaneKey::new(topic_key, "poisoned-order", 1);
    poison_mutex(&poisoned_ordering_inner.ordering_lanes);
    let mut poisoned_guard = OrderedLaneRunnerGuard::new(
        Arc::clone(&poisoned_ordering_inner),
        poisoned_lane_key.clone(),
    );
    assert!(matches!(
        poisoned_ordering_inner.finish_ordered_lane_turn(&poisoned_lane_key, &mut poisoned_guard),
        OrderedLaneTurn::Cancelled
    ));
    poisoned_ordering_inner.cancel_ordered_lane(&poisoned_lane_key);

    errors
}
