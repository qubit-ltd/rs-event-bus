// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Coverage-only helpers for [`super::LocalEventBus`] internals.

use std::any::{
    Any,
    TypeId,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use qubit_executor::ExecutorService;

use crate::core::SubscriptionState;
use crate::{
    DeadLetterPayload,
    DeadLetterRecord,
    EventBusError,
    EventBusResult,
    EventEnvelope,
    PublishOptions,
    SubscribeOptions,
    Topic,
    TopicKey,
};

use super::{
    HandlerFn,
    IntoPublisherInterceptorResult,
    LocalEventBus,
    TypedSubscriptionEntry,
    create_publisher_interceptor_entry,
    create_subscriber_interceptor_entry,
    normalize_subscriber_interceptor_error,
    process_subscription_event,
};
use crate::LocalEventBusFactory;
use crate::local::erased_subscription::ErasedSubscription;
use crate::local::local_event_bus_inner::LocalEventBusRuntimeOptions;
use crate::local::processing_task::ProcessingTask;
use crate::local::publisher_interceptor_entry::PublisherInterceptorEntry;
use crate::local::subscriber_interceptor_chain::{
    SubscriberInterceptorChain,
    create_downstream_error_slot,
};
use crate::local::subscriber_interceptor_entry::SubscriberInterceptorEntry;

struct CoverageWrongPublisherInterceptor;

impl PublisherInterceptorEntry for CoverageWrongPublisherInterceptor {
    fn payload_type_id(&self) -> TypeId {
        TypeId::of::<String>()
    }

    fn intercept(
        &self,
        _envelope: Box<dyn Any + Send>,
    ) -> EventBusResult<Option<Box<dyn Any + Send>>> {
        let topic = Topic::<u32>::try_new("coverage-wrong-publisher")
            .expect("coverage topic should build");
        Ok(Some(Box::new(EventEnvelope::create(topic, 1_u32))))
    }
}

struct CoverageWrongSubscriberInterceptor;

impl SubscriberInterceptorEntry for CoverageWrongSubscriberInterceptor {
    fn payload_type_id(&self) -> TypeId {
        TypeId::of::<String>()
    }

    fn wrap_handler(
        &self,
        _handler: Box<dyn Any + Send + Sync>,
    ) -> EventBusResult<Box<dyn Any + Send + Sync>> {
        Ok(Box::new("wrong handler".to_string()))
    }
}

fn coverage_string_handler(
    _event: EventEnvelope<String>,
) -> EventBusResult<()> {
    Ok(())
}

fn coverage_number_handler(_event: EventEnvelope<u32>) -> EventBusResult<()> {
    Ok(())
}

fn coverage_dead_letter_record_handler(
    _event: EventEnvelope<DeadLetterRecord>,
) -> EventBusResult<()> {
    Ok(())
}

fn coverage_dead_letter_payload_handler(
    _event: EventEnvelope<DeadLetterPayload>,
) -> EventBusResult<()> {
    Ok(())
}

fn coverage_noop_task() {}

fn coverage_subscriber_passthrough(
    event: EventEnvelope<String>,
    chain: SubscriberInterceptorChain<String>,
) -> EventBusResult<()> {
    chain.proceed(event)
}

fn coverage_failing_string_handler(
    _event: EventEnvelope<String>,
) -> EventBusResult<()> {
    Err(EventBusError::handler_failed(
        "coverage downstream handler failed",
    ))
}

fn coverage_panicking_string_handler(
    _event: EventEnvelope<String>,
) -> EventBusResult<()> {
    panic!("coverage downstream handler panic");
}

fn inactive_subscription_state() -> Arc<SubscriptionState> {
    let state = Arc::new(SubscriptionState::active());
    state.deactivate();
    state
}

/// Exercises defensive local event-bus branches that are not public behavior.
///
/// # Returns
/// Errors produced by intentionally mismatched type-erased adapters.
pub fn coverage_exercise_local_event_bus_defensive_paths() -> Vec<EventBusError>
{
    let mut errors = Vec::new();
    coverage_noop_task();
    let string_topic = Topic::<String>::try_new("coverage-local-defensive")
        .expect("coverage topic should build");
    let number_topic = Topic::<u32>::try_new("coverage-local-defensive-number")
        .expect("coverage topic should build");

    let converted_envelope =
        EventEnvelope::create(string_topic.clone(), "direct".to_string())
            .into_publisher_interceptor_result()
            .expect("direct envelope conversion should succeed");
    assert!(converted_envelope.is_some());
    let converted_result: EventBusResult<EventEnvelope<String>> = Ok(
        EventEnvelope::create(string_topic.clone(), "result".to_string()),
    );
    let converted_result = converted_result
        .into_publisher_interceptor_result()
        .expect("result envelope conversion should succeed");
    assert!(converted_result.is_some());
    let converted_optional_result: EventBusResult<
        Option<EventEnvelope<String>>,
    > = Ok(Some(EventEnvelope::create(
        string_topic.clone(),
        "optional-result".to_string(),
    )));
    let converted_optional_result = converted_optional_result
        .into_publisher_interceptor_result()
        .expect("optional result conversion should succeed");
    assert!(converted_optional_result.is_some());

    let publisher = create_publisher_interceptor_entry::<String, _>(Some);
    let publisher_error = publisher
        .intercept(Box::new(EventEnvelope::create(number_topic.clone(), 1_u32)))
        .expect_err("wrong publisher envelope type should fail");
    errors.push(publisher_error);

    let mut direct_api_factory = LocalEventBusFactory::new();
    direct_api_factory
        .add_publisher_interceptor::<String, _>(
            |event: EventEnvelope<String>| {
                event.with_header("coverage", "direct")
            },
        )
        .expect("direct publisher interceptor should register");
    let direct_api_bus = direct_api_factory
        .create_started()
        .expect("coverage bus should start");
    let direct_topic = Topic::<String>::try_new("coverage-local-direct-api")
        .expect("coverage topic should build");
    direct_api_bus
        .subscribe(
            "coverage-direct-sub",
            &direct_topic,
            coverage_string_handler,
        )
        .expect("coverage direct subscriber should register");
    direct_api_bus
        .publish_with_options(
            &direct_topic,
            "payload".to_string(),
            PublishOptions::empty(),
        )
        .expect("coverage direct publish with options should succeed");
    direct_api_bus
        .publish_all_with_options(
            vec![EventEnvelope::create(
                direct_topic.clone(),
                "batch".to_string(),
            )],
            PublishOptions::empty(),
        )
        .expect("coverage direct batch publish with options should succeed");
    direct_api_bus
        .wait_for_idle(&direct_topic)
        .expect("coverage direct topic should become idle");
    let dead_letter_payload_topic = Topic::<DeadLetterPayload>::try_new(
        "coverage-local-dead-letter-handler",
    )
    .expect("coverage dead-letter payload topic should build");
    let dead_letter_subscription = direct_api_bus
        .add_dead_letter_handler(
            &dead_letter_payload_topic,
            coverage_dead_letter_payload_handler,
            SubscribeOptions::empty(),
        )
        .expect("coverage dead-letter handler should register");
    assert!(dead_letter_subscription.is_active());
    direct_api_bus
        .publish(
            &dead_letter_payload_topic,
            DeadLetterRecord::new(
                qubit_metadata::Metadata::new(),
                Arc::new("payload".to_string()),
            ),
        )
        .expect("coverage dead-letter payload should publish");
    direct_api_bus
        .wait_for_idle(&dead_letter_payload_topic)
        .expect("coverage dead-letter payload topic should become idle");
    direct_api_bus.shutdown();

    let mut error_interceptor_factory = LocalEventBusFactory::new();
    error_interceptor_factory
        .add_publisher_interceptor::<String, _>(
            |_event: EventEnvelope<String>| -> EventBusResult<Option<EventEnvelope<String>>> {
                Err(EventBusError::handler_failed("coverage publisher interceptor failed"))
            },
        )
        .expect("coverage failing publisher interceptor should register");
    let error_interceptor_bus = error_interceptor_factory
        .create_started()
        .expect("coverage bus should start");
    let interceptor_error = error_interceptor_bus
        .publish(&string_topic, "payload".to_string())
        .expect_err("coverage publisher interceptor should fail");
    errors.push(interceptor_error);
    error_interceptor_bus.shutdown();

    let wrong_publisher_bus =
        LocalEventBus::with_runtime_options(LocalEventBusRuntimeOptions {
            default_publish_options: HashMap::new(),
            default_subscribe_options: HashMap::new(),
            default_dead_letter_strategies: HashMap::new(),
            global_default_dead_letter_strategy: None,
            global_publisher_interceptors: Vec::new(),
            global_subscriber_interceptors: Vec::new(),
            publisher_interceptors: vec![Arc::new(
                CoverageWrongPublisherInterceptor,
            )],
            subscriber_interceptors: Vec::new(),
            subscription_handler_pool_size: 1,
            subscription_handler_queue_capacity: None,
        });
    wrong_publisher_bus
        .start()
        .expect("coverage bus should start");
    let publisher_error = wrong_publisher_bus
        .publish(&string_topic, "payload".to_string())
        .expect_err("wrong publisher output type should fail");
    errors.push(publisher_error);

    let subscriber = create_subscriber_interceptor_entry::<String, _>(
        coverage_subscriber_passthrough,
    );
    let pass_handler: Arc<HandlerFn<String>> =
        Arc::new(coverage_string_handler);
    let wrapped_handler = subscriber
        .wrap_handler(Box::new(pass_handler))
        .expect("subscriber interceptor should wrap matching handler");
    let wrapped_handler = *wrapped_handler
        .downcast::<Arc<HandlerFn<String>>>()
        .expect("subscriber interceptor should return matching handler");
    wrapped_handler(EventEnvelope::create(
        string_topic.clone(),
        "payload".to_string(),
    ))
    .expect("wrapped handler should proceed");

    let wrong_handler: Arc<HandlerFn<u32>> = Arc::new(coverage_number_handler);
    wrong_handler(EventEnvelope::create(number_topic.clone(), 2_u32))
        .expect("coverage number handler should succeed");
    let subscriber_error = subscriber
        .wrap_handler(Box::new(wrong_handler))
        .expect_err("wrong subscriber handler type should fail");
    errors.push(subscriber_error);
    let failing_handler: Arc<HandlerFn<String>> =
        Arc::new(coverage_failing_string_handler);
    let failing_chain = SubscriberInterceptorChain::with_downstream_error(
        failing_handler,
        create_downstream_error_slot(),
    );
    let downstream_error = failing_chain
        .proceed(EventEnvelope::create(
            string_topic.clone(),
            "downstream-error".to_string(),
        ))
        .expect_err("failing downstream handler should be preserved");
    errors.push(downstream_error);
    let panicking_handler: Arc<HandlerFn<String>> =
        Arc::new(coverage_panicking_string_handler);
    let panicking_chain = SubscriberInterceptorChain::with_downstream_error(
        panicking_handler,
        create_downstream_error_slot(),
    );
    let downstream_panic = panicking_chain
        .proceed(EventEnvelope::create(
            string_topic.clone(),
            "downstream-panic".to_string(),
        ))
        .expect_err("panicking downstream handler should be converted");
    errors.push(downstream_panic);
    let preserved_error = normalize_subscriber_interceptor_error(
        EventBusError::interceptor_failed("subscribe", "coverage preserved"),
    );
    errors.push(preserved_error);

    let direct_interceptor_bus =
        LocalEventBus::with_runtime_options(LocalEventBusRuntimeOptions {
            default_publish_options: HashMap::new(),
            default_subscribe_options: HashMap::new(),
            default_dead_letter_strategies: HashMap::new(),
            global_default_dead_letter_strategy: None,
            global_publisher_interceptors: Vec::new(),
            global_subscriber_interceptors: Vec::new(),
            publisher_interceptors: vec![create_publisher_interceptor_entry::<
                String,
                _,
            >(Some)],
            subscriber_interceptors: vec![
                create_subscriber_interceptor_entry::<String, _>(
                    coverage_subscriber_passthrough,
                ),
            ],
            subscription_handler_pool_size: 1,
            subscription_handler_queue_capacity: None,
        });
    let publisher_output = direct_interceptor_bus
        .apply_publisher_interceptors(EventEnvelope::create(
            string_topic.clone(),
            "payload".to_string(),
        ))
        .expect("matching publisher interceptor should run")
        .expect("matching publisher interceptor should keep the event");
    assert_eq!(publisher_output.payload(), "payload");
    let direct_handler: Arc<HandlerFn<String>> =
        Arc::new(coverage_string_handler);
    let wrapped_handler = direct_interceptor_bus
        .apply_subscriber_interceptors(direct_handler)
        .expect("matching subscriber interceptor should wrap handler");
    wrapped_handler(EventEnvelope::create(
        string_topic.clone(),
        "payload".to_string(),
    ))
    .expect("wrapped handler should run");

    let wrong_subscriber_bus =
        LocalEventBus::with_runtime_options(LocalEventBusRuntimeOptions {
            default_publish_options: HashMap::new(),
            default_subscribe_options: HashMap::new(),
            default_dead_letter_strategies: HashMap::new(),
            global_default_dead_letter_strategy: None,
            global_publisher_interceptors: Vec::new(),
            global_subscriber_interceptors: Vec::new(),
            publisher_interceptors: Vec::new(),
            subscriber_interceptors: vec![Arc::new(
                CoverageWrongSubscriberInterceptor,
            )],
            subscription_handler_pool_size: 1,
            subscription_handler_queue_capacity: None,
        });
    wrong_subscriber_bus
        .start()
        .expect("coverage bus should start");
    let subscriber_error = wrong_subscriber_bus
        .subscribe("sub", &string_topic, coverage_string_handler)
        .err()
        .expect("wrong subscriber output type should fail");
    errors.push(subscriber_error);

    let handler: Arc<HandlerFn<String>> = Arc::new(coverage_string_handler);
    let inactive_entry = TypedSubscriptionEntry {
        id: 1,
        subscriber_id: "inactive".to_string(),
        topic: string_topic.clone(),
        active: inactive_subscription_state(),
        handler: Arc::clone(&handler),
        options: SubscribeOptions::empty(),
    };
    let bus = LocalEventBus::new();
    inactive_entry
        .dispatch(
            Box::new(EventEnvelope::create(
                string_topic.clone(),
                "payload".to_string(),
            )),
            Arc::clone(&bus.inner),
            false,
        )
        .expect("inactive entry should skip dispatch");

    let mismatched_entry = TypedSubscriptionEntry {
        id: 2,
        subscriber_id: "mismatch".to_string(),
        topic: string_topic.clone(),
        active: Arc::new(SubscriptionState::active()),
        handler,
        options: SubscribeOptions::empty(),
    };
    let dispatch_error = mismatched_entry
        .dispatch(
            Box::new(EventEnvelope::create(number_topic, 1_u32)),
            Arc::clone(&bus.inner),
            false,
        )
        .expect_err("wrong dispatch envelope type should fail");
    errors.push(dispatch_error);

    process_subscription_event(
        inactive_subscription_state(),
        Arc::new(coverage_string_handler),
        SubscribeOptions::empty(),
        "inactive-process".to_string(),
        EventEnvelope::create(string_topic, "payload".to_string()),
        bus,
    );

    let zero_timeout_bus =
        LocalEventBus::started().expect("coverage bus should start");
    errors.extend(zero_timeout_bus.shutdown_with_timeout(Duration::ZERO).err());

    let missing_executor_bus =
        LocalEventBus::started().expect("coverage bus should start");
    if let Some(executor) = missing_executor_bus.inner.take_executor() {
        executor.shutdown();
    }
    missing_executor_bus
        .shutdown_with_timeout(Duration::from_millis(20))
        .expect("coverage missing executor shutdown should complete");

    let delay_timeout_bus =
        LocalEventBus::started().expect("coverage bus should start");
    let delay_timeout_topic_key = TopicKey::new(
        "coverage-local-delay-timeout".to_string(),
        TypeId::of::<String>(),
    );
    delay_timeout_bus
        .inner
        .submit_delayed_processing_task(
            ProcessingTask::new(
                Arc::clone(&delay_timeout_bus.inner),
                delay_timeout_topic_key,
                coverage_noop_task,
            ),
            Duration::from_millis(50),
            Arc::new(SubscriptionState::active()),
            false,
        )
        .expect("coverage delayed task should schedule");
    errors.extend(
        delay_timeout_bus
            .shutdown_with_timeout(Duration::from_millis(1))
            .err(),
    );

    let handler_timeout_bus =
        LocalEventBus::started().expect("coverage bus should start");
    handler_timeout_bus
        .inner
        .submit_processing_task(
            || thread::sleep(Duration::from_millis(100)),
            false,
        )
        .expect("coverage raw handler task should schedule");
    errors.extend(
        handler_timeout_bus
            .shutdown_with_timeout(Duration::from_millis(20))
            .err(),
    );

    let dead_letter_bus =
        LocalEventBus::started().expect("coverage bus should start");
    let dead_letter_topic =
        Topic::<DeadLetterRecord>::try_new("coverage-dead-letter-dispatch")
            .expect("coverage topic should build");
    dead_letter_bus
        .subscribe(
            "coverage-dead-letter-sub",
            &dead_letter_topic,
            coverage_dead_letter_record_handler,
        )
        .expect("coverage dead-letter subscriber should register");
    dead_letter_bus
        .publish(
            &dead_letter_topic,
            DeadLetterRecord::new(
                qubit_metadata::Metadata::new(),
                Arc::new("payload".to_string()),
            ),
        )
        .expect("coverage dead-letter event should publish");
    dead_letter_bus
        .wait_for_idle(&dead_letter_topic)
        .expect("coverage dead-letter topic should become idle");
    dead_letter_bus.shutdown();

    errors
}
