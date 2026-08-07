// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Qubit Event Bus
//!
//! A lightweight, thread-safe in-process event bus for Rust.
// qubit-style: allow coverage-cfg

#![deny(missing_docs)]

mod core;
mod local;
mod transactional;

pub use core::{
    AckMode,
    Acknowledgement,
    BatchPublishFailure,
    BatchPublishResult,
    DEAD_LETTER_EVENT_ID,
    DEAD_LETTER_FAILED_AT_UNIX_MILLIS,
    DEAD_LETTER_FAILURE_REASON,
    DEAD_LETTER_FAILURE_TYPE,
    DEAD_LETTER_MARKER,
    DEAD_LETTER_ORDERING_KEY,
    DEAD_LETTER_PAYLOAD_TYPE,
    DEAD_LETTER_SUBSCRIBER_ID,
    DEAD_LETTER_TOPIC,
    DeadLetterOriginalPayload,
    DeadLetterPayload,
    DeadLetterRecord,
    DeadLetterStrategy,
    DeadLetterStrategyAny,
    DeadLetterStrategyAnyCallback,
    DeadLetterStrategyCallback,
    EventBus,
    EventBusError,
    EventBusFactory,
    EventBusResult,
    EventEnvelope,
    EventEnvelopeBuilder,
    EventEnvelopeMetadata,
    IntoEventBusResult,
    PublishOptions,
    PublishOptionsBuilder,
    SubscribeOptions,
    SubscribeOptionsBuilder,
    Subscription,
    Topic,
    TopicKey,
    discard_dead_letters,
    prefixed_dead_letters,
    standard_dead_letters_to,
};
#[cfg(coverage)]
pub use core::{
    coverage_exercise_core_defensive_paths,
    coverage_exercise_event_bus_factory_default_regions,
};
pub use local::{
    IntoPublisherInterceptorAnyResult,
    IntoPublisherInterceptorResult,
    LocalEventBus,
    LocalEventBusFactory,
    PublisherInterceptor,
    PublisherInterceptorAny,
    SubscriberInterceptor,
    SubscriberInterceptorAny,
    SubscriberInterceptorAnyChain,
    SubscriberInterceptorChain,
};
#[cfg(coverage)]
pub use local::{
    coverage_exercise_local_event_bus_defensive_paths,
    coverage_exercise_local_event_bus_inner_defensive_paths,
    coverage_exercise_subscriber_interceptor_chain_defensive_paths,
};
pub use transactional::{
    StagedEvent,
    StagedEventEnvelope,
    TransactionalEventBus,
    TransactionalPublisher,
    UnsupportedTransactionalEventBus,
    UnsupportedTransactionalPublisher,
};
