/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! # Qubit Event Bus
//!
//! A lightweight, thread-safe in-process event bus for Rust.
//!
// qubit-style: allow coverage-cfg

#![deny(missing_docs)]

mod core;
mod local;
mod transactional;

pub use core::{
    AckMode,
    Acknowledgement,
    AttemptTimeoutOption,
    AttemptTimeoutPolicy,
    BatchPublishFailure,
    BatchPublishResult,
    DeadLetterPayload,
    DeadLetterRecord,
    DeadLetterStrategy,
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
    RetryDelay,
    RetryJitter,
    RetryOptions,
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
};
pub use transactional::{
    TransactionalEventBus,
    TransactionalPublisher,
    UnsupportedTransactionalEventBus,
    UnsupportedTransactionalPublisher,
};
