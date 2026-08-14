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

pub use core::AckMode;
pub use core::Acknowledgement;
pub use core::BatchPublishFailure;
pub use core::BatchPublishResult;
pub use core::DEAD_LETTER_EVENT_ID;
pub use core::DEAD_LETTER_FAILED_AT_UNIX_MILLIS;
pub use core::DEAD_LETTER_FAILURE_REASON;
pub use core::DEAD_LETTER_FAILURE_TYPE;
pub use core::DEAD_LETTER_MARKER;
pub use core::DEAD_LETTER_ORDERING_KEY;
pub use core::DEAD_LETTER_PAYLOAD_TYPE;
pub use core::DEAD_LETTER_SUBSCRIBER_ID;
pub use core::DEAD_LETTER_TOPIC;
pub use core::DeadLetterOriginalPayload;
pub use core::DeadLetterPayload;
pub use core::DeadLetterRecord;
pub use core::DeadLetterStrategy;
pub use core::DeadLetterStrategyAny;
pub use core::DeadLetterStrategyAnyCallback;
pub use core::DeadLetterStrategyCallback;
pub use core::EventBus;
pub use core::EventBusError;
pub use core::EventBusFactory;
pub use core::EventBusResult;
pub use core::EventEnvelope;
pub use core::EventEnvelopeBuilder;
pub use core::EventEnvelopeMetadata;
pub use core::IntoEventBusResult;
pub use core::PublishOptions;
pub use core::PublishOptionsBuilder;
pub use core::SubscribeOptions;
pub use core::SubscribeOptionsBuilder;
pub use core::Subscription;
pub use core::Topic;
pub use core::TopicKey;
#[cfg(coverage)]
pub use core::coverage_exercise_core_defensive_paths;
#[cfg(coverage)]
pub use core::coverage_exercise_event_bus_factory_default_regions;
pub use core::discard_dead_letters;
pub use core::prefixed_dead_letters;
pub use core::standard_dead_letters_to;

pub use local::IntoPublisherInterceptorAnyResult;
pub use local::IntoPublisherInterceptorResult;
pub use local::LocalEventBus;
pub use local::LocalEventBusFactory;
pub use local::PublisherInterceptor;
pub use local::PublisherInterceptorAny;
pub use local::SubscriberInterceptor;
pub use local::SubscriberInterceptorAny;
pub use local::SubscriberInterceptorAnyChain;
pub use local::SubscriberInterceptorChain;
#[cfg(coverage)]
pub use local::coverage_exercise_local_event_bus_defensive_paths;
#[cfg(coverage)]
pub use local::coverage_exercise_local_event_bus_inner_defensive_paths;
#[cfg(coverage)]
pub use local::coverage_exercise_subscriber_interceptor_chain_defensive_paths;
pub use transactional::StagedEvent;
pub use transactional::StagedEventEnvelope;
pub use transactional::TransactionalEventBus;
pub use transactional::TransactionalPublisher;
pub use transactional::UnsupportedTransactionalEventBus;
pub use transactional::UnsupportedTransactionalPublisher;
