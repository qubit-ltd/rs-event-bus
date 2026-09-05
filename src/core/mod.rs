// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Core event bus contracts, value objects, options, and errors.
// qubit-style: allow coverage-cfg

mod ack_mode;
mod acknowledgement;
#[cfg(coverage)]
mod coverage;
mod dead_letter_record;
mod event_bus;
mod event_bus_error;
mod event_bus_factory;
mod event_bus_retry_rule;
pub(crate) mod event_envelope;
mod event_envelope_builder;
mod into_event_bus_result;
pub(crate) mod publish_options;
mod publish_options_builder;
pub(crate) mod subscribe_options;
mod subscribe_options_builder;
mod subscription;
mod topic;
mod topic_key;

pub use ack_mode::AckMode;
pub use acknowledgement::Acknowledgement;
#[cfg(coverage)]
pub use coverage::coverage_exercise_core_defensive_paths;
pub use dead_letter_record::DEAD_LETTER_EVENT_ID;
pub use dead_letter_record::DEAD_LETTER_FAILED_AT_UNIX_MILLIS;
pub use dead_letter_record::DEAD_LETTER_FAILURE_REASON;
pub use dead_letter_record::DEAD_LETTER_FAILURE_TYPE;
pub use dead_letter_record::DEAD_LETTER_MARKER;
pub use dead_letter_record::DEAD_LETTER_ORDERING_KEY;
pub use dead_letter_record::DEAD_LETTER_PAYLOAD_TYPE;
pub use dead_letter_record::DEAD_LETTER_SUBSCRIBER_ID;
pub use dead_letter_record::DEAD_LETTER_TOPIC;
pub use dead_letter_record::DeadLetterOriginalPayload;
pub use dead_letter_record::DeadLetterPayload;
pub use dead_letter_record::DeadLetterRecord;
pub use event_bus::BatchPublishFailure;
pub use event_bus::BatchPublishResult;
pub use event_bus::EventBus;
pub use event_bus_error::EventBusError;
pub use event_bus_error::EventBusResult;
pub use event_bus_factory::EventBusFactory;
#[cfg(coverage)]
pub use event_bus_factory::coverage_exercise_event_bus_factory_default_regions;
pub use event_bus_retry_rule::EventBusRetryRule;
pub use event_envelope::EventEnvelope;
pub use event_envelope::EventEnvelopeMetadata;
pub use event_envelope_builder::EventEnvelopeBuilder;
pub use into_event_bus_result::IntoEventBusResult;
pub use publish_options::PublishOptions;
pub use publish_options_builder::PublishOptionsBuilder;
pub use subscribe_options::DeadLetterStrategy;
pub use subscribe_options::DeadLetterStrategyAny;
pub use subscribe_options::DeadLetterStrategyAnyCallback;
pub use subscribe_options::DeadLetterStrategyCallback;
pub use subscribe_options::SubscribeOptions;
pub use subscribe_options::discard_dead_letters;
pub use subscribe_options::prefixed_dead_letters;
pub use subscribe_options::standard_dead_letters_to;
pub use subscribe_options_builder::SubscribeOptionsBuilder;
pub use subscription::Subscription;
pub(crate) use subscription::SubscriptionState;
pub use topic::Topic;
pub use topic_key::TopicKey;
