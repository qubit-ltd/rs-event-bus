/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Core event bus contracts, value objects, options, and errors.

mod ack_mode;
mod acknowledgement;
mod event_bus;
mod event_bus_error;
mod event_bus_factory;
pub(crate) mod event_envelope;
mod event_envelope_builder;
mod into_event_bus_result;
pub(crate) mod publish_options;
mod publish_options_builder;
mod retry_options;
pub(crate) mod subscribe_options;
mod subscribe_options_builder;
mod subscription;
mod topic;
mod topic_key;

pub use ack_mode::AckMode;
pub use acknowledgement::Acknowledgement;
pub use event_bus::EventBus;
pub use event_bus_error::{EventBusError, EventBusResult};
pub use event_bus_factory::EventBusFactory;
pub use event_envelope::EventEnvelope;
pub use event_envelope_builder::EventEnvelopeBuilder;
pub use into_event_bus_result::IntoEventBusResult;
pub use publish_options::PublishOptions;
pub use publish_options_builder::PublishOptionsBuilder;
pub use retry_options::RetryOptions;
pub use subscribe_options::{DeadLetterPayload, SubscribeOptions};
pub use subscribe_options_builder::SubscribeOptionsBuilder;
pub use subscription::Subscription;
pub use topic::Topic;
pub use topic_key::TopicKey;
