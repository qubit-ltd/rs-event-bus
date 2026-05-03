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

#![deny(missing_docs)]

mod ack_mode;
mod acknowledgement;
mod erased_subscription;
mod event_bus;
mod event_bus_error;
mod event_bus_factory;
mod event_envelope;
mod event_envelope_builder;
mod into_event_bus_result;
mod local_event_bus;
mod local_event_bus_factory;
mod local_event_bus_inner;
mod publish_options;
mod publish_options_builder;
mod publisher_interceptor_entry;
mod retry_options;
mod subscribe_options;
mod subscribe_options_builder;
mod subscription;
mod topic;
mod topic_key;
mod transactional_event_bus;
mod transactional_publisher;
mod unsupported_transactional_event_bus;
mod unsupported_transactional_publisher;

pub use ack_mode::AckMode;
pub use acknowledgement::Acknowledgement;
pub use event_bus::EventBus;
pub use event_bus_error::{EventBusError, EventBusResult};
pub use event_bus_factory::EventBusFactory;
pub use event_envelope::EventEnvelope;
pub use event_envelope_builder::EventEnvelopeBuilder;
pub use into_event_bus_result::IntoEventBusResult;
pub use local_event_bus::LocalEventBus;
#[doc(hidden)]
pub use local_event_bus::coverage_support;
pub use local_event_bus_factory::LocalEventBusFactory;
pub use publish_options::PublishOptions;
pub use publish_options_builder::PublishOptionsBuilder;
pub use retry_options::RetryOptions;
pub use subscribe_options::SubscribeOptions;
pub use subscribe_options_builder::SubscribeOptionsBuilder;
pub use subscription::Subscription;
pub use topic::Topic;
pub use topic_key::TopicKey;
pub use transactional_event_bus::TransactionalEventBus;
pub use transactional_publisher::TransactionalPublisher;
pub use unsupported_transactional_event_bus::UnsupportedTransactionalEventBus;
pub use unsupported_transactional_publisher::UnsupportedTransactionalPublisher;
