/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Qubit Event Bus
//!
//! A lightweight, thread-safe in-process event bus for Rust.
//!
//! # Author
//!
//! Haixing Hu

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
pub use event_bus_error::EventBusError;
pub use event_bus_factory::EventBusFactory;
pub use event_envelope::EventEnvelope;
pub use event_envelope_builder::EventEnvelopeBuilder;
pub use into_event_bus_result::IntoEventBusResult;
pub use local_event_bus::LocalEventBus;
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

/// Result type used by event bus operations.
pub type EventBusResult<T> = Result<T, EventBusError>;

/// Coverage-only helpers for defensive internal paths.
#[cfg(coverage)]
pub mod coverage_support {
    /// Exercises defensive local event bus branches that are hard to reach
    /// through safe public APIs.
    ///
    /// # Returns
    /// Diagnostic strings collected from covered branches.
    pub fn exercise_local_event_bus_paths() -> Vec<String> {
        let mut diagnostics = crate::local_event_bus::coverage_exercise_local_event_bus_paths();
        diagnostics.extend(crate::local_event_bus_inner::coverage_exercise_inner_poison_paths());
        diagnostics
    }
}
