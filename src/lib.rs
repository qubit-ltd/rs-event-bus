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

#[cfg(coverage)]
pub use core::coverage_exercise_event_bus_factory_default_regions;
pub use core::{
    AckMode,
    Acknowledgement,
    AttemptTimeoutOption,
    AttemptTimeoutPolicy,
    DeadLetterPayload,
    DeadLetterRecord,
    EventBus,
    EventBusError,
    EventBusFactory,
    EventBusResult,
    EventEnvelope,
    EventEnvelopeBuilder,
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
};
pub use local::{
    LocalEventBus,
    LocalEventBusFactory,
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
