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

mod core;
mod local;
mod transactional;

pub use core::{
    AckMode, Acknowledgement, DeadLetterPayload, EventBus, EventBusError, EventBusFactory,
    EventBusResult, EventEnvelope, EventEnvelopeBuilder, IntoEventBusResult, PublishOptions,
    PublishOptionsBuilder, RetryOptions, SubscribeOptions, SubscribeOptionsBuilder, Subscription,
    Topic, TopicKey,
};
pub use local::{LocalEventBus, LocalEventBusFactory, SubscriberInterceptorChain};
pub use transactional::{
    TransactionalEventBus, TransactionalPublisher, UnsupportedTransactionalEventBus,
    UnsupportedTransactionalPublisher,
};
