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
    AckMode, Acknowledgement, EventBus, EventBusError, EventBusFactory, EventBusResult,
    EventEnvelope, EventEnvelopeBuilder, IntoEventBusResult, PublishOptions, PublishOptionsBuilder,
    RetryOptions, SubscribeOptions, SubscribeOptionsBuilder, Subscription, Topic, TopicKey,
};
#[doc(hidden)]
pub use local::coverage_support;
pub use local::{LocalEventBus, LocalEventBusFactory};
pub use transactional::{
    TransactionalEventBus, TransactionalPublisher, UnsupportedTransactionalEventBus,
    UnsupportedTransactionalPublisher,
};
