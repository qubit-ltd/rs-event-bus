/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! In-process event bus implementation and local runtime internals.

pub(crate) mod erased_subscription;
mod local_event_bus;
mod local_event_bus_factory;
pub(crate) mod local_event_bus_inner;
pub(crate) mod publisher_interceptor_entry;
mod subscriber_interceptor_chain;
pub(crate) mod subscriber_interceptor_entry;

pub use local_event_bus::LocalEventBus;
pub use local_event_bus_factory::LocalEventBusFactory;
pub use subscriber_interceptor_chain::SubscriberInterceptorChain;
