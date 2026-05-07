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
// qubit-style: allow coverage-cfg

pub(crate) mod erased_subscription;
mod local_event_bus;
mod local_event_bus_factory;
pub(crate) mod local_event_bus_inner;
pub(crate) mod ordering_lane_key;
pub(crate) mod processing_task;
pub(crate) mod publisher_interceptor_entry;
mod subscriber_interceptor_chain;
pub(crate) mod subscriber_interceptor_entry;

#[cfg(coverage)]
pub use local_event_bus::coverage_exercise_local_event_bus_defensive_paths;
pub use local_event_bus::{
    IntoPublisherInterceptorResult,
    LocalEventBus,
    PublisherInterceptor,
    SubscriberInterceptor,
};
pub use local_event_bus_factory::LocalEventBusFactory;
#[cfg(coverage)]
pub use local_event_bus_inner::coverage_exercise_local_event_bus_inner_defensive_paths;
pub use subscriber_interceptor_chain::SubscriberInterceptorChain;
