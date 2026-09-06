// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for local event bus implementations.

#[cfg(coverage)]
mod coverage_tests;
mod erased_subscription_tests;
mod local_event_bus_factory_tests;
mod local_event_bus_inner_tests;
mod local_event_bus_tests;
#[cfg(coverage)]
mod ordering_lane_key_tests;
#[cfg(coverage)]
mod processing_task_tests;
mod publisher_interceptor_entry_tests;
mod retry_conversion_contract_tests;
mod subscriber_interceptor_chain_tests;
mod subscriber_interceptor_entry_tests;
