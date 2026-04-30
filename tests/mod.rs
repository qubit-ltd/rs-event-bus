/*******************************************************************************
 *
 *    Copyright (c) 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! Integration tests for `qubit-event-bus`.

#[path = "core/topic_tests.rs"]
mod topic_tests;

#[path = "core/event_envelope_tests.rs"]
mod event_envelope_tests;

#[path = "core/error_tests.rs"]
mod error_tests;

#[path = "core/options_tests.rs"]
mod options_tests;

#[path = "local/local_event_bus_tests.rs"]
mod local_event_bus_tests;

#[cfg(coverage)]
#[path = "coverage_support/coverage_support_tests.rs"]
mod coverage_support_tests;
