// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared publishing and subscription processing.

#[path = "admission_tracker.rs"]
mod admission_tracker;
mod dead_letter;
mod dead_letter_admission;
mod dead_letter_build_error;
mod dead_letter_forward;
mod dead_letter_forward_error;
mod diagnostic;
mod failure_decision;
mod global_publisher_interceptor;
mod ordering_lane;
#[path = "publisher_pipeline.rs"]
mod publisher;
mod retry;
mod retry_terminal;
mod subscriber_pipeline;

#[cfg(test)]
pub(crate) mod subscriber {
    pub(crate) use super::subscriber_pipeline::DeliveryFailureAction;
}

pub(crate) use admission_tracker::AdmissionPermit;
pub(crate) use admission_tracker::AdmissionTracker;
pub(crate) use dead_letter::dead_letter_envelope;
pub(crate) use dead_letter_admission::was_accepted as dead_letter_was_accepted;
pub(crate) use dead_letter_build_error::DeadLetterBuildError;
pub(crate) use dead_letter_forward::retry_config as dead_letter_retry_config;
pub(crate) use dead_letter_forward_error::DeadLetterForwardError;
pub use diagnostic::Diagnostic;
pub use diagnostic::DiagnosticObserver;
pub(crate) use diagnostic::PipelineFailure;
#[cfg(test)]
pub(crate) use diagnostic::PipelineFailureOrigin;
pub(crate) use diagnostic::emit_diagnostic;
pub(crate) use failure_decision::choose_failure_directive;
pub(crate) use global_publisher_interceptor::GlobalPublisherInterceptor;
pub(crate) use ordering_lane::AsyncOrderingGuard;
pub(crate) use ordering_lane::AsyncOrderingLanes;
#[cfg(test)]
pub(crate) use ordering_lane::AsyncOrderingTurn;
pub(crate) use ordering_lane::OrderingLaneKey;
#[cfg(test)]
pub(crate) use ordering_lane::OrderingLanes;
pub(crate) use publisher::PublisherPipeline;
pub(crate) use retry_terminal::is_retry_rule_failure;
pub(crate) use retry_terminal::terminal_directive;
pub(crate) use subscriber_pipeline::DeliveryFailureAction;
pub(crate) use subscriber_pipeline::DeliveryOutcome;
pub(crate) use subscriber_pipeline::SubscriberPipeline;

#[cfg(test)]
#[path = "../../tests/support/publisher_pipeline_tests.rs"]
mod publisher_pipeline_tests;

#[cfg(test)]
#[path = "../../tests/support/subscriber_pipeline_tests.rs"]
mod subscriber_pipeline_tests;
