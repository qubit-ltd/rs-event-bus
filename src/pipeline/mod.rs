// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared publishing and subscription processing.

mod admission;
mod dead_letter;
mod diagnostic;
mod interceptor;
mod ordering_lane;
mod publisher;
mod retry;
mod subscriber;

pub(crate) use admission::AdmissionPermit;
pub(crate) use admission::AdmissionTracker;
pub(crate) use dead_letter::dead_letter_envelope;
pub use diagnostic::Diagnostic;
pub use diagnostic::DiagnosticObserver;
pub(crate) use diagnostic::PipelineFailure;
#[cfg(test)]
pub(crate) use diagnostic::PipelineFailureOrigin;
pub(crate) use diagnostic::emit_diagnostic;
pub(crate) use interceptor::GlobalPublisherInterceptor;
pub(crate) use ordering_lane::AsyncOrderingGuard;
pub(crate) use ordering_lane::AsyncOrderingLanes;
#[cfg(test)]
pub(crate) use ordering_lane::AsyncOrderingTurn;
pub(crate) use ordering_lane::OrderingLaneKey;
#[cfg(test)]
pub(crate) use ordering_lane::OrderingLanes;
pub(crate) use publisher::PublisherPipeline;
pub(crate) use subscriber::DeliveryFailureAction;
pub(crate) use subscriber::DeliveryOutcome;
pub(crate) use subscriber::SubscriberPipeline;

#[cfg(test)]
#[path = "../../tests/support/publisher_pipeline_tests.rs"]
mod publisher_pipeline_tests;

#[cfg(test)]
#[path = "../../tests/support/subscriber_pipeline_tests.rs"]
mod subscriber_pipeline_tests;
