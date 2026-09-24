// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types

//! Public runtime observations emitted by event-bus facades.

use qubit_id::Id;

use crate::error::EventBusError;
use crate::model::EventId;
use crate::model::SubscriberId;
use crate::spi::DeliveryDisposition;
use crate::spi::DeliveryGap;

/// A runtime fact reported by an event-bus facade.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Diagnostic {
    /// A local destination did not admit a published event.
    AdmissionRejected {
        /// Event associated with the rejected admission.
        event_id: EventId,
        /// Topic associated with the rejected admission.
        topic: Box<str>,
        /// Logical subscriber that was rejected.
        subscriber_id: SubscriberId,
        /// Provider-supplied rejection reason.
        reason: Box<str>,
    },
    /// A provider reported a non-fatal gap while receiving.
    ReceiveGap {
        /// Subscription whose receiver observed the gap.
        subscription_id: Id,
        /// Logical subscriber associated with the receiver.
        subscriber_id: SubscriberId,
        /// Topic being consumed.
        topic: Box<str>,
        /// Provider description of the gap.
        gap: DeliveryGap,
    },
    /// A delivery remained unsuccessful after its configured processing policy.
    DeliveryFailed {
        /// Event associated with the failed delivery.
        event_id: EventId,
        /// Topic associated with the failed delivery.
        topic: Box<str>,
        /// Bus-local subscription object ID.
        subscription_id: Id,
        /// Logical subscriber associated with the failure.
        subscriber_id: SubscriberId,
        /// Number of handler attempts made by the facade.
        attempts: u32,
        /// Human-readable terminal failure detail.
        error: Box<str>,
    },
    /// Provider settlement failed after handler processing reached a terminal
    /// state.
    SettlementFailed {
        /// Event associated with the failed settlement.
        event_id: EventId,
        /// Topic associated with the failed settlement.
        topic: Box<str>,
        /// Bus-local subscription object ID.
        subscription_id: Id,
        /// Logical subscriber associated with the failed settlement.
        subscriber_id: SubscriberId,
        /// Terminal disposition that could not be applied.
        disposition: DeliveryDisposition,
        /// Human-readable provider failure detail.
        error: Box<str>,
    },
    /// A requested terminal disposition was unavailable or its token was
    /// invalid.
    SettlementUnavailable {
        /// Event associated with the unavailable settlement.
        event_id: EventId,
        /// Topic associated with the unavailable settlement.
        topic: Box<str>,
        /// Bus-local subscription object ID.
        subscription_id: Id,
        /// Logical subscriber associated with the unavailable settlement.
        subscriber_id: SubscriberId,
        /// Terminal disposition the facade attempted to request.
        requested: DeliveryDisposition,
    },
    /// A non-fatal internal operation failed outside a more specific category.
    InternalFailure {
        /// Stable source stage, when known.
        origin: Box<str>,
        /// Human-readable detail.
        message: Box<str>,
    },
}

/// Callback used by a facade to deliver runtime diagnostics.
pub type DiagnosticObserver = dyn Fn(&Diagnostic) + Send + Sync + 'static;

/// The stage that produced a publisher pipeline failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum PipelineFailureOrigin {
    /// Typed or global publisher interceptor failed.
    Interceptor,
    /// The selected SPI does not support the requested payload mode.
    Capability,
    /// Encoding the event payload failed.
    Codec,
    /// The selected provider failed during publication.
    Provider,
    /// Retry execution terminated before publication succeeded.
    Retry,
}

/// Failure carrying its origin without inspecting or cloning the error.
#[derive(Debug, thiserror::Error)]
#[error("publisher pipeline failed at {origin:?}: {error}")]
pub(crate) struct PipelineFailure {
    origin: PipelineFailureOrigin,
    #[source]
    error: Box<EventBusError>,
}

impl PipelineFailure {
    /// Creates a failure with explicit publisher pipeline provenance.
    pub(crate) fn new(origin: PipelineFailureOrigin, error: impl Into<EventBusError>) -> Self {
        Self {
            origin,
            error: Box::new(error.into()),
        }
    }

    /// Returns the publisher pipeline failure stage.
    #[cfg(test)]
    pub(crate) fn origin(&self) -> PipelineFailureOrigin {
        self.origin
    }

    /// Returns the borrowed aggregate error for classification.
    #[cfg(test)]
    pub(crate) fn error(&self) -> &EventBusError {
        &self.error
    }

    /// Consumes the wrapper and returns its original operation error.
    pub(crate) fn into_error(self) -> EventBusError {
        *self.error
    }
}

/// Calls each active observer in registration order, containing observer
/// panics.
pub(crate) fn emit_diagnostic(observers: &[std::sync::Arc<DiagnosticObserver>], diagnostic: &Diagnostic) {
    for observer in observers {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(diagnostic)));
    }
}
