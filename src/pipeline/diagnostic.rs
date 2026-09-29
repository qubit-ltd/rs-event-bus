// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public runtime observations emitted by event-bus facades.

mod internal;

pub(crate) use internal::PipelineFailure;
pub(crate) use internal::PipelineFailureOrigin;
use qubit_id::Id;

use crate::model::EventId;
use crate::model::SubscriberId;
use crate::spi::DeliveryDisposition;
use crate::spi::DeliveryGap;

/// A runtime fact reported by an event-bus facade.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::model::EventId;
/// use qubit_event_bus::model::SubscriberId;
/// use qubit_event_bus::pipeline::Diagnostic;
///
/// let diagnostic = Diagnostic::AdmissionRejected {
///     event_id: EventId::new("event-1").unwrap(),
///     topic: "orders.created".into(),
///     subscriber_id: SubscriberId::new("audit-log").unwrap(),
///     reason: "queue is full".into(),
/// };
/// assert!(matches!(diagnostic, Diagnostic::AdmissionRejected { .. }));
/// ```
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

/// Calls each active observer in registration order, containing observer
/// panics.
///
/// # Parameters
/// - `observers`: observers to invoke in their registration order.
/// - `diagnostic`: runtime fact passed to each observer.
pub(crate) fn emit_diagnostic(observers: &[std::sync::Arc<DiagnosticObserver>], diagnostic: &Diagnostic) {
    for observer in observers {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| observer(diagnostic)));
    }
}
