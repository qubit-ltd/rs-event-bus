// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Single-owner state retained from receive through delivery settlement.

use std::sync::Arc;

use super::super::super::async_admission::AsyncAdmissionPermit;
use crate::EventId;
use crate::error::DeliveryError;
use crate::facade::async_event_bus::AsyncDeliveryGuard;
use crate::model::EventEnvelope;
use crate::model::ProviderMessageMetadata;
use crate::pipeline::AsyncOrderingGuard;
use crate::spi::DeliveryDisposition;
use crate::spi::SettlementToken;

/// Retains the message, provider token, admission and tracking owners as it
/// moves between runner stages.
pub(in crate::facade) struct PendingDelivery<T: 'static> {
    /// Tracks the message from receive through settlement or explicit
    /// abandonment.
    pub(in crate::facade) _tracking: AsyncDeliveryGuard,
    /// Stable event identity retained even if decoding fails.
    pub(in crate::facade) event_id: EventId,
    /// Decoded event retained while handler work and settlement are pending.
    pub(in crate::facade) event: Option<Arc<EventEnvelope<T>>>,
    /// Provider-owned token required for any later settlement attempt.
    pub(in crate::facade) token: Option<SettlementToken>,
    /// Metadata observed when the provider delivered the event.
    pub(in crate::facade) metadata: ProviderMessageMetadata,
    /// Decode failure converted into a settlement outcome without handler work.
    pub(in crate::facade) decode_error: Option<DeliveryError>,
    /// Immutable settlement choice retained across provider settlement retries.
    pub(in crate::facade) settlement_intent: Option<DeliveryDisposition>,
    /// Number of attempts to settle this exact token and disposition.
    pub(in crate::facade) settlement_failures: u32,
    /// Last failure to report once settlement reaches a terminal state.
    pub(in crate::facade) failure_diagnostic: Option<(u32, Box<str>)>,
    /// Bus-wide capacity held until this delivery reaches a terminal state.
    pub(in crate::facade) admission: Option<AsyncAdmissionPermit>,
    /// Per-key order held until handler and settlement finish.
    pub(in crate::facade) lane: Option<AsyncOrderingGuard<()>>,
}
