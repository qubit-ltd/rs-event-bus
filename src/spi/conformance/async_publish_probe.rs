// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared inputs for one asynchronous publish conformance probe.

use super::super::EventBusCapabilities;
use super::super::TransportPayload;
use super::internal::ProbeScope;
use crate::model::SubscriptionDurability;

/// Inputs shared by the asynchronous publish and receive conformance check.
pub(super) struct AsyncPublishProbe<'a> {
    /// Stable identifier for the payload probe.
    pub(in crate::spi::conformance) case_id: &'static str,
    /// Payload representation used by the probe.
    pub(in crate::spi::conformance) payload: TransportPayload,
    /// Provider capabilities used to shape the probe.
    pub(in crate::spi::conformance) capabilities: EventBusCapabilities,
    /// Subscription mode used by the probe.
    pub(in crate::spi::conformance) durability: SubscriptionDurability,
    /// Unique identities shared by the request and publish.
    pub(in crate::spi::conformance) scope: &'a ProbeScope,
}
