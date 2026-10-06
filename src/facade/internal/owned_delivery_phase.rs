// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Ownership phases independent of handler capacity.

/// Current use of one owned delivery credit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OwnedDeliveryPhase {
    /// Credit reserved before entering the provider receive operation.
    ReservedReceive,
    /// Received delivery waiting for handler admission.
    Queued,
    /// Received delivery waiting for a lane grant to settle without running a
    /// handler.
    QueuedSettlement,
    /// Handler execution holding one running slot.
    Running,
    /// Retry backoff retains the owned credit and ordering lane without H.
    WaitingRetry,
    /// Provider settlement owns the credit and any lane retained from handler
    /// execution.
    Settling,
}
