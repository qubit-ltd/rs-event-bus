// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Receiver-owned payload and settlement lifetime for one scheduler lease.

use std::sync::Arc;
use std::time::Duration;

use qubit_clock::MonotonicInstant;

use crate::error::CodecError;
use crate::error::SpiError;
use crate::facade::DeliveryTrackerGuard;
use crate::facade::event_bus::CoordinatorMessage;
use crate::facade::internal::SettlementRetryState;
use crate::spi::InboundMessage;

/// Keeps tracking and retry state alive until settlement or provider recovery.
///
/// # Type Parameters
/// - `'tracking`: lifetime of the scheduler lease's delivery tracker guard.
/// - `T`: decoded event payload retained while settlement is pending.
pub(in crate::facade) struct OwnedSyncDelivery<'tracking, T> {
    /// Received payload awaiting a core grant; moved into its handler job once.
    pub(in crate::facade) inbound: Option<(InboundMessage, Result<Arc<T>, CodecError>)>,
    /// First immutable settlement intent; the opaque token never gets cloned.
    pub(in crate::facade) settlement: Option<CoordinatorMessage>,
    /// True only after the job returned or unstarted work was canceled.
    pub(in crate::facade) handler_finished: bool,
    /// Whether the delivery has acquired its lane for settlement.
    pub(in crate::facade) settlement_granted: bool,
    /// Whether cancellation already abandoned this payload without settlement.
    pub(in crate::facade) abandoned: bool,
    /// Infrastructure failure prevents successful lifecycle completion even if
    /// SPI settlement succeeded.
    pub(in crate::facade) lifecycle_failed: bool,
    /// Per-token finite retry budget.
    pub(in crate::facade) retry: SettlementRetryState,
    /// Start sampled in the injected clock's exact domain.
    pub(in crate::facade) first_attempt: Option<MonotonicInstant>,
    /// Next due offset relative to the first attempt.
    pub(in crate::facade) next_attempt: Duration,
    /// Canonical provider error retained for a deadline reached before another
    /// call.
    pub(in crate::facade) last_error: Option<Arc<SpiError>>,
    /// Shutdown tracking outlives payload and token destruction above.
    pub(in crate::facade) _tracker: DeliveryTrackerGuard<'tracking>,
    /// Claimed scheduling credit returns after all delivery resources are
    /// dropped.
    pub(in crate::facade) _lease: super::ReceiveLeaseGuard,
}
