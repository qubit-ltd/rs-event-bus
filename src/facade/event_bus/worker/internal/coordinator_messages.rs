// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Applies coordinator notifications to receiver-owned delivery state.

use std::collections::HashMap;
use std::sync::mpsc::Receiver;

use super::fail_internal;
use super::settlement_disposition;
use crate::facade::SubscriptionControl;
use crate::facade::event_bus::CoordinatorMessage;
use crate::facade::event_bus::EventBusInner;
use crate::facade::event_bus::internal::OwnedSyncDelivery;

/// Applies all notifications currently queued for this receiver owner.
///
/// # Type Parameters
/// - `'tracking`: lifetime of the delivery lease and tracking guard.
/// - `T`: decoded payload retained by each owned delivery.
///
/// # Parameters
/// - `receiver`: owner channel receiving handler and settlement notifications.
/// - `owned`: lease state updated by each notification.
/// - `inner`: bus context used to report conflicting or stale intents.
/// - `control`: subscription receiving invariant failures.
///
/// # Side Effects
/// Consumes queued messages, records their state transitions, and may stop the
/// subscription when two settlement dispositions conflict.
pub(super) fn drain<'tracking, T>(
    receiver: &Receiver<CoordinatorMessage>,
    owned: &mut HashMap<u64, OwnedSyncDelivery<'tracking, T>>,
    inner: &EventBusInner,
    control: &SubscriptionControl,
) {
    while let Ok(message) = receiver.try_recv() {
        match message {
            CoordinatorMessage::AttemptFinished {
                lease_id,
                outcome,
                directive,
            } => {
                if let Some(delivery) = owned.get_mut(&lease_id) {
                    delivery.result = Some((outcome, directive));
                }
            }
            CoordinatorMessage::Abandoned(lease) => {
                if let Some(delivery) = owned.get_mut(&lease) {
                    delivery.abandoned = true;
                }
            }
            CoordinatorMessage::HandlerFinished(lease) => {
                if let Some(delivery) = owned.get_mut(&lease) {
                    delivery.handler_finished = true;
                    delivery.running = false;
                }
            }
            message @ CoordinatorMessage::Settlement { lease_id, .. } => {
                if let Some(delivery) = owned.get_mut(&lease_id) {
                    if let Some(previous) = delivery.settlement.as_ref() {
                        if settlement_disposition(previous) != settlement_disposition(&message) {
                            fail_internal(inner, control, "conflicting_settlement_intent");
                        }
                    } else {
                        delivery.settlement = Some(message);
                    }
                } else {
                    inner.emit_internal(
                        "settlement_owner_disconnected",
                        "settlement lease is no longer owned".into(),
                    );
                }
            }
        }
    }
}
