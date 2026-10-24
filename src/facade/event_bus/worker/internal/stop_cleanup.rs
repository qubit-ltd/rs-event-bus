// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Converts receiver-owned work into the subscription's stop-time actions.

use std::collections::HashMap;

use super::canceled_intent;
use super::count_abandoned;
use crate::SubscriberId;
use crate::facade::SubscriptionControl;
use crate::facade::event_bus::CoordinatorMessage;
use crate::facade::event_bus::EventBusInner;
use crate::facade::event_bus::internal::OwnedSyncDelivery;
use crate::model::Topic;
use crate::spi::DeliveryDisposition;

/// Replaces queued handler work with best-effort retry intents when stopping.
///
/// Running handlers and already-created settlement intents remain untouched so
/// the owner can observe their completion in its normal iteration order.
///
/// # Type Parameters
/// - `'tracking`: lifetime of delivery tracker guards stored in `owned`.
/// - `T`: payload type retained by each owned delivery.
///
/// # Parameters
/// - `inner`: provider capabilities and abandonment accounting context.
/// - `control`: subscription identity and terminal failure state.
/// - `subscriber_id`: logical identity included in settlement intents.
/// - `topic`: destination used when constructing retry intents.
/// - `owned`: receiver-owned deliveries whose unstarted work is stopped.
pub(super) fn prepare_stopping_deliveries<'tracking, T: 'static>(
    inner: &EventBusInner,
    control: &SubscriptionControl,
    subscriber_id: &SubscriberId,
    topic: &Topic<T>,
    owned: &mut HashMap<u64, OwnedSyncDelivery<'tracking, T>>,
) {
    for (&lease, delivery) in owned {
        if let Some((message, _)) = delivery.inbound.take() {
            delivery.handler_finished = true;
            if control.terminal_failure().is_none() {
                delivery.settlement = canceled_intent(inner, lease, control.id, subscriber_id, message);
                delivery.abandoned = delivery.settlement.is_none();
            } else {
                count_abandoned(inner, control.id);
            }
        } else if !delivery.running
            && !delivery.abandoned
            && delivery.result.is_none()
            && delivery.settlement.is_none()
            && let Some(prepared) = delivery.delivery.as_ref()
        {
            delivery.due = None;
            delivery.handler_finished = true;
            delivery.retry_pending = false;
            let event = prepared.event_arc();
            delivery.settlement = if delivery.token.is_some()
                && inner.capabilities.settlement() == crate::spi::SettlementCapabilities::AcceptRetryReject
            {
                Some(CoordinatorMessage::Settlement {
                    lease_id: lease,
                    token: delivery.token.take(),
                    disposition: DeliveryDisposition::Retry,
                    event_id: event.id().clone(),
                    topic: topic.name().into(),
                    subscription_id: control.id,
                    subscriber_id: subscriber_id.clone(),
                })
            } else {
                count_abandoned(inner, control.id);
                delivery.abandoned = true;
                None
            };
            delivery.result = None;
        }
    }
}
