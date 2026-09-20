// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow type-file-name
//! Typed subscription-entry implementation.

use std::any::Any;
use std::any::type_name;
use std::sync::Arc;

use super::super::erased_subscription::DispatchAdmission;
use super::super::erased_subscription::ErasedSubscription;
use super::super::ordering_lane_key::OrderingLaneKey;
use super::super::processing_task::DeliveryContext;
use super::super::processing_task::ProcessingTask;
use super::HandlerFn;
use super::LocalEventBus;
use super::LocalEventBusInner;
use super::SubscriptionWorkerContext;
use super::local_event_bus_id;
use super::process_subscription_event;
use crate::EventBusError;
use crate::EventBusResult;
use crate::EventEnvelope;
use crate::SubscribeOptions;
use crate::Topic;
use crate::core::SubscriptionState;

/// Typed subscription entry stored in the subscription map.
pub(super) struct TypedSubscriptionEntry<T: Clone + Send + Sync + 'static> {
    pub(super) id: usize,
    pub(super) subscriber_id: String,
    pub(super) topic: Topic<T>,
    pub(super) active: Arc<SubscriptionState>,
    pub(super) handler: Arc<HandlerFn<T>>,
    pub(super) options: SubscribeOptions<T>,
}

impl<T> ErasedSubscription for TypedSubscriptionEntry<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn id(&self) -> usize {
        self.id
    }

    fn subscriber_id(&self) -> &str {
        &self.subscriber_id
    }

    fn priority(&self) -> i32 {
        self.options.priority()
    }

    fn deactivate(&self) {
        self.active.deactivate();
    }

    fn dispatch(
        &self,
        envelope: Box<dyn Any + Send>,
        bus: Arc<LocalEventBusInner>,
        allow_stopping: bool,
    ) -> EventBusResult<DispatchAdmission> {
        if !self.active.is_active() {
            return Ok(DispatchAdmission::Filtered);
        }
        let envelope = envelope
            .downcast::<EventEnvelope<T>>()
            .map_err(|_| EventBusError::type_mismatch(type_name::<EventEnvelope<T>>(), "unknown"))?;
        if !self.options.try_should_handle(&envelope)? {
            return Ok(DispatchAdmission::Filtered);
        }
        let topic_key = self.topic.key();
        let delivery_permit = bus.try_acquire_delivery_permit()?;
        bus.start_processing(&topic_key)?;
        let ordering_lane_key = envelope
            .ordering_key()
            .map(|ordering_key| OrderingLaneKey::new(topic_key.clone(), ordering_key, self.id));
        let delay = envelope.delay();
        let active = Arc::clone(&self.active);
        let delayed_active = Arc::clone(&self.active);
        let handler = Arc::clone(&self.handler);
        let options = self.options.clone();
        let subscriber_id = self.subscriber_id.clone();
        let subscription_id = self.id;
        let event_bus = LocalEventBus {
            inner: Arc::clone(&bus),
        };
        let bus_id = local_event_bus_id(&bus);
        let delivery_context = DeliveryContext {
            event_id: envelope.id().to_string(),
            topic_name: self.topic.name().to_string(),
            subscriber_id: self.subscriber_id.clone(),
        };
        let processing_task = ProcessingTask::with_delivery_context_and_permit(
            Arc::clone(&bus),
            topic_key,
            delivery_context,
            delivery_permit,
            move || {
                let _worker_context = SubscriptionWorkerContext::enter(bus_id);
                if !active.is_active() {
                    return;
                }
                process_subscription_event(
                    active,
                    handler,
                    options,
                    subscription_id,
                    subscriber_id,
                    *envelope,
                    event_bus,
                );
            },
        );
        let result = if let Some(ordering_lane_key) = ordering_lane_key {
            if let Some(delay) = delay
                && !delay.is_zero()
            {
                bus.submit_delayed_ordered_processing_task(
                    ordering_lane_key,
                    processing_task,
                    delay,
                    delayed_active,
                    allow_stopping,
                )
            } else {
                bus.submit_ordered_processing_task(ordering_lane_key, processing_task, allow_stopping)
            }
        } else if let Some(delay) = delay
            && !delay.is_zero()
        {
            bus.submit_delayed_processing_task(processing_task, delay, delayed_active, allow_stopping)
        } else {
            bus.submit_processing_task(move || processing_task.run(), allow_stopping)
        };
        result.map(|_| DispatchAdmission::Accepted)
    }
}
