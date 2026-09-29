// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event bus subscribing operations.

use std::thread;

use super::internal::close_spi_subscription;
use crate::CapabilityError;
use crate::ConfigurationError;
use crate::EventBus;
use crate::IntoHandlerResult;
use crate::SpiError;
use crate::SubscribeError;
use crate::SubscriberId;
use crate::Subscription;
use crate::facade::SubscriptionControl;
use crate::facade::event_bus::Arc;
use crate::facade::event_bus::EventBusInner;
use crate::facade::event_bus::Id;
use crate::facade::event_bus::Mutex;
use crate::facade::event_bus::Ordering;
use crate::facade::event_bus::SubscriptionCloseFailure;
use crate::facade::event_bus::resolve_codec;
use crate::facade::event_bus::worker::run_subscription_worker;
use crate::facade::internal::BusContextGuard;
use crate::model::Delivery;
use crate::model::SubscribeRequest;
use crate::pipeline::SubscriberPipeline;
use crate::spi::PayloadModes;
use crate::spi::SpiSubscriptionRequest;
use crate::spi::TopicAddress;

impl EventBus {
    /// Creates a provider subscription and starts its SPI coordinator.
    ///
    /// The handler is invoked outside facade locks. The returned handle does
    /// not cancel the worker when dropped; call `cancel` or shut down the
    /// bus.
    ///
    /// # Errors
    /// Returns `Closed` after shutdown begins, `Capability` for unsupported
    /// acknowledgement, ordering, durability, consumer-group, replay, or codec
    /// requirements, `Configuration` for runtime-model mismatches, or the
    /// provider subscription error.
    pub fn subscribe<T, H, R>(&self, request: SubscribeRequest<T>, handler: H) -> Result<Subscription, SubscribeError>
    where
        T: Send + Sync + 'static,
        H: Fn(Delivery<T>) -> R + Send + Sync + 'static,
        R: IntoHandlerResult + 'static,
    {
        let _operation = self.inner.operations.enter().ok_or(SubscribeError::Closed)?;
        let worker_permit =
            self.inner
                .subscription_worker_budget
                .try_reserve()
                .ok_or(SubscribeError::ResourceLimit {
                    resource: "subscription_workers",
                    limit: self.inner.subscription_worker_budget.limit,
                })?;
        let bus_identity = Arc::as_ptr(&self.inner) as usize;
        let _call_context = BusContextGuard::enter(bus_identity);
        let (subscriber_id, topic, options) = request.into_parts();
        if !options.async_interceptors().is_empty() || self.inner.facade_config.has_async_subscriber_interceptors::<T>()
        {
            return Err(SubscribeError::Configuration(ConfigurationError::InvalidField {
                field: "async_subscriber_interceptor",
                message: "synchronous EventBus requires synchronous subscriber middleware".into(),
            }));
        }
        let capabilities = self.inner.capabilities;
        let codec = resolve_codec(&topic, self.inner.facade_config.codec_registry());
        SubscriberPipeline::validate_ack_capability(options.ack_mode(), capabilities.settlement())?;
        if options.ordering_policy() == crate::model::OrderingPolicy::PerKey
            && !capabilities.ordering().supports_per_key()
        {
            return Err(SubscribeError::Capability(CapabilityError::Unsupported {
                capability: "ordering.per_key",
            }));
        }
        if capabilities.payload_modes() == PayloadModes::Encoded && codec.is_none() {
            return Err(SubscribeError::Capability(CapabilityError::CodecRequired));
        }
        SubscriberPipeline::validate_subscription_capabilities(&options, capabilities)?;
        if options.dead_letter().is_some_and(|policy| {
            policy.admission_policy() == crate::model::DeadLetterAdmissionPolicy::KnownDestination
                && capabilities.publish_visibility() == crate::spi::PublishVisibility::Opaque
        }) {
            return Err(SubscribeError::Capability(CapabilityError::Unsupported {
                capability: "dead_letter.known_destination_admission",
            }));
        }
        let id = self.next_subscription_id()?;
        let address = TopicAddress::new(topic.name())?;
        let spi_request = SpiSubscriptionRequest::new(
            id,
            address,
            subscriber_id.clone(),
            options.consumer_group().cloned(),
            options.durability(),
            options.start_position().clone(),
            options.provider_options().clone(),
            topic.payload_type_id(),
        );
        let spi_subscription = crate::spi::panic_boundary::catch_spi_call(
            self.inner.provider_id.as_str(),
            "subscribe",
            Some(subscriber_id.as_str()),
            || self.inner.spi.subscribe(spi_request),
        )??;
        let spi_subscription_slot = Arc::new(Mutex::new(Some(spi_subscription)));
        if let Err(error) = self.inner.scheduler.start() {
            let spi_subscription = spi_subscription_slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            return Err(cleanup_failed_worker_spawn(
                &self.inner,
                &subscriber_id,
                spi_subscription,
                error,
            ));
        }
        let control = SubscriptionControl::new(id, subscriber_id.clone());
        {
            let _lifecycle = self.lock_lifecycle();
            self.inner.tracker.worker_started();
            self.inner
                .subscriptions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(id, control.clone());
        }
        let inner = self.inner.clone();
        let handler = Arc::new(move |delivery: Delivery<T>| handler(delivery).into_handler_result());
        let thread_control = control.clone();
        let thread_topic = topic.clone();
        let thread_codec = codec.clone();
        let thread_options = options.clone();
        let thread_subscriber_id = subscriber_id.clone();
        let thread_spi_subscription_slot = spi_subscription_slot.clone();
        let worker = thread::Builder::new()
            .name(format!("event-bus-subscription-{}", id.value()))
            .spawn(move || {
                let _worker_permit = worker_permit;
                let spi_subscription = thread_spi_subscription_slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .expect("subscription worker owns an initialized SPI subscription");
                run_subscription_worker(
                    inner,
                    bus_identity,
                    thread_control,
                    spi_subscription,
                    thread_topic,
                    thread_codec,
                    thread_subscriber_id,
                    thread_options,
                    handler,
                );
            });
        match worker {
            Ok(worker) => {
                control.set_worker(worker);
                Ok(Subscription::new(control, bus_identity, self.inner.scheduler.clone()))
            }
            Err(error) => {
                self.inner.tracker.worker_finished();
                self.inner
                    .subscriptions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&id);
                let spi_subscription = spi_subscription_slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                Err(cleanup_failed_worker_spawn(
                    &self.inner,
                    &subscriber_id,
                    spi_subscription,
                    error,
                ))
            }
        }
    }

    /// Allocates one monotonically increasing, bus-local subscription ID.
    fn next_subscription_id(&self) -> Result<Id, SubscribeError> {
        self.inner
            .next_subscription_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| current.checked_add(1))
            .map(Id::new)
            .map_err(|_| {
                SubscribeError::Configuration(ConfigurationError::InvalidField {
                    field: "subscription_id",
                    message: "bus-local subscription ID space is exhausted".into(),
                })
            })
    }
}

/// Closes a provider receiver when its facade worker could not be spawned.
pub(in crate::facade) fn cleanup_failed_worker_spawn(
    inner: &EventBusInner,
    subscriber_id: &SubscriberId,
    spi_subscription: Option<Box<dyn crate::spi::EventSubscriptionSpi>>,
    spawn_error: std::io::Error,
) -> SubscribeError {
    if let Some(mut spi_subscription) = spi_subscription
        && let Err(close_error) = close_spi_subscription(inner, subscriber_id, &mut *spi_subscription)
    {
        inner.emit_internal("subscription_spawn_close", close_error.to_string());
        inner
            .close_errors
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(Arc::new(SubscriptionCloseFailure::new(
                subscriber_id.clone(),
                close_error,
            )));
    }
    SubscribeError::Spi(SpiError::Operation {
        provider_id: inner.provider_id.as_str().into(),
        operation: "spawn_subscription_worker",
        resource: Some(subscriber_id.as_str().into()),
        kind: "worker_spawn_failed",
        retryable: None,
        source: Box::new(spawn_error),
    })
}
