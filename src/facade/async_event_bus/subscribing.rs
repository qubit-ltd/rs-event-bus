// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous event bus subscribing operations.

use std::sync::atomic::Ordering;

use qubit_id::Id;

use super::AsyncShutdownDriver;
use crate::AsyncEventBus;
use crate::AsyncSubscription;
use crate::CapabilityError;
use crate::SubscribeError;
use crate::codec::resolve_codec;
use crate::facade::async_event_bus::BusState;
use crate::facade::async_event_bus::catch_spi_future;
use crate::model::SubscribeRequest;
use crate::spi::PayloadModes;
use crate::spi::ShutdownMode;
use crate::spi::SpiSubscriptionRequest;
use crate::spi::TopicAddress;

impl AsyncEventBus {
    /// Creates an asynchronous provider subscription without spawning a task.
    /// Until [`AsyncSubscription::run`] starts, the facade retains ownership
    /// of its provider receiver so [`Self::shutdown`] can close it even if the
    /// returned subscription is never run. Unsupported acknowledgement,
    /// ordering, durability, consumer-group, replay, and codec requirements
    /// return a capability error before provider subscription.
    ///
    /// # Type Parameters
    /// - `T`: Payload type expected from the subscription.
    ///
    /// # Parameters
    /// - `request`: Validated subscription request.
    ///
    /// # Returns
    /// An unstarted subscription whose receiver remains owned by the facade
    /// until its runner takes control.
    ///
    /// # Errors
    /// Returns an error when shutdown has started, options require unsupported
    /// provider capabilities, or provider subscription fails.
    pub async fn subscribe<T: Send + Sync + 'static>(
        &self,
        request: SubscribeRequest<T>,
    ) -> Result<AsyncSubscription<T>, SubscribeError> {
        let _subscribe = self.inner.begin_subscribe().ok_or(SubscribeError::Closed)?;
        let (subscriber_id, topic, options) = request.into_parts();
        if !options.interceptors().is_empty() || self.inner.facade_config.has_sync_subscriber_interceptors::<T>() {
            return Err(SubscribeError::Configuration(
                crate::error::ConfigurationError::InvalidField {
                    field: "sync_subscriber_interceptor",
                    message: "AsyncEventBus requires async subscriber middleware".into(),
                },
            ));
        }
        let capabilities = self.inner.capabilities;
        let codec = resolve_codec(&topic, self.inner.facade_config.codec_registry());
        crate::pipeline::SubscriberPipeline::validate_ack_capability(options.ack_mode(), capabilities.settlement())?;
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
        crate::pipeline::SubscriberPipeline::validate_subscription_capabilities(&options, capabilities)?;
        if options.dead_letter().is_some_and(|policy| {
            policy.admission_policy() == crate::model::DeadLetterAdmissionPolicy::KnownDestination
                && capabilities.publish_visibility() == crate::spi::PublishVisibility::Opaque
        }) {
            return Err(SubscribeError::Capability(CapabilityError::Unsupported {
                capability: "dead_letter.known_destination_admission",
            }));
        }
        let raw_id = self.inner.next_subscription_id.fetch_add(1, Ordering::Relaxed);
        let id = Id::new(raw_id);
        let spi_request = SpiSubscriptionRequest::new(
            id,
            TopicAddress::new(topic.name())?,
            subscriber_id.clone(),
            options.consumer_group().cloned(),
            options.durability(),
            options.start_position().clone(),
            options.provider_options().clone(),
            topic.payload_type_id(),
        );
        let subscribe = crate::spi::panic_boundary::catch_spi_call(
            self.inner.provider_id.as_str(),
            "subscribe",
            Some(subscriber_id.as_str()),
            || self.inner.spi.subscribe(spi_request),
        )?;
        let receiver = catch_spi_future(
            subscribe,
            &self.inner.provider_id,
            "subscribe",
            Some(subscriber_id.as_str()),
        )
        .await?;
        let (subscription, control) = AsyncSubscription::new(
            self.inner.clone(),
            id,
            subscriber_id.clone(),
            topic,
            codec,
            options,
            receiver,
        );
        let admitted = {
            let state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if *state != BusState::Running {
                false
            } else {
                self.inner
                    .controls
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(id, control.clone());
                true
            }
        };
        if !admitted {
            control.stop(ShutdownMode::Immediate);
            let _ = control.shutdown(ShutdownMode::Immediate).await;
            return Err(SubscribeError::Closed);
        }
        Ok(subscription)
    }
}
