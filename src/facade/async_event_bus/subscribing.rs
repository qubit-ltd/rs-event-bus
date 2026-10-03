// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Asynchronous event bus subscribing operations.

use std::sync::PoisonError;
use std::sync::atomic::Ordering;

use qubit_id::Id;

use super::AsyncShutdownDriver;
use crate::AsyncEventBus;
use crate::AsyncSubscription;
use crate::CapabilityError;
use crate::SubscribeError;
use crate::codec::resolve_codec;
use crate::error::ConfigurationError;
use crate::facade::async_event_bus::BusState;
use crate::facade::async_event_bus::catch_spi_future;
use crate::model::DeadLetterAdmissionPolicy;
use crate::model::OrderingPolicy;
use crate::model::SubscribeRequest;
use crate::pipeline::SubscriberPipeline;
use crate::spi::PayloadModes;
use crate::spi::PublishVisibility;
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
    /// provider capabilities, the bus-local subscription ID space is exhausted,
    /// or provider subscription fails.
    pub async fn subscribe<T: Send + Sync + 'static>(
        &self,
        request: SubscribeRequest<T>,
    ) -> Result<AsyncSubscription<T>, SubscribeError> {
        let _subscribe = self.inner.begin_subscribe().ok_or(SubscribeError::Closed)?;
        let (subscriber_id, topic, options) = request.into_parts();
        if !options.interceptors().is_empty() || self.inner.facade_config.has_sync_subscriber_interceptors::<T>() {
            return Err(SubscribeError::Configuration(ConfigurationError::InvalidField {
                field: "sync_subscriber_interceptor",
                message: "AsyncEventBus requires async subscriber middleware".into(),
            }));
        }
        let capabilities = self.inner.capabilities;
        let codec = resolve_codec(&topic, self.inner.facade_config.codec_registry());
        SubscriberPipeline::validate_ack_capability(options.ack_mode(), capabilities.settlement())?;
        if options.ordering_policy() == OrderingPolicy::PerKey && !capabilities.ordering().supports_per_key() {
            return Err(SubscribeError::Capability(CapabilityError::Unsupported {
                capability: "ordering.per_key",
            }));
        }
        if capabilities.payload_modes() == PayloadModes::Encoded && codec.is_none() {
            return Err(SubscribeError::Capability(CapabilityError::CodecRequired));
        }
        SubscriberPipeline::validate_subscription_capabilities(&options, capabilities)?;
        if options.dead_letter().is_some_and(|policy| {
            policy.admission_policy() == DeadLetterAdmissionPolicy::KnownDestination
                && capabilities.publish_visibility() == PublishVisibility::Opaque
        }) {
            return Err(SubscribeError::Capability(CapabilityError::Unsupported {
                capability: "dead_letter.known_destination_admission",
            }));
        }
        let id = self
            .inner
            .next_subscription_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| current.checked_add(1))
            .map(Id::new)
            .map_err(|_| {
                SubscribeError::Configuration(ConfigurationError::InvalidField {
                    field: "subscription_id",
                    message: "bus-local subscription ID space is exhausted".into(),
                })
            })?;
        if !self.inner.scheduler.register(id) {
            return Err(SubscribeError::ResourceLimit {
                resource: "subscriptions",
                limit: self.inner.facade_config.delivery_scheduling().max_subscriptions().get(),
            });
        }
        let mut registration = super::internal::scheduler_registration::SchedulerRegistration {
            inner: self.inner.clone(),
            id,
            committed: false,
        };
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
            let state = self.inner.state.lock().unwrap_or_else(PoisonError::into_inner);
            if *state != BusState::Running {
                false
            } else {
                self.inner
                    .controls
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(id, control.clone());
                true
            }
        };
        if !admitted {
            control.stop(ShutdownMode::Immediate);
            let _ = control.shutdown(ShutdownMode::Immediate).await;
            return Err(SubscribeError::Closed);
        }
        registration.committed = true;
        Ok(subscription)
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::io::Error as IoError;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use qubit_id::Id;

    use crate::AsyncEventBus;
    use crate::SubscribeError;
    use crate::error::ConfigurationError;
    use crate::error::SpiError;
    use crate::model::ProviderId;
    use crate::model::PublishAcknowledgement;
    use crate::model::SubscribeRequest;
    use crate::model::Topic;
    use crate::spi::AsyncEventBusSpi;
    use crate::spi::AsyncEventSubscriptionSpi;
    use crate::spi::DelayedDeliveryCapability;
    use crate::spi::DurabilityCapability;
    use crate::spi::EventBusCapabilities;
    use crate::spi::OrderingCapability;
    use crate::spi::OutboundMessage;
    use crate::spi::PayloadModes;
    use crate::spi::PublishGuarantee;
    use crate::spi::PublishVisibility;
    use crate::spi::ReplayCapability;
    use crate::spi::SettlementCapabilities;
    use crate::spi::ShutdownMode;
    use crate::spi::ShutdownOutcome;
    use crate::spi::SpiFuture;
    use crate::spi::SpiSubscriptionRequest;
    use crate::spi::SubscriptionModes;

    /// Counts provider subscribe calls without creating a receiver.
    struct SubscribeProbe {
        calls: AtomicUsize,
    }

    impl AsyncEventBusSpi for SubscribeProbe {
        fn capabilities(&self) -> EventBusCapabilities {
            EventBusCapabilities::new(
                PayloadModes::Native,
                SettlementCapabilities::None,
                OrderingCapability::None,
                DelayedDeliveryCapability::None,
                DurabilityCapability::Ephemeral,
                SubscriptionModes::EPHEMERAL,
                false,
                ReplayCapability::None,
                PublishGuarantee::Accepted,
                PublishVisibility::Opaque,
            )
        }

        fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
            Box::pin(async { unreachable!("subscribe test does not publish") })
        }

        fn subscribe<'a>(
            &'a self,
            _: SpiSubscriptionRequest,
        ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Box::pin(async {
                Err(SpiError::Operation {
                    provider_id: "overflow-probe".into(),
                    operation: "subscribe",
                    resource: None,
                    kind: "unexpected_call",
                    retryable: Some(false),
                    source: Box::new(IoError::other("SPI must not be called on ID overflow")),
                })
            })
        }

        fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
            Box::pin(async { Ok(ShutdownOutcome::Complete) })
        }
    }

    /// Polls the immediate subscribe result without choosing an async runtime.
    fn drive_ready<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let mut context = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("subscribe probe must finish immediately"),
        }
    }

    #[test]
    fn test_async_subscribe_rejects_exhausted_subscription_ids_before_registration() {
        let spi = Arc::new(SubscribeProbe {
            calls: AtomicUsize::new(0),
        });
        let bus = AsyncEventBus::from_spi(ProviderId::new("overflow-probe").expect("valid provider"), spi.clone())
            .expect("valid provider capabilities");
        bus.inner.next_subscription_id.store(u64::MAX, Ordering::Release);

        let result = drive_ready(
            bus.subscribe(
                SubscribeRequest::new("overflow-subscriber", Topic::<u32>::new("overflow.topic").unwrap())
                    .expect("valid request"),
            ),
        );
        assert!(
            matches!(result, Err(SubscribeError::Configuration(ConfigurationError::InvalidField {
            field: "subscription_id", message
        })) if message.as_ref() == "bus-local subscription ID space is exhausted")
        );
        assert_eq!(spi.calls.load(Ordering::Acquire), 0, "provider SPI must not be called");
        let exhausted_id = Id::new(u64::MAX);
        assert!(
            bus.inner.scheduler.register(exhausted_id),
            "overflow ID must not be registered"
        );
        assert!(
            bus.inner.scheduler.unregister(exhausted_id),
            "probe registration is cleaned up"
        );
    }
}
