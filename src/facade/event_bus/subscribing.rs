// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Event bus subscribing operations.

use std::io::Error;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::Ordering;
use std::thread;

use qubit_id::Id;

use super::internal::close_spi_subscription;
use crate::CapabilityError;
use crate::ConfigurationError;
use crate::DeliveryError;
use crate::EventBus;
use crate::IntoHandlerResult;
use crate::SpiError;
use crate::SubscribeError;
use crate::SubscriberId;
use crate::Subscription;
use crate::codec::EventCodec;
use crate::codec::resolve_codec;
use crate::error::SubscriptionCloseFailure;
use crate::facade::SubscriptionControl;
use crate::facade::event_bus::EventBusInner;
use crate::facade::event_bus::internal::HandlerStartRejected;
use crate::facade::event_bus::worker::run_subscription_worker;
use crate::facade::internal::BusContextGuard;
use crate::facade::internal::DeliveryMetrics;
use crate::model::DeadLetterAdmissionPolicy;
use crate::model::Delivery;
use crate::model::OrderingPolicy;
use crate::model::SubscribeOptions;
use crate::model::SubscribeRequest;
use crate::model::Topic;
use crate::pipeline::SubscriberPipeline;
use crate::spi::EventSubscriptionSpi;
use crate::spi::PayloadModes;
use crate::spi::PublishVisibility;
use crate::spi::SpiSubscriptionRequest;
use crate::spi::TopicAddress;
use crate::spi::panic_boundary::catch_spi_call;

impl EventBus {
    /// Creates a provider subscription and starts its SPI coordinator.
    ///
    /// The handler is invoked outside facade locks. The returned handle does
    /// not cancel the worker when dropped; call `cancel` or shut down the
    /// bus.
    ///
    /// # Type Parameters
    /// - `T`: payload type received by the handler.
    /// - `H`: synchronous handler callback type.
    /// - `R`: handler return type accepted by [`IntoHandlerResult`].
    ///
    /// # Parameters
    /// - `request`: validated subscriber, topic, and processing options.
    /// - `handler`: callback invoked for each accepted delivery.
    ///
    /// # Returns
    /// A handle that controls the started provider subscription.
    ///
    /// # Errors
    /// Returns `Closed` after shutdown begins, `Capability` for unsupported
    /// acknowledgement, ordering, durability, consumer-group, replay, or codec
    /// requirements, `Configuration` for runtime-model mismatches, or the
    /// provider subscription error.
    ///
    /// # Panics
    /// Panics if the SPI subscription request is incomplete or the worker
    /// cannot take ownership of its initialized provider receiver.
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
                    resource: "subscriptions",
                    limit: self.inner.subscription_worker_budget.limit,
                })?;
        let bus_identity = Arc::as_ptr(&self.inner) as usize;
        let _call_context = BusContextGuard::enter(bus_identity);
        let (subscriber_id, topic, options) = request.into_parts();
        let codec = self.validate_subscription(&topic, &options)?;
        let id = self.next_subscription_id()?;
        let spi_subscription = self.create_spi_subscription(id, &subscriber_id, &topic, &options)?;
        let spi_subscription_slot = Arc::new(Mutex::new(Some(spi_subscription)));
        let control = self.register_subscription(id, &subscriber_id, &spi_subscription_slot)?;
        let inner = self.inner.clone();
        let handler = wrap_subscription_handler(&self.inner, control.clone(), handler);
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
                    .unwrap_or_else(PoisonError::into_inner)
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
                Ok(Subscription::new(
                    control,
                    bus_identity,
                    self.inner.scheduler.clone(),
                    Arc::downgrade(&self.inner),
                ))
            }
            Err(error) => {
                self.inner.scheduler.finish_subscription(id);
                self.inner.tracker.worker_finished();
                self.inner
                    .subscriptions
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .remove(&id);
                let spi_subscription = spi_subscription_slot
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
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

    /// Validates facade-specific subscription options and resolves its codec.
    ///
    /// # Type Parameters
    /// - `T`: payload type received by the subscription.
    ///
    /// # Parameters
    /// - `topic`: typed topic whose codec may be selected.
    /// - `options`: middleware and delivery policies to validate.
    ///
    /// # Returns
    /// The selected codec, or `None` when native payloads need no codec.
    ///
    /// # Errors
    /// Returns a configuration or capability error when the synchronous
    /// facade cannot honor the request.
    fn validate_subscription<T: Send + Sync + 'static>(
        &self,
        topic: &Topic<T>,
        options: &SubscribeOptions<T>,
    ) -> Result<Option<Arc<dyn EventCodec<T>>>, SubscribeError> {
        if !options.async_interceptors().is_empty() || self.inner.facade_config.has_async_subscriber_interceptors::<T>()
        {
            return Err(SubscribeError::Configuration(ConfigurationError::InvalidField {
                field: "async_subscriber_interceptor",
                message: "synchronous EventBus requires synchronous subscriber middleware".into(),
            }));
        }
        let capabilities = self.inner.capabilities;
        let codec = resolve_codec(topic, self.inner.facade_config.codec_registry());
        SubscriberPipeline::validate_ack_capability(options.ack_mode(), capabilities.settlement())?;
        if options.ordering_policy() == OrderingPolicy::PerKey && !capabilities.ordering().supports_per_key() {
            return Err(SubscribeError::Capability(CapabilityError::Unsupported {
                capability: "ordering.per_key",
            }));
        }
        if capabilities.payload_modes() == PayloadModes::Encoded && codec.is_none() {
            return Err(SubscribeError::Capability(CapabilityError::CodecRequired));
        }
        SubscriberPipeline::validate_subscription_capabilities(options, capabilities)?;
        if options.dead_letter().is_some_and(|policy| {
            policy.admission_policy() == DeadLetterAdmissionPolicy::KnownDestination
                && capabilities.publish_visibility() == PublishVisibility::Opaque
        }) {
            return Err(SubscribeError::Capability(CapabilityError::Unsupported {
                capability: "dead_letter.known_destination_admission",
            }));
        }
        Ok(codec)
    }

    /// Creates the provider subscription after facade validation succeeds.
    ///
    /// # Type Parameters
    /// - `T`: payload type declared by the typed topic.
    ///
    /// # Parameters
    /// - `id`: bus-local ID allocated for this subscription.
    /// - `subscriber_id`: logical subscriber identity.
    /// - `topic`: typed topic used to construct the provider address.
    /// - `options`: provider-visible subscription settings.
    ///
    /// # Returns
    /// The initialized provider receiver.
    ///
    /// # Errors
    /// Returns topic-address validation or provider subscription errors.
    fn create_spi_subscription<T: Send + Sync + 'static>(
        &self,
        id: Id,
        subscriber_id: &SubscriberId,
        topic: &Topic<T>,
        options: &SubscribeOptions<T>,
    ) -> Result<Box<dyn EventSubscriptionSpi>, SubscribeError> {
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
        catch_spi_call(
            self.inner.provider_id.as_str(),
            "subscribe",
            Some(subscriber_id.as_str()),
            || self.inner.spi.subscribe(spi_request),
        )?
        .map_err(Into::into)
    }

    /// Starts and registers the subscription lifecycle before spawning its
    /// worker.
    ///
    /// # Parameters
    /// - `id`: bus-local subscription ID.
    /// - `subscriber_id`: logical subscriber identity for cleanup errors.
    /// - `spi_subscription_slot`: receiver slot retained for spawn rollback.
    ///
    /// # Returns
    /// The control shared by the facade handle and its coordinator.
    ///
    /// # Errors
    /// Returns an error after cleaning up the receiver when scheduler startup
    /// or registration fails.
    fn register_subscription(
        &self,
        id: Id,
        subscriber_id: &SubscriberId,
        spi_subscription_slot: &Arc<Mutex<Option<Box<dyn EventSubscriptionSpi>>>>,
    ) -> Result<Arc<SubscriptionControl>, SubscribeError> {
        if let Err(error) = self.inner.scheduler.start() {
            let spi_subscription = spi_subscription_slot
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            return Err(cleanup_failed_worker_spawn(
                &self.inner,
                subscriber_id,
                spi_subscription,
                error,
            ));
        }
        let control = SubscriptionControl::with_metrics(
            id,
            subscriber_id.clone(),
            Arc::new(DeliveryMetrics::new_subscription(self.inner.delivery_metrics.clone())),
        );
        if !self.inner.scheduler.register(id) {
            let receiver = spi_subscription_slot
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            return Err(cleanup_failed_worker_spawn(
                &self.inner,
                subscriber_id,
                receiver,
                Error::other("subscription scheduler registration invariant violated"),
            ));
        }
        {
            let _lifecycle = self.lock_lifecycle();
            self.inner.tracker.worker_started();
            self.inner
                .subscriptions
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(id, control.clone());
        }
        Ok(control)
    }

    /// Allocates one monotonically increasing, bus-local subscription ID.
    ///
    /// # Returns
    /// The next available subscription ID.
    ///
    /// # Errors
    /// Returns a configuration error when the ID space is exhausted.
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

/// Applies lifecycle checks and panic normalization around the user callback.
///
/// # Type Parameters
/// - `T`: payload delivered to the callback.
/// - `H`: user callback type.
/// - `R`: callback result accepted by [`IntoHandlerResult`].
///
/// # Parameters
/// - `inner`: shared bus state retained weakly by the wrapper.
/// - `control`: subscription state used to gate and measure handler execution.
/// - `handler`: user callback to invoke after the scheduler grants a delivery.
///
/// # Returns
/// A shared callback that rejects cancelled starts, records duration, and
/// resumes user panics after recording them.
fn wrap_subscription_handler<T, H, R>(
    inner: &Arc<EventBusInner>,
    control: Arc<SubscriptionControl>,
    handler: H,
) -> Arc<dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync>
where
    T: Send + Sync + 'static,
    H: Fn(Delivery<T>) -> R + Send + Sync + 'static,
    R: IntoHandlerResult + 'static,
{
    let handler_inner = Arc::downgrade(inner);
    Arc::new(move |delivery: Delivery<T>| {
        let inner = handler_inner
            .upgrade()
            .expect("receiver owner retains bus during handler execution");
        let started = inner.clock.now();
        if !control.try_start_handler(|cancelled| inner.scheduler.handler_may_start(control.id, cancelled)) {
            return Err(DeliveryError::Handler {
                source: Box::new(HandlerStartRejected),
            });
        }
        let result = catch_unwind(AssertUnwindSafe(|| handler(delivery).into_handler_result()));
        if let Err(error) = control
            .delivery_metrics
            .record_handler_duration(started, inner.clock.now())
        {
            inner.fail_clock(&control, "handler_clock", error);
        }
        match result {
            Ok(result) => result,
            Err(panic) => resume_unwind(panic),
        }
    })
}

/// Closes a provider receiver when its facade worker could not be spawned.
///
/// # Parameters
/// - `inner`: bus state used to record cleanup failures.
/// - `subscriber_id`: logical identity associated with the receiver.
/// - `spi_subscription`: provider receiver to close, when one was created.
/// - `spawn_error`: worker thread creation failure.
///
/// # Returns
/// A public subscription error containing the worker spawn failure and any
/// retained close failure.
pub(in crate::facade) fn cleanup_failed_worker_spawn(
    inner: &EventBusInner,
    subscriber_id: &SubscriberId,
    spi_subscription: Option<Box<dyn EventSubscriptionSpi>>,
    spawn_error: Error,
) -> SubscribeError {
    if let Some(mut spi_subscription) = spi_subscription
        && let Err(close_error) = close_spi_subscription(inner, subscriber_id, &mut *spi_subscription)
    {
        inner.emit_internal("subscription_spawn_close", close_error.to_string());
        inner
            .close_errors
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
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
