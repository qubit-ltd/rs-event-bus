// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
//! Publisher and subscriber interceptor implementations.

use std::any::Any;
use std::any::TypeId;
use std::any::type_name;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use super::super::subscriber_interceptor_chain::SubscriberInterceptorAnyChain;
use super::super::subscriber_interceptor_chain::SubscriberInterceptorChain;
use super::super::subscriber_interceptor_chain::create_downstream_error_slot;
use super::super::subscriber_interceptor_entry::SubscriberInterceptorEntry;
use super::HandlerFn;
use super::LocalEventBus;
use super::normalize_subscriber_interceptor_result;
use crate::EventBusError;
use crate::EventBusResult;
use crate::EventEnvelope;
use crate::EventEnvelopeMetadata;
use crate::IntoEventBusResult;
use crate::local::publisher_interceptor_entry::PublisherInterceptorEntry;

/// Converts typed publisher interceptor return values into a common result.
pub trait IntoPublisherInterceptorResult<T: Clone + Send + Sync + 'static> {
    /// Converts this value into an optional event envelope result.
    fn into_publisher_interceptor_result(self) -> EventBusResult<Option<EventEnvelope<T>>>;
}

impl<T> IntoPublisherInterceptorResult<T> for EventEnvelope<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn into_publisher_interceptor_result(self) -> EventBusResult<Option<EventEnvelope<T>>> {
        Ok(Some(self))
    }
}
impl<T> IntoPublisherInterceptorResult<T> for Option<EventEnvelope<T>>
where
    T: Clone + Send + Sync + 'static,
{
    fn into_publisher_interceptor_result(self) -> EventBusResult<Option<EventEnvelope<T>>> {
        Ok(self)
    }
}
impl<T> IntoPublisherInterceptorResult<T> for EventBusResult<EventEnvelope<T>>
where
    T: Clone + Send + Sync + 'static,
{
    fn into_publisher_interceptor_result(self) -> EventBusResult<Option<EventEnvelope<T>>> {
        self.map(Some)
    }
}
impl<T> IntoPublisherInterceptorResult<T> for EventBusResult<Option<EventEnvelope<T>>>
where
    T: Clone + Send + Sync + 'static,
{
    fn into_publisher_interceptor_result(self) -> EventBusResult<Option<EventEnvelope<T>>> {
        self
    }
}

/// Intercepts typed events before they are published.
pub trait PublisherInterceptor<T: Clone + Send + Sync + 'static>: Send + Sync + 'static {
    /// Applies the interceptor to an outgoing event.
    fn on_publish(&self, envelope: EventEnvelope<T>) -> EventBusResult<Option<EventEnvelope<T>>>;
}
impl<T, F, R> PublisherInterceptor<T> for F
where
    T: Clone + Send + Sync + 'static,
    F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
    R: IntoPublisherInterceptorResult<T> + 'static,
{
    fn on_publish(&self, envelope: EventEnvelope<T>) -> EventBusResult<Option<EventEnvelope<T>>> {
        self(envelope).into_publisher_interceptor_result()
    }
}

/// Converts global publisher interceptor return values into a common result.
pub trait IntoPublisherInterceptorAnyResult {
    /// Converts this value into an optional metadata result.
    fn into_publisher_interceptor_any_result(self) -> EventBusResult<Option<EventEnvelopeMetadata>>;
}
impl IntoPublisherInterceptorAnyResult for EventEnvelopeMetadata {
    fn into_publisher_interceptor_any_result(self) -> EventBusResult<Option<EventEnvelopeMetadata>> {
        Ok(Some(self))
    }
}
impl IntoPublisherInterceptorAnyResult for Option<EventEnvelopeMetadata> {
    fn into_publisher_interceptor_any_result(self) -> EventBusResult<Option<EventEnvelopeMetadata>> {
        Ok(self)
    }
}
impl IntoPublisherInterceptorAnyResult for EventBusResult<EventEnvelopeMetadata> {
    fn into_publisher_interceptor_any_result(self) -> EventBusResult<Option<EventEnvelopeMetadata>> {
        self.map(Some)
    }
}
impl IntoPublisherInterceptorAnyResult for EventBusResult<Option<EventEnvelopeMetadata>> {
    fn into_publisher_interceptor_any_result(self) -> EventBusResult<Option<EventEnvelopeMetadata>> {
        self
    }
}

/// Intercepts outgoing metadata for every payload type.
pub trait PublisherInterceptorAny: Send + Sync + 'static {
    /// Applies the interceptor to outgoing event metadata.
    fn on_publish(&self, metadata: EventEnvelopeMetadata) -> EventBusResult<Option<EventEnvelopeMetadata>>;
}
impl<F, R> PublisherInterceptorAny for F
where
    F: Fn(EventEnvelopeMetadata) -> R + Send + Sync + 'static,
    R: IntoPublisherInterceptorAnyResult + 'static,
{
    fn on_publish(&self, metadata: EventEnvelopeMetadata) -> EventBusResult<Option<EventEnvelopeMetadata>> {
        self(metadata).into_publisher_interceptor_any_result()
    }
}

/// Intercepts typed subscriber processing.
pub trait SubscriberInterceptor<T: Clone + Send + Sync + 'static>: Send + Sync + 'static {
    /// Applies the interceptor to an incoming event and continuation chain.
    fn on_consume(&self, envelope: EventEnvelope<T>, chain: SubscriberInterceptorChain<T>) -> EventBusResult<()>;
}
impl<T, F, R> SubscriberInterceptor<T> for F
where
    T: Clone + Send + Sync + 'static,
    F: Fn(EventEnvelope<T>, SubscriberInterceptorChain<T>) -> R + Send + Sync + 'static,
    R: IntoEventBusResult + 'static,
{
    fn on_consume(&self, envelope: EventEnvelope<T>, chain: SubscriberInterceptorChain<T>) -> EventBusResult<()> {
        self(envelope, chain).into_event_bus_result()
    }
}

/// Intercepts subscriber processing for every payload type.
pub trait SubscriberInterceptorAny: Send + Sync + 'static {
    /// Applies the interceptor to incoming event metadata and continuation.
    fn on_consume(&self, metadata: EventEnvelopeMetadata, chain: SubscriberInterceptorAnyChain) -> EventBusResult<()>;
}
impl<F, R> SubscriberInterceptorAny for F
where
    F: Fn(EventEnvelopeMetadata, SubscriberInterceptorAnyChain) -> R + Send + Sync + 'static,
    R: IntoEventBusResult + 'static,
{
    fn on_consume(&self, metadata: EventEnvelopeMetadata, chain: SubscriberInterceptorAnyChain) -> EventBusResult<()> {
        self(metadata, chain).into_event_bus_result()
    }
}

type PublisherInterceptorFn<T> = dyn PublisherInterceptor<T>;
type SubscriberInterceptorFn<T> = dyn SubscriberInterceptor<T>;

/// Typed publisher interceptor adapter.
struct TypedPublisherInterceptor<T: Clone + Send + Sync + 'static> {
    interceptor: Arc<PublisherInterceptorFn<T>>,
}

/// Creates a type-erased publisher interceptor entry.
///
/// # Parameters
/// - `interceptor`: Typed publisher interceptor callback.
///
/// # Returns
/// Type-erased entry suitable for local bus storage.
pub(in crate::local) fn create_publisher_interceptor_entry<T, I>(interceptor: I) -> Arc<dyn PublisherInterceptorEntry>
where
    T: Clone + Send + Sync + 'static,
    I: PublisherInterceptor<T>,
{
    Arc::new(TypedPublisherInterceptor::<T> {
        interceptor: Arc::new(interceptor),
    })
}

impl<T> PublisherInterceptorEntry for TypedPublisherInterceptor<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Returns the payload [`TypeId`] handled by this interceptor.
    fn payload_type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }

    /// Downcasts and applies the typed interceptor.
    fn intercept(&self, envelope: Box<dyn Any + Send>) -> EventBusResult<Option<Box<dyn Any + Send>>> {
        let envelope = envelope
            .downcast::<EventEnvelope<T>>()
            .map_err(|_| EventBusError::type_mismatch(type_name::<EventEnvelope<T>>(), "unknown"))?;
        match panic::catch_unwind(AssertUnwindSafe(|| self.interceptor.on_publish(*envelope))) {
            Ok(Ok(envelope)) => Ok(envelope.map(|envelope| Box::new(envelope) as Box<dyn Any + Send>)),
            Ok(Err(error)) => Err(EventBusError::interceptor_failed("publish", error.to_string())),
            Err(_) => Err(EventBusError::interceptor_failed(
                "publish",
                "publisher interceptor panicked",
            )),
        }
    }
}

/// Typed subscriber interceptor adapter.
struct TypedSubscriberInterceptor<T: Clone + Send + Sync + 'static> {
    interceptor: Arc<SubscriberInterceptorFn<T>>,
}

/// Creates a type-erased subscriber interceptor entry.
///
/// # Parameters
/// - `interceptor`: Typed subscriber interceptor callback.
///
/// # Returns
/// Type-erased entry suitable for local bus storage.
pub(in crate::local) fn create_subscriber_interceptor_entry<T, I>(interceptor: I) -> Arc<dyn SubscriberInterceptorEntry>
where
    T: Clone + Send + Sync + 'static,
    I: SubscriberInterceptor<T>,
{
    Arc::new(TypedSubscriberInterceptor::<T> {
        interceptor: Arc::new(interceptor),
    })
}

impl<T> SubscriberInterceptorEntry for TypedSubscriberInterceptor<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Returns the payload [`TypeId`] handled by this interceptor.
    fn payload_type_id(&self) -> TypeId {
        TypeId::of::<T>()
    }

    /// Downcasts and wraps the typed handler.
    fn wrap_handler(&self, handler: Box<dyn Any + Send + Sync>) -> EventBusResult<Box<dyn Any + Send + Sync>> {
        let next = handler
            .downcast::<Arc<HandlerFn<T>>>()
            .map_err(|_| EventBusError::type_mismatch(type_name::<Arc<HandlerFn<T>>>(), "unknown"))?;
        let next = *next;
        let interceptor = Arc::clone(&self.interceptor);
        let wrapped: Arc<HandlerFn<T>> = Arc::new(move |event| {
            let downstream_error = create_downstream_error_slot();
            let next_chain =
                SubscriberInterceptorChain::with_downstream_error(Arc::clone(&next), Arc::clone(&downstream_error));
            let result = panic::catch_unwind(AssertUnwindSafe(|| interceptor.on_consume(event, next_chain)));
            normalize_subscriber_interceptor_result(result, &downstream_error, "subscriber interceptor panicked")
        });
        Ok(Box::new(wrapped))
    }
}

impl LocalEventBus {
    /// Applies global publisher interceptors in registration order.
    pub(super) fn apply_global_publisher_interceptors<T>(
        &self,
        mut envelope: EventEnvelope<T>,
    ) -> EventBusResult<Option<EventEnvelope<T>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        for interceptor in self.inner.global_publisher_interceptors()? {
            let metadata = envelope.metadata();
            let metadata = match panic::catch_unwind(AssertUnwindSafe(|| interceptor.on_publish(metadata))) {
                Ok(Ok(Some(metadata))) => metadata,
                Ok(Ok(None)) => return Ok(None),
                Ok(Err(error)) => {
                    return Err(EventBusError::interceptor_failed("publish", error.to_string()));
                }
                Err(_) => {
                    return Err(EventBusError::interceptor_failed(
                        "publish",
                        "global publisher interceptor panicked",
                    ));
                }
            };
            envelope.apply_metadata(metadata);
        }
        Ok(Some(envelope))
    }

    /// Applies typed publisher interceptors in registration order.
    pub(super) fn apply_typed_publisher_interceptors<T>(
        &self,
        envelope: EventEnvelope<T>,
    ) -> EventBusResult<Option<EventEnvelope<T>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let interceptors = self.inner.publisher_interceptors()?;
        let mut current: Option<Box<dyn Any + Send>> = Some(Box::new(envelope));
        for interceptor in interceptors {
            if interceptor.payload_type_id() == TypeId::of::<T>()
                && let Some(boxed) = current.take()
            {
                current = interceptor.intercept(boxed)?;
            }
        }
        current
            .map(|boxed| {
                boxed
                    .downcast::<EventEnvelope<T>>()
                    .map(|envelope| *envelope)
                    .map_err(|_| EventBusError::type_mismatch(type_name::<EventEnvelope<T>>(), "unknown"))
            })
            .transpose()
    }

    /// Applies matching subscriber interceptors to a handler.
    pub(super) fn apply_subscriber_interceptors<T>(
        &self,
        handler: Arc<HandlerFn<T>>,
    ) -> EventBusResult<Arc<HandlerFn<T>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let interceptors = self.inner.subscriber_interceptors()?;
        let mut chain = Box::new(handler) as Box<dyn Any + Send + Sync>;
        for interceptor in interceptors.into_iter().rev() {
            if interceptor.payload_type_id() == TypeId::of::<T>() {
                chain = interceptor.wrap_handler(chain)?;
            }
        }
        let handler = chain
            .downcast::<Arc<HandlerFn<T>>>()
            .map(|handler| *handler)
            .map_err(|_| EventBusError::type_mismatch(type_name::<Arc<HandlerFn<T>>>(), "unknown"))?;
        self.apply_global_subscriber_interceptors(handler)
    }

    /// Applies global subscriber interceptors around a typed handler chain.
    fn apply_global_subscriber_interceptors<T>(&self, handler: Arc<HandlerFn<T>>) -> EventBusResult<Arc<HandlerFn<T>>>
    where
        T: Clone + Send + Sync + 'static,
    {
        let mut chain = handler;
        for interceptor in self.inner.global_subscriber_interceptors()?.into_iter().rev() {
            let next = Arc::clone(&chain);
            chain = Arc::new(move |event: EventEnvelope<T>| {
                let metadata = event.metadata();
                let next = Arc::clone(&next);
                let event_for_next = event.clone();
                let downstream_error = create_downstream_error_slot();
                let chain = SubscriberInterceptorAnyChain::with_downstream_error(
                    Arc::new(move || next(event_for_next.clone())),
                    Arc::clone(&downstream_error),
                );
                let result = panic::catch_unwind(AssertUnwindSafe(|| interceptor.on_consume(metadata, chain)));
                normalize_subscriber_interceptor_result(
                    result,
                    &downstream_error,
                    "global subscriber interceptor panicked",
                )
            });
        }
        Ok(chain)
    }
}
