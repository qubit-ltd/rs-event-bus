// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Publisher and subscriber interceptor implementations.

use super::super::subscriber_interceptor_chain::SubscriberInterceptorAnyChain;
use super::super::subscriber_interceptor_chain::SubscriberInterceptorChain;
use crate::EventBusResult;
use crate::EventEnvelope;
use crate::EventEnvelopeMetadata;
use crate::IntoEventBusResult;

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
