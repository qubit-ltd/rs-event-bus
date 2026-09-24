// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// qubit-style: allow multiple-public-types
// qubit-style: allow type-file-name

//! Subscriber processing semantics shared by sync and async facades.

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use qubit_retry::RetryError;

use crate::error::CapabilityError;
use crate::error::DeliveryAttemptError;
use crate::error::DeliveryError;
use crate::model::AckMode;
use crate::model::AcknowledgementState;
use crate::model::AsyncSubscriberInterceptor;
use crate::model::Delivery;
use crate::model::FailureDirective;
use crate::model::SubscriberInterceptor;
use crate::spi::DeliveryDisposition;
use crate::spi::SettlementCapabilities;
use crate::spi::SpiFuture;

/// The outcome of one handler attempt after applying its acknowledgement mode.
#[derive(Debug)]
pub(crate) enum DeliveryOutcome {
    /// The handler accepted this delivery attempt.
    Success,
    /// The handler failed or a manual acknowledgement was not affirmative.
    Failure(DeliveryError),
}

/// Distinguishes local attempts from broker-level retry and terminal actions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DeliveryFailureAction {
    /// Re-run handler and middleware on the same received delivery.
    RetryLocally,
    /// Release the message for provider redelivery.
    Requeue,
    /// Publish a facade dead-letter record before rejecting the provider
    /// message.
    DeadLetter,
    /// Stop processing without dead-letter publication.
    Discard,
}

/// Shared subscriber chain, attempt, and settlement policy.
pub(crate) struct SubscriberPipeline;

type SyncDeliveryHandler<T> = dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync + 'static;
type AsyncDeliveryHandler<T> =
    dyn Fn(Delivery<T>) -> SpiFuture<'static, Result<(), DeliveryError>> + Send + Sync + 'static;

impl SubscriberPipeline {
    /// Executes outer-to-inner synchronous middleware and the user handler.
    pub(crate) fn run_sync<T: 'static, H>(
        delivery: Delivery<T>,
        global: &[Arc<SubscriberInterceptor<T>>],
        typed: &[Arc<SubscriberInterceptor<T>>],
        handler: H,
    ) -> Result<(), DeliveryError>
    where
        H: Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync + 'static,
    {
        let chain = global.iter().chain(typed.iter()).cloned().collect::<Vec<_>>();
        sync_next(0, chain, Arc::new(handler))(delivery)
    }

    /// Executes the runtime-neutral async middleware chain and handler.
    pub(crate) fn run_async<T: Send + Sync + 'static, H>(
        delivery: Delivery<T>,
        global: &[Arc<AsyncSubscriberInterceptor<T>>],
        typed: &[Arc<AsyncSubscriberInterceptor<T>>],
        handler: H,
    ) -> SpiFuture<'static, Result<(), DeliveryError>>
    where
        H: Fn(Delivery<T>) -> SpiFuture<'static, Result<(), DeliveryError>> + Send + Sync + 'static,
    {
        let chain = global.iter().chain(typed.iter()).cloned().collect::<Vec<_>>();
        let next = async_next(0, chain, Arc::new(handler));
        catch_async_panic(next(delivery))
    }

    /// Executes all sync middleware and applies the ACK result matrix once.
    pub(crate) fn attempt_sync<T: 'static, H>(
        mode: AckMode,
        delivery: Delivery<T>,
        global: &[Arc<SubscriberInterceptor<T>>],
        typed: &[Arc<SubscriberInterceptor<T>>],
        handler: H,
    ) -> DeliveryOutcome
    where
        H: Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync + 'static,
    {
        let result = Self::run_sync(delivery.clone(), global, typed, handler);
        Self::finish_attempt(mode, &delivery, result)
    }

    /// Executes async middleware and applies the ACK result matrix once.
    pub(crate) async fn attempt_async<T: Send + Sync + 'static, H>(
        mode: AckMode,
        delivery: Delivery<T>,
        global: &[Arc<AsyncSubscriberInterceptor<T>>],
        typed: &[Arc<AsyncSubscriberInterceptor<T>>],
        handler: H,
    ) -> DeliveryOutcome
    where
        H: Fn(Delivery<T>) -> SpiFuture<'static, Result<(), DeliveryError>> + Send + Sync + 'static,
    {
        let result = Self::run_async(delivery.clone(), global, typed, handler).await;
        Self::finish_attempt(mode, &delivery, result)
    }

    /// Wraps one failed attempt as a `qubit-retry` input without losing source.
    #[cfg(test)]
    pub(crate) fn attempt_error(error: DeliveryError) -> DeliveryAttemptError {
        DeliveryAttemptError::new("delivery", None, error)
    }

    /// Preserves retry terminal metadata and its full source chain publicly.
    pub(crate) fn retry_error(error: RetryError<DeliveryAttemptError>) -> DeliveryError {
        DeliveryError::Retry(Box::new(error))
    }

    /// Applies Auto/Manual acknowledgement rules to a single attempt.
    pub(crate) fn finish_attempt<T: 'static>(
        mode: AckMode,
        delivery: &Delivery<T>,
        result: Result<(), DeliveryError>,
    ) -> DeliveryOutcome {
        match (mode, result) {
            (_, Err(error)) => DeliveryOutcome::Failure(error),
            (AckMode::Auto, Ok(())) => DeliveryOutcome::Success,
            (AckMode::Manual, Ok(())) => match delivery.acknowledgement().state() {
                AcknowledgementState::Acknowledged => DeliveryOutcome::Success,
                AcknowledgementState::Pending => {
                    DeliveryOutcome::Failure(ack_state_error("manual acknowledgement remained pending"))
                }
                AcknowledgementState::NegativelyAcknowledged => {
                    DeliveryOutcome::Failure(ack_state_error("delivery was negatively acknowledged"))
                }
            },
        }
    }

    /// Separates facade retry from provider redelivery and dead-letter actions.
    pub(crate) fn failure_action(directive: FailureDirective) -> DeliveryFailureAction {
        match directive {
            FailureDirective::Retry => DeliveryFailureAction::RetryLocally,
            FailureDirective::Requeue => DeliveryFailureAction::Requeue,
            FailureDirective::DeadLetter => DeliveryFailureAction::DeadLetter,
            FailureDirective::Discard => DeliveryFailureAction::Discard,
        }
    }

    /// Maps a completed failure action to a provider settlement when possible.
    pub(crate) fn failure_disposition(
        action: DeliveryFailureAction,
        capability: SettlementCapabilities,
    ) -> Option<DeliveryDisposition> {
        match (action, capability) {
            (_, SettlementCapabilities::None | SettlementCapabilities::AcceptOnly) => None,
            (DeliveryFailureAction::Requeue, SettlementCapabilities::AcceptRetryReject) => {
                Some(DeliveryDisposition::Retry)
            }
            (DeliveryFailureAction::RetryLocally, SettlementCapabilities::AcceptRetryReject) => None,
            (
                DeliveryFailureAction::DeadLetter | DeliveryFailureAction::Discard,
                SettlementCapabilities::AcceptRetryReject,
            ) => Some(DeliveryDisposition::Reject),
        }
    }

    /// Maps successful handler completion to provider acceptance when possible.
    #[cfg(test)]
    pub(crate) fn success_disposition(capability: SettlementCapabilities) -> Option<DeliveryDisposition> {
        match capability {
            SettlementCapabilities::None => None,
            SettlementCapabilities::AcceptOnly | SettlementCapabilities::AcceptRetryReject => {
                Some(DeliveryDisposition::Accept)
            }
        }
    }

    /// Rejects manual ACK when a provider cannot settle consumed deliveries.
    pub(crate) fn validate_ack_capability(
        mode: AckMode,
        capability: SettlementCapabilities,
    ) -> Result<(), CapabilityError> {
        if mode == AckMode::Manual && capability != SettlementCapabilities::AcceptRetryReject {
            return Err(CapabilityError::Unsupported {
                capability: "manual_ack",
            });
        }
        Ok(())
    }
}

fn sync_next<T: 'static>(
    index: usize,
    chain: Vec<Arc<SubscriberInterceptor<T>>>,
    handler: Arc<SyncDeliveryHandler<T>>,
) -> Arc<SyncDeliveryHandler<T>> {
    Arc::new(move |delivery| {
        if index == chain.len() {
            return std::panic::catch_unwind(AssertUnwindSafe(|| handler(delivery)))
                .unwrap_or_else(|panic| Err(panic_to_delivery_error(panic)));
        }
        let next = sync_next(index + 1, chain.clone(), handler.clone());
        std::panic::catch_unwind(AssertUnwindSafe(|| {
            (chain[index])(delivery, Box::new(move |delivery| next(delivery)))
        }))
        .unwrap_or_else(|panic| Err(panic_to_delivery_error(panic)))
    })
}

fn async_next<T: 'static>(
    index: usize,
    chain: Vec<Arc<AsyncSubscriberInterceptor<T>>>,
    handler: Arc<AsyncDeliveryHandler<T>>,
) -> Arc<AsyncDeliveryHandler<T>> {
    Arc::new(move |delivery| {
        if index == chain.len() {
            let future = std::panic::catch_unwind(AssertUnwindSafe(|| handler(delivery)))
                .unwrap_or_else(|panic| Box::pin(async move { Err(panic_to_delivery_error(panic)) }));
            return catch_async_panic(future);
        }
        let next = async_next(index + 1, chain.clone(), handler.clone());
        let future = std::panic::catch_unwind(AssertUnwindSafe(|| {
            (chain[index])(delivery, Box::new(move |delivery| next(delivery)))
        }))
        .unwrap_or_else(|panic| Box::pin(async move { Err(panic_to_delivery_error(panic)) }));
        catch_async_panic(future)
    })
}

fn catch_async_panic<F>(future: F) -> SpiFuture<'static, Result<(), DeliveryError>>
where
    F: Future<Output = Result<(), DeliveryError>> + Send + 'static,
{
    Box::pin(CatchUnwindFuture {
        future: Box::pin(future),
    })
}

struct CatchUnwindFuture<F: Future> {
    future: Pin<Box<F>>,
}

impl<F: Future<Output = Result<(), DeliveryError>>> Future for CatchUnwindFuture<F> {
    type Output = Result<(), DeliveryError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        match std::panic::catch_unwind(AssertUnwindSafe(|| this.future.as_mut().poll(context))) {
            Ok(Poll::Ready(Ok(()))) => Poll::Ready(Ok(())),
            Ok(Poll::Ready(Err(error))) => Poll::Ready(Err(error)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(panic) => Poll::Ready(Err(panic_to_delivery_error(panic))),
        }
    }
}

fn panic_to_delivery_error(panic: Box<dyn std::any::Any + Send>) -> DeliveryError {
    DeliveryError::Handler {
        source: Box::new(std::io::Error::other(panic_message(panic.as_ref()))),
    }
}

fn panic_message(panic: &(dyn std::any::Any + Send)) -> &'static str {
    if panic.is::<&'static str>() || panic.is::<String>() {
        "subscriber middleware or handler panicked"
    } else {
        "subscriber middleware or handler panicked with a non-string payload"
    }
}

fn ack_state_error(message: &'static str) -> DeliveryError {
    DeliveryError::Handler {
        source: Box::new(std::io::Error::other(message)),
    }
}
