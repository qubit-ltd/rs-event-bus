// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Subscriber processing semantics shared by sync and async facades.

mod internal;

use std::any::Any;
use std::future::Future;
use std::io::Error;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;

use qubit_retry::RetryError;

use self::internal::CatchUnwindFuture;
pub(crate) use self::internal::DeliveryFailureAction;
pub(crate) use self::internal::DeliveryOutcome;
use crate::error::CapabilityError;
use crate::error::DeliveryAttemptError;
use crate::error::DeliveryError;
use crate::model::AckMode;
use crate::model::AcknowledgementState;
use crate::model::AsyncSubscriberInterceptor;
use crate::model::Delivery;
use crate::model::FailureDirective;
use crate::model::StartPosition;
use crate::model::SubscribeOptions;
use crate::model::SubscriberInterceptor;
use crate::model::SubscriptionDurability;
use crate::spi::DeliveryDisposition;
use crate::spi::DurabilityCapability;
use crate::spi::EventBusCapabilities;
use crate::spi::ReplayCapability;
use crate::spi::SettlementCapabilities;
use crate::spi::SpiFuture;

/// Shared subscriber chain, attempt, and settlement policy.
pub(crate) struct SubscriberPipeline;

/// Synchronous callback invoked at the end of a subscriber chain.
///
/// # Type Parameters
/// - `T`: Payload type carried by each delivery.
type SyncDeliveryHandler<T> =
    dyn Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync + 'static;
/// Runtime-neutral future callback invoked at the end of an async subscriber
/// chain.
///
/// # Type Parameters
/// - `T`: Payload type carried by each delivery.
type AsyncDeliveryHandler<T> =
    dyn Fn(Delivery<T>) -> SpiFuture<'static, Result<(), DeliveryError>> + Send + Sync + 'static;

impl SubscriberPipeline {
    /// Executes outer-to-inner synchronous middleware and the user handler.
    ///
    /// # Type Parameters
    /// - `T`: Delivery payload type.
    /// - `H`: Terminal handler callback type.
    ///
    /// # Parameters
    /// - `delivery`: Event and context passed through the chain.
    /// - `global`: Bus-wide middleware, run before typed middleware.
    /// - `typed`: Subscription-specific middleware.
    /// - `handler`: Callback run after all middleware.
    ///
    /// # Returns
    /// The handler result after middleware has completed.
    ///
    /// # Errors
    /// Returns middleware or handler errors; panics are converted to
    /// `DeliveryError`.
    pub(crate) fn run_sync<T: 'static, H>(
        delivery: Delivery<T>,
        global: &[Arc<SubscriberInterceptor<T>>],
        typed: &[Arc<SubscriberInterceptor<T>>],
        handler: H,
    ) -> Result<(), DeliveryError>
    where
        H: Fn(Delivery<T>) -> Result<(), DeliveryError> + Send + Sync + 'static,
    {
        let chain = global
            .iter()
            .chain(typed.iter())
            .cloned()
            .collect::<Vec<_>>();
        sync_next(0, chain, Arc::new(handler))(delivery)
    }

    /// Executes the runtime-neutral async middleware chain and handler.
    ///
    /// # Type Parameters
    /// - `T`: Delivery payload type.
    /// - `H`: Terminal asynchronous handler callback type.
    ///
    /// # Parameters
    /// - `delivery`: Event and context passed through the chain.
    /// - `global`: Bus-wide middleware, run before typed middleware.
    /// - `typed`: Subscription-specific middleware.
    /// - `handler`: Callback producing the terminal handler future.
    ///
    /// # Returns
    /// A future resolving to the handler result after middleware completes.
    ///
    /// # Errors
    /// Resolves to middleware or handler errors; panics are converted to
    /// `DeliveryError`.
    pub(crate) fn run_async<T: Send + Sync + 'static, H>(
        delivery: Delivery<T>,
        global: &[Arc<AsyncSubscriberInterceptor<T>>],
        typed: &[Arc<AsyncSubscriberInterceptor<T>>],
        handler: H,
    ) -> SpiFuture<'static, Result<(), DeliveryError>>
    where
        H: Fn(Delivery<T>) -> SpiFuture<'static, Result<(), DeliveryError>> + Send + Sync + 'static,
    {
        let chain = global
            .iter()
            .chain(typed.iter())
            .cloned()
            .collect::<Vec<_>>();
        let next = async_next(0, chain, Arc::new(handler));
        catch_async_panic(next(delivery))
    }

    /// Executes all sync middleware and applies the ACK result matrix once.
    ///
    /// # Type Parameters
    /// - `T`: Delivery payload type.
    /// - `H`: Terminal handler callback type.
    ///
    /// # Parameters
    /// - `mode`: Automatic or manual acknowledgement policy.
    /// - `delivery`: Delivery supplied to the chain.
    /// - `global`: Bus-wide middleware.
    /// - `typed`: Subscription-specific middleware.
    /// - `handler`: Terminal handler callback.
    ///
    /// # Returns
    /// The result after applying the acknowledgement policy to the chain
    /// result.
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
    ///
    /// # Type Parameters
    /// - `T`: Delivery payload type.
    /// - `H`: Terminal asynchronous handler callback type.
    ///
    /// # Parameters
    /// - `mode`: Automatic or manual acknowledgement policy.
    /// - `delivery`: Delivery supplied to the chain.
    /// - `global`: Bus-wide middleware.
    /// - `typed`: Subscription-specific middleware.
    /// - `handler`: Terminal handler callback.
    ///
    /// # Returns
    /// The result after applying the acknowledgement policy to the chain
    /// result.
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

    /// Preserves retry terminal metadata and its full source chain publicly.
    ///
    /// # Parameters
    /// - `error`: Retry failure and its attempt metadata.
    ///
    /// # Returns
    /// A delivery error retaining the complete retry error chain.
    pub(crate) fn retry_error(error: RetryError<DeliveryAttemptError>) -> DeliveryError {
        DeliveryError::Retry(Box::new(error))
    }

    /// Applies Auto/Manual acknowledgement rules to a single attempt.
    ///
    /// # Type Parameters
    /// - `T`: Delivery payload type.
    ///
    /// # Parameters
    /// - `mode`: Acknowledgement policy for the subscription.
    /// - `delivery`: Delivery whose acknowledgement state is inspected.
    /// - `result`: Middleware and handler result.
    ///
    /// # Returns
    /// Success or the appropriate handler/acknowledgement failure.
    #[inline]
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
                AcknowledgementState::Pending => DeliveryOutcome::Failure(ack_state_error(
                    "manual acknowledgement remained pending",
                )),
                AcknowledgementState::NegativelyAcknowledged => DeliveryOutcome::Failure(
                    ack_state_error("delivery was negatively acknowledged"),
                ),
            },
        }
    }

    /// Separates facade retry from provider redelivery and dead-letter actions.
    ///
    /// # Parameters
    /// - `directive`: Terminal failure directive selected by error handling.
    ///
    /// # Returns
    /// The corresponding action performed by the facade.
    #[inline]
    pub(crate) fn failure_action(directive: FailureDirective) -> DeliveryFailureAction {
        match directive {
            FailureDirective::Retry => DeliveryFailureAction::RetryLocally,
            FailureDirective::Requeue => DeliveryFailureAction::Requeue,
            FailureDirective::DeadLetter => DeliveryFailureAction::DeadLetter,
            FailureDirective::Discard => DeliveryFailureAction::Discard,
        }
    }

    /// Maps a completed failure action to a provider settlement when possible.
    ///
    /// # Parameters
    /// - `action`: Completed local failure action.
    /// - `capability`: Provider settlement operations supported by the
    ///   transport.
    ///
    /// # Returns
    /// The provider disposition required by the action, or `None` when no
    /// settlement applies.
    #[must_use]
    #[inline]
    pub(crate) fn failure_disposition(
        action: DeliveryFailureAction,
        capability: SettlementCapabilities,
    ) -> Option<DeliveryDisposition> {
        match (action, capability) {
            (_, SettlementCapabilities::None | SettlementCapabilities::AcceptOnly) => None,
            (DeliveryFailureAction::Requeue, SettlementCapabilities::AcceptRetryReject) => {
                Some(DeliveryDisposition::Retry)
            }
            (DeliveryFailureAction::RetryLocally, SettlementCapabilities::AcceptRetryReject) => {
                None
            }
            (
                DeliveryFailureAction::DeadLetter | DeliveryFailureAction::Discard,
                SettlementCapabilities::AcceptRetryReject,
            ) => Some(DeliveryDisposition::Reject),
        }
    }

    /// Rejects manual ACK when a provider cannot settle consumed deliveries.
    ///
    /// # Parameters
    /// - `mode`: Requested acknowledgement policy.
    /// - `capability`: Provider settlement operations supported by the
    ///   transport.
    ///
    /// # Returns
    /// `Ok(())` when the capability supports the requested policy.
    ///
    /// # Errors
    /// Returns `Unsupported` when manual acknowledgement requires unavailable
    /// settlement.
    #[inline]
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

    /// Checks request-level provider capabilities shared by both facades.
    ///
    /// Returns `CapabilityError::Unsupported` before provider subscription
    /// when durable delivery, consumer groups, or historical replay are
    /// requested but unavailable. `New`, ephemeral subscriptions require no
    /// capability beyond the default.
    ///
    /// # Type Parameters
    /// - `T`: Payload type described by the subscription options.
    ///
    /// # Parameters
    /// - `options`: Requested durability, consumer group, and start position.
    /// - `capabilities`: Provider features available for this subscription.
    ///
    /// # Returns
    /// `Ok(())` when every requested subscription feature is supported.
    ///
    /// # Errors
    /// Returns `Unsupported` for the first unavailable requested feature.
    #[inline]
    pub(crate) fn validate_subscription_capabilities<T: 'static>(
        options: &SubscribeOptions<T>,
        capabilities: EventBusCapabilities,
    ) -> Result<(), CapabilityError> {
        if !capabilities
            .subscription_modes()
            .supports(options.durability())
        {
            return Err(CapabilityError::Unsupported {
                capability: "subscription_durability",
            });
        }
        if options.durability() == SubscriptionDurability::Durable
            && capabilities.durability() != DurabilityCapability::Durable
        {
            return Err(CapabilityError::Unsupported {
                capability: "durability",
            });
        }
        if options.consumer_group().is_some() && !capabilities.consumer_groups() {
            return Err(CapabilityError::Unsupported {
                capability: "consumer_groups",
            });
        }
        if !matches!(options.start_position(), StartPosition::New)
            && !matches!(
                capabilities.replay(),
                ReplayCapability::Position | ReplayCapability::Timestamp
            )
        {
            return Err(CapabilityError::Unsupported {
                capability: "replay",
            });
        }
        Ok(())
    }
}

/// Builds the remaining synchronous middleware chain from a given position.
///
/// # Type Parameters
/// - `T`: Delivery payload type.
///
/// # Parameters
/// - `index`: Next middleware index to invoke.
/// - `chain`: Ordered middleware callbacks.
/// - `handler`: Terminal callback retained by each chain node.
///
/// # Returns
/// A callback representing the remaining middleware and handler chain.
#[must_use]
fn sync_next<T: 'static>(
    index: usize,
    chain: Vec<Arc<SubscriberInterceptor<T>>>,
    handler: Arc<SyncDeliveryHandler<T>>,
) -> Arc<SyncDeliveryHandler<T>> {
    Arc::new(move |delivery| {
        if index == chain.len() {
            return catch_unwind(AssertUnwindSafe(|| handler(delivery)))
                .unwrap_or_else(|panic| Err(panic_to_delivery_error(panic)));
        }
        let next = sync_next(index + 1, chain.clone(), handler.clone());
        catch_unwind(AssertUnwindSafe(|| {
            (chain[index])(delivery, Box::new(move |delivery| next(delivery)))
        }))
        .unwrap_or_else(|panic| Err(panic_to_delivery_error(panic)))
    })
}

/// Builds the remaining asynchronous middleware chain from a given position.
///
/// # Type Parameters
/// - `T`: Delivery payload type.
///
/// # Parameters
/// - `index`: Next middleware index to invoke.
/// - `chain`: Ordered middleware callbacks.
/// - `handler`: Terminal future callback retained by each chain node.
///
/// # Returns
/// A callback representing the remaining middleware and handler chain.
#[must_use]
fn async_next<T: 'static>(
    index: usize,
    chain: Vec<Arc<AsyncSubscriberInterceptor<T>>>,
    handler: Arc<AsyncDeliveryHandler<T>>,
) -> Arc<AsyncDeliveryHandler<T>> {
    Arc::new(move |delivery| {
        if index == chain.len() {
            let future =
                catch_unwind(AssertUnwindSafe(|| handler(delivery))).unwrap_or_else(|panic| {
                    Box::pin(async move { Err(panic_to_delivery_error(panic)) })
                });
            return catch_async_panic(future);
        }
        let next = async_next(index + 1, chain.clone(), handler.clone());
        let future = catch_unwind(AssertUnwindSafe(|| {
            (chain[index])(delivery, Box::new(move |delivery| next(delivery)))
        }))
        .unwrap_or_else(|panic| Box::pin(async move { Err(panic_to_delivery_error(panic)) }));
        catch_async_panic(future)
    })
}

/// Converts a panic while polling an async handler or middleware future into a
/// delivery error.
///
/// # Type Parameters
/// - `F`: Future returned by asynchronous middleware or a handler.
///
/// # Parameters
/// - `future`: Callback future to poll behind the unwind boundary.
///
/// # Returns
/// A runtime-neutral future that resolves to the original result or a panic
/// error.
///
/// # Errors
/// The returned future preserves callback errors and converts polling panics
/// into a delivery handler error.
fn catch_async_panic<F>(future: F) -> SpiFuture<'static, Result<(), DeliveryError>>
where
    F: Future<Output = Result<(), DeliveryError>> + Send + 'static,
{
    Box::pin(CatchUnwindFuture {
        future: Box::pin(future),
    })
}

/// Converts a caught middleware or handler panic to a delivery handler error.
///
/// # Parameters
/// - `panic`: Payload captured by `catch_unwind`.
///
/// # Returns
/// A handler error containing a stable message derived from the payload type.
fn panic_to_delivery_error(panic: Box<dyn Any + Send>) -> DeliveryError {
    DeliveryError::Handler {
        source: Box::new(Error::other(panic_message(panic.as_ref()))),
    }
}

/// Selects a diagnostic message that does not expose arbitrary panic payload
/// data.
///
/// # Parameters
/// - `panic`: Captured panic payload to classify.
///
/// # Returns
/// A stable message distinguishing string and non-string payloads.
#[must_use]
fn panic_message(panic: &(dyn Any + Send)) -> &'static str {
    if panic.is::<&'static str>() || panic.is::<String>() {
        "subscriber middleware or handler panicked"
    } else {
        "subscriber middleware or handler panicked with a non-string payload"
    }
}

/// Creates a handler error for a manual acknowledgement state that cannot
/// succeed.
///
/// # Parameters
/// - `message`: Explanation of the invalid acknowledgement state.
///
/// # Returns
/// A delivery handler error wrapping the supplied message.
fn ack_state_error(message: &'static str) -> DeliveryError {
    DeliveryError::Handler {
        source: Box::new(Error::other(message)),
    }
}
