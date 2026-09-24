// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime-neutral asynchronous event bus backend contract.

use super::AsyncEventSubscriptionSpi;
use super::EventBusCapabilities;
use super::OutboundMessage;
use super::ShutdownMode;
use super::ShutdownOutcome;
use super::SpiFuture;
use super::SpiSubscriptionRequest;
use crate::error::SpiError;
use crate::model::ProviderId;
use crate::model::PublishAcknowledgement;

/// Minimal object-safe asynchronous transport interface implemented by a
/// backend.
///
/// Returned futures are runtime-neutral `Send` futures. The provider does not
/// create a task or choose an executor on behalf of the facade.
///
/// # Examples
///
/// ```
/// use qubit_event_bus::spi::AsyncEventBusSpi;
///
/// fn payload_modes(provider: &dyn AsyncEventBusSpi) -> qubit_event_bus::spi::PayloadModes {
///     provider.capabilities().payload_modes()
/// }
/// ```
pub trait AsyncEventBusSpi: Send + Sync + 'static {
    /// Returns an optional facade provider identity attached by a registry.
    ///
    /// Provider implementations should leave the default unchanged. Registry
    /// adapters override this hidden metadata hook on a transparent proxy so
    /// the facade can report the exact provider that succeeded after fallback.
    #[doc(hidden)]
    fn provider_id(&self) -> Option<ProviderId> {
        None
    }

    /// Returns the immutable capabilities of this backend instance.
    fn capabilities(&self) -> EventBusCapabilities;

    /// Publishes one transport message.
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>>;

    /// Creates one asynchronous single-owner subscription receiver.
    ///
    /// The caller may cancel this future by dropping it before a receiver is
    /// returned. In that case the provider remains responsible for releasing
    /// any partially allocated subscription resources: because no receiver
    /// was transferred, the caller cannot invoke
    /// [`AsyncEventSubscriptionSpi::close`] and must not rely on
    /// receiver-drop cleanup. Implementations should defer
    /// resource commitment until completion or use cancellation-safe RAII so
    /// dropping the future synchronously aborts or detaches the partial setup.
    /// The provider must not require a hidden runtime task to finish cleanup.
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>>;

    /// Closes this backend according to the requested mode.
    ///
    /// Dropping the returned future or timing out the facade does not imply
    /// that provider-side shutdown work was rolled back. A later call must be
    /// safe and continue or finish shutdown; calling shutdown after the
    /// backend has already closed must succeed and report a stable outcome.
    /// A later `Immediate` call may strengthen a previously requested
    /// `Graceful` shutdown.
    fn shutdown<'a>(&'a self, mode: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>>;
}
