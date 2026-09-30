// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Provider service aliases and subscription calls compiled by documentation checks.

use std::time::Duration;

use qubit_event_bus::EventBusSpec;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::SpiFuture;
use qubit_spi::AsyncServiceSpec;
use qubit_spi::ServiceSpec;
use qubit_spi::SyncServiceSpec;

/// Configuration type selected by the event-bus provider service specification.
pub type ProviderConfig = <EventBusSpec as ServiceSpec>::Config;
/// Output returned by the synchronous event-bus provider.
pub type SyncOutput = <EventBusSpec as SyncServiceSpec>::Output;
/// Output returned by the asynchronous event-bus provider.
pub type AsyncOutput = <EventBusSpec as AsyncServiceSpec>::Output;

/// Performs one nonblocking receive attempt on a synchronous subscription.
pub fn receive_once(receiver: &mut dyn EventSubscriptionSpi) -> Result<ReceiveOutcome, SpiError> {
    receiver.receive(Duration::ZERO)
}

/// Starts accepting a token without tying the returned future to the token borrow.
///
/// The future borrows the receiver for `'a`; the caller may release the token
/// borrow independently after this method returns.
pub fn settle_without_borrowing_token<'a>(
    receiver: &'a mut dyn AsyncEventSubscriptionSpi,
    token: &SettlementToken,
) -> SpiFuture<'a, Result<(), SpiError>> {
    receiver.settle(token, DeliveryDisposition::Accept)
}
