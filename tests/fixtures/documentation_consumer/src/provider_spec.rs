// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================





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

pub type ProviderConfig = <EventBusSpec as ServiceSpec>::Config;
pub type SyncOutput = <EventBusSpec as SyncServiceSpec>::Output;
pub type AsyncOutput = <EventBusSpec as AsyncServiceSpec>::Output;

pub fn receive_once(receiver: &mut dyn EventSubscriptionSpi) -> Result<ReceiveOutcome, SpiError> {
    receiver.receive(Duration::ZERO)
}

pub fn settle_without_borrowing_token<'a>(
    receiver: &'a mut dyn AsyncEventSubscriptionSpi,
    token: &SettlementToken,
) -> SpiFuture<'a, Result<(), SpiError>> {
    receiver.settle(token, DeliveryDisposition::Accept)
}
