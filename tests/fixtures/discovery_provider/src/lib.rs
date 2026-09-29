// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Registers an independently compiled provider through linked inventory.

mod fixture_provider;

use qubit_event_bus::EventBusSpec;
use qubit_event_bus::registry::sync_provider_inventory::Entry;
use qubit_spi::submit_sync_provider;
use fixture_provider::FixtureProvider;


submit_sync_provider! {
    inventory_entry = Entry;
    spec = EventBusSpec;
    provider = FixtureProvider;
}
