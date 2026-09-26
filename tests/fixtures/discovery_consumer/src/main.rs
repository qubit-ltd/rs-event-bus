// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use discovery_provider_fixture as _;
use qubit_event_bus::EventBusConfig;
use qubit_event_bus::EventBusRegistry;
use qubit_spi::ProviderSelection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let registry = EventBusRegistry::discover()?;
    assert!(registry.provider_ids().iter().any(|id| id.as_str() == "fixture-provider"));
    registry.set_default_selection(ProviderSelection::named("fixture-provider")?)?;
    registry.seal();
    let _bus = registry.create(&EventBusConfig::default())?;
    Ok(())
}
