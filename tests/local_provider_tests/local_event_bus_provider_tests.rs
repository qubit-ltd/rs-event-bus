// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public contract tests for the built-in local provider.

use std::sync::Arc;

use qubit_event_bus::EventBusConfig;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::ShutdownMode;
use qubit_spi::ProviderMetadata;
use qubit_spi::ProviderSelector;
use qubit_spi::ServiceProvider;

/// Verifies default configuration constructs a native local SPI.
#[test]
fn test_default_configuration_creates_a_native_local_spi() {
    let descriptor = LocalEventBusProvider.descriptor();
    assert_eq!("local", descriptor.id().as_str());
    assert_eq!(
        ["memory", "in-process"],
        descriptor
            .aliases()
            .iter()
            .map(ProviderSelector::as_str)
            .collect::<Vec<_>>()
            .as_slice()
    );

    let spi: Arc<dyn EventBusSpi> = LocalEventBusProvider
        .create_configured(&EventBusConfig::default())
        .expect("default provider config is valid");
    assert_eq!(PayloadModes::Native, spi.capabilities().payload_modes());
    let _ = spi.shutdown(ShutdownMode::Immediate).expect("local SPI closes");
}
