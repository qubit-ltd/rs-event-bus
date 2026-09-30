// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;

use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::ShutdownMode;
use qubit_spi::ProviderMetadata;
use qubit_spi::ServiceProvider;

#[test]
fn test_default_configuration_creates_a_native_local_spi() {
    assert_eq!("local", LocalEventBusProvider.descriptor().id().as_str());
    let spi: Arc<dyn EventBusSpi> = LocalEventBusProvider
        .create_configured(&EventBusConfig::default())
        .expect("default provider config is valid");

    assert_eq!(PayloadModes::Native, spi.capabilities().payload_modes());
    spi.shutdown(ShutdownMode::Immediate)
        .expect("local SPI closes");
}
