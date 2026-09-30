// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public configuration contracts for the built-in local provider.

use qubit_event_bus::local::LocalEventBusConfig;

#[test]
fn test_local_configuration_defaults_and_provider_options_match() {
    let config = LocalEventBusConfig::default();
    assert_eq!(1024, config.get_queue_capacity());
    assert_eq!(65_536, config.get_max_total_outstanding());
    assert!(config.validate().is_ok());
    let options = config.provider_options();
    assert_eq!(Some("1024"), options.get("local.queue_capacity").map(String::as_str));
    assert_eq!(
        Some("65536"),
        options.get("local.max_total_outstanding").map(String::as_str)
    );
}

#[test]
fn test_local_configuration_rejects_each_zero_capacity() {
    assert!(LocalEventBusConfig::new().queue_capacity(0).validate().is_err());
    assert!(LocalEventBusConfig::new().max_total_outstanding(0).validate().is_err());
}
