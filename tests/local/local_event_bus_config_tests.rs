// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public configuration contracts for the built-in local provider.

use std::num::NonZeroUsize;

use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::registry::EventBusConfig;
use qubit_event_bus::spi::ShutdownMode;
use qubit_spi::ServiceProvider;

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

/// Weight metadata is optional and serialized only after explicit
/// configuration.
#[test]
fn test_native_payload_weight_budget_provider_options() {
    let defaults = LocalEventBusConfig::new();
    assert_eq!(defaults.get_max_total_outstanding_weight_bytes(), None);
    assert!(
        !defaults
            .provider_options()
            .contains_key("local.max_total_outstanding_weight_bytes")
    );
    let weight = NonZeroUsize::new(4096).expect("positive budget");
    let config = defaults.max_total_outstanding_weight_bytes(weight);
    assert_eq!(config.get_max_total_outstanding_weight_bytes(), Some(weight));
    assert_eq!(config.get_queue_capacity(), 1024);
    assert_eq!(config.get_max_total_outstanding(), 65_536);
    let options = config.provider_options();
    assert_eq!(
        options
            .get("local.max_total_outstanding_weight_bytes")
            .map(String::as_str),
        Some("4096")
    );
    let spi = LocalEventBusProvider
        .create_configured(&EventBusConfig::default().with_provider_options(options))
        .expect("serialized weight option parses");
    let _ = spi.shutdown(ShutdownMode::Immediate).expect("provider closes");
}

/// Parsing rejects zero, malformed values, and unknown weight option names.
#[test]
fn test_native_payload_weight_budget_rejects_invalid_options() {
    for (key, value) in [
        ("local.max_total_outstanding_weight_bytes", "0"),
        ("local.max_total_outstanding_weight_bytes", "invalid"),
        ("local.unknown_weight_bytes", "4096"),
    ] {
        let config = EventBusConfig::default().with_provider_options([(key.to_owned(), value.to_owned())].into());
        let error = match LocalEventBusProvider.create_configured(&config) {
            Ok(_) => panic!("invalid option must fail: {key}={value}"),
            Err(error) => error,
        };
        assert!(error.to_string().contains(key), "error must identify {key}: {error}");
    }
}
