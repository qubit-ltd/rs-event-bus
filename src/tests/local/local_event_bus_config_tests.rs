// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal configuration round-trip contracts unavailable from the facade.

use std::num::NonZeroUsize;

use crate::error::ConfigurationError;
use crate::local::LocalEventBusConfig;
use crate::registry::EventBusConfig;

/// Parsing preserves the entire configuration, including the optional budget.
#[test]
fn test_native_payload_weight_budget_round_trip_and_zero_field() {
    let config = LocalEventBusConfig::new()
        .queue_capacity(17)
        .max_total_outstanding(31)
        .max_total_outstanding_weight_bytes(NonZeroUsize::new(4096).expect("positive budget"));
    let registry = EventBusConfig::default().with_provider_options(config.provider_options());
    assert_eq!(
        LocalEventBusConfig::from_provider_options(&registry).expect("options parse"),
        config
    );
    let invalid = EventBusConfig::default().with_provider_options(
        [(
            "local.max_total_outstanding_weight_bytes".to_owned(),
            "0".to_owned(),
        )]
        .into(),
    );
    assert!(matches!(
        LocalEventBusConfig::from_provider_options(&invalid),
        Err(ConfigurationError::InvalidField {
            field: "local.max_total_outstanding_weight_bytes",
            ..
        })
    ));
}
