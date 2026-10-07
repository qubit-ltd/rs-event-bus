// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Crate-internal source-chain contracts for provider panic classification.

use std::error::Error;

use crate::SpiError;
use crate::model::ProviderId;
use crate::spi::panic_boundary::provider_panic;

#[test]
fn test_provider_panic_error_has_stable_operation_context() {
    let provider_id = ProviderId::new("panic-test").expect("valid provider ID");
    let error = provider_panic(provider_id.as_str(), "publish", None, Box::new("provider SPI panicked"));
    assert!(matches!(
        &error,
        SpiError::Operation {
            operation: "publish",
            kind: "provider_panicked",
            retryable: Some(false),
            ..
        }
    ));
    assert_eq!(
        Error::source(&error)
            .expect("provider panic source remains chained")
            .to_string(),
        "provider SPI panicked"
    );
}
