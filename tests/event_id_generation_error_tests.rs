// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public API contract for event ID generation results.

use std::error::Error;

use qubit_event_bus::EventIdGenerationError;
use qubit_event_bus::model::EventId;

#[test]
fn test_event_id_generation_exposes_public_error_type_and_preserves_generator_source() {
    let result: Result<EventId, EventIdGenerationError> = EventId::generate();

    match result {
        Ok(event_id) => assert!(!event_id.as_str().is_empty(), "generated event ID should not be empty"),
        Err(error) => {
            assert_eq!(error.to_string(), "failed to generate event ID");
            assert!(
                Error::source(&error).is_some(),
                "generation error should preserve its underlying source"
            );
        }
    }
}
