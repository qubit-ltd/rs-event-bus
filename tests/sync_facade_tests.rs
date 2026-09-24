// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public contract tests for the synchronous event-bus facade.

pub mod error {
    pub use qubit_event_bus::error::*;
}

pub mod facade {
    pub use qubit_event_bus::facade::*;
}

pub mod model {
    pub use qubit_event_bus::model::*;
}

pub mod pipeline {
    pub use qubit_event_bus::pipeline::*;
}

pub mod spi {
    pub use qubit_event_bus::spi::*;
}

#[path = "support/sync_facade_tests.rs"]
mod sync_facade_tests;
