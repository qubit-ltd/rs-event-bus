// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Built-in local event bus provider.

mod config;
mod provider;
mod spi;
mod state;
mod subscription;

pub use config::LocalEventBusConfig;
pub use provider::LocalEventBusProvider;
