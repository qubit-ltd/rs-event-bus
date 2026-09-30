// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal state for publish retry adapters.

// Owns the drop guard that preserves admission uncertainty for in-flight calls.
mod in_flight_publish;

pub(in crate::pipeline::retry) use in_flight_publish::InFlightPublish;
