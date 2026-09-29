// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private state and RAII accounting for the synchronous lifecycle tracker.

mod delivery_tracker_guard;
mod tracker_state;

pub(crate) use delivery_tracker_guard::DeliveryTrackerGuard;
pub(in crate::facade) use tracker_state::TrackerState;
