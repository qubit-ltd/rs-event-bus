// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal subscriber outcome and panic-isolation types.

mod catch_unwind_future;
mod delivery_failure_action;
mod delivery_outcome;

pub(in crate::pipeline::subscriber_pipeline) use catch_unwind_future::CatchUnwindFuture;
pub(crate) use delivery_failure_action::DeliveryFailureAction;
pub(crate) use delivery_outcome::DeliveryOutcome;
