// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private state and execution guards used by facade operations.

pub(super) use bus_context_guard::BusContextGuard;
pub(super) use bus_context_guard::is_current_bus_context;
pub(super) use erased_middleware_list::ErasedMiddlewareList;
pub(super) use lifecycle_state::LifecycleState;

mod bus_context_guard;
mod erased_middleware_list;
mod lifecycle_state;

mod shutdown_registration;
pub(super) use shutdown_registration::ShutdownRegistration;
