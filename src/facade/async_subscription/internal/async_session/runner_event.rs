// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Receiver-owner transitions selected by the caller-driven poll loop.

/// One fair scheduler transition, selected without retaining a receiver borrow.
pub(super) enum RunnerEvent {
    /// New handler grant already charged by the scheduler.
    Ready(u64),
    /// An ordered terminal disposition is ready to settle on its ordering lane.
    Settle(u64),
    /// A finite receive credit already claimed by this owner.
    Reserve(u64),
    /// A handler or deadline changed the runnable work set.
    Changed,
    /// No work remains after stop.
    Finished,
}
