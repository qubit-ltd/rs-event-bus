// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal asynchronous subscription state.

use super::AsyncSession;

/// Temporarily leased subscription session state.
///
/// # Type Parameters
/// - `T`: payload type retained by the subscription session.
pub(in crate::facade::async_subscription) struct SessionSlot<T: 'static> {
    /// Session available to the active runner or shutdown caller.
    pub(in crate::facade::async_subscription) session: Option<AsyncSession<T>>,
    /// Whether a caller currently owns the session lease.
    pub(in crate::facade::async_subscription) active: bool,
    /// Whether the public subscription handle was dropped.
    pub(in crate::facade::async_subscription) disposed: bool,
}
