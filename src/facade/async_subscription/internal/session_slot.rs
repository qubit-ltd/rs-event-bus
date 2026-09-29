// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::facade::async_subscription::AsyncSession;

/// Temporarily leased subscription session state.
pub(in crate::facade) struct SessionSlot<T: 'static> {
    /// Session available to the active runner or shutdown caller.
    pub(super) session: Option<AsyncSession<T>>,
    /// Whether a caller currently owns the session lease.
    pub(super) active: bool,
    /// Whether the public subscription handle was dropped.
    pub(super) disposed: bool,
}
