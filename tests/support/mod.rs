/*******************************************************************************
 *
 *    Copyright (c) 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Shared integration test helpers.

use std::panic::{self, PanicHookInfo};

type PanicHook = Box<dyn Fn(&PanicHookInfo<'_>) + Send + Sync + 'static>;

/// Restores the previously installed panic hook when dropped.
pub(crate) struct PanicHookGuard {
    previous_hook: Option<PanicHook>,
}

impl PanicHookGuard {
    /// Installs a no-op panic hook for code paths that intentionally catch panics.
    ///
    /// # Returns
    /// Guard restoring the previous hook when it leaves scope.
    pub(crate) fn suppress() -> Self {
        let previous_hook = panic::take_hook();
        panic::set_hook(Box::new(|_| {}));
        Self {
            previous_hook: Some(previous_hook),
        }
    }
}

impl Drop for PanicHookGuard {
    /// Restores the panic hook that was active when the guard was created.
    fn drop(&mut self) {
        if let Some(previous_hook) = self.previous_hook.take() {
            panic::set_hook(previous_hook);
        }
    }
}
