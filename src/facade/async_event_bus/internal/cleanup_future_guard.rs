// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Guards a cleanup SPI future against destructor panics when it is cancelled.

use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use crate::error::SpiError;
use crate::internal::panic_payload::dispose_panic_payload;
use crate::spi::SpiFuture;

/// Retains a cleanup future until its terminal result or cancellation.
///
/// Dropping the guard catches and disposes any panic payload from destroying a
/// still-pending future.
///
/// # Type Parameters
/// - `T`: successful cleanup output.
pub(super) struct CleanupFutureGuard<'a, T> {
    /// Future owned until it is explicitly destroyed or the guard is dropped.
    pub(super) future: Option<SpiFuture<'a, Result<T, SpiError>>>,
}

/// Operations for explicitly destroying a retained cleanup future.
///
/// # Type Parameters
/// - `T`: successful cleanup output.
impl<T> CleanupFutureGuard<'_, T> {
    /// Destroys the retained future once and returns any destructor panic.
    ///
    /// # Returns
    /// `Ok(())` when destruction succeeds, or the captured panic payload.
    pub(super) fn drop_future(&mut self) -> Result<(), Box<dyn Any + Send>> {
        let future = self.future.take();
        catch_unwind(AssertUnwindSafe(|| drop(future)))
    }
}

/// Disposes a destructor panic when a pending cleanup future is cancelled.
///
/// # Type Parameters
/// - `T`: successful cleanup output.
impl<T> Drop for CleanupFutureGuard<'_, T> {
    /// Catches a destructor panic so it cannot unwind through cancellation.
    fn drop(&mut self) {
        if let Err(payload) = self.drop_future() {
            dispose_panic_payload(payload);
        }
    }
}
