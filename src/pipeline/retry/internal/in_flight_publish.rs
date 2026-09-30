// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Drop guard for in-flight provider publication attempts.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

/// Retains conservative admission evidence if retry abandons an in-flight SPI
/// call. Creating an unpolled attempt future does not create this guard.
#[must_use]
pub(in crate::pipeline::retry) struct InFlightPublish {
    /// Monotonic evidence shared with the complete publication.
    pub(in crate::pipeline::retry) seen_unknown: Arc<AtomicBool>,
    /// Whether SPI returned an explicit result before the future was dropped.
    pub(in crate::pipeline::retry) completed: bool,
}

impl Drop for InFlightPublish {
    /// Marks an unfinished attempt as potentially admitted without fabricating
    /// a public failure or invoking error callbacks when the caller drops it.
    #[inline]
    fn drop(&mut self) {
        if !self.completed {
            self.seen_unknown.store(true, Ordering::Release);
        }
    }
}
