// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Blocking lifecycle support for synchronous facade operations.

use std::time::Duration;

/// Returns a finite polling interval used to observe cancellation of a blocking
/// SPI receive.
///
/// # Returns
/// A 50 millisecond interval that bounds how long cancellation waits for a
/// blocking receive poll to return.
#[inline]
pub(crate) fn receive_poll_interval() -> Duration {
    Duration::from_millis(50)
}
