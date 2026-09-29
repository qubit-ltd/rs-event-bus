// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Drop-based reservation that releases one pipeline admission slot.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// RAII reservation; dropping it releases exactly one admission slot.
#[derive(Debug)]
pub(crate) struct AdmissionPermit {
    /// Shared counter decremented when this reservation is released.
    pub(in crate::pipeline) in_flight: Arc<AtomicUsize>,
}

impl Drop for AdmissionPermit {
    /// Releases this permit's slot from the shared in-flight count.
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}
