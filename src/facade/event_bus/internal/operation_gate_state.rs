// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Admission state linearized against synchronous facade shutdown.

/// Counts calls admitted before shutdown closed operation admission.
#[derive(Default)]
pub(super) struct OperationGateState {
    /// Whether new facade operations are rejected.
    pub(super) closing: bool,
    /// Number of publish and subscribe calls admitted so far.
    pub(super) active: usize,
}
