// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Additional public-facade scheduling measurement entry point.

// Implements synthetic provider fixtures and their measurement workloads.
mod workloads;

pub(super) use workloads::run;
