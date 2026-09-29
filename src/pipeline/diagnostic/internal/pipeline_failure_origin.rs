// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Stages at which publisher pipeline execution can fail.

/// The stage that produced a publisher pipeline failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub(crate) enum PipelineFailureOrigin {
    /// Typed or global publisher interceptor failed.
    Interceptor,
    /// The selected SPI does not support the requested payload mode.
    Capability,
    /// Encoding the event payload failed.
    Codec,
    /// The selected provider failed during publication.
    Provider,
    /// Retry execution terminated before publication succeeded.
    Retry,
}
