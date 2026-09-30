// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outcomes of the common publication preparation boundary.

use super::PreparedPublish;
use crate::model::PublishReceipt;

/// A completed interceptor drop or a publication ready for the provider.
///
/// # Type Parameters
/// - `T`: the payload type held by the prepared publication.
#[must_use]
pub(in crate::pipeline::publisher) enum PublishPreparation<T: 'static> {
    /// An interceptor completed the publication without entering SPI.
    Dropped(PublishReceipt),
    /// Prepared data ready for one or more provider attempts.
    Ready(Box<PreparedPublish<T>>),
}
