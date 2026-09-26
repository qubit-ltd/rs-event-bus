// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Outcome observed after one queued notification reaches the publisher worker.

use crate::error::EventIdGenerationError;
use crate::error::PublishError;
use crate::model::PublishReceipt;

/// The result of constructing or publishing one queued notification.
///
/// A `Published` receipt reports provider admission only. It does not imply
/// that a subscriber handler completed.
#[non_exhaustive]
#[derive(Debug)]
pub enum NotificationOutcome {
    /// The provider returned an admission receipt.
    Published(PublishReceipt),
    /// The facade or provider rejected the publication.
    PublishFailed(PublishError),
    /// A request could not be built because event identity generation failed.
    RequestFailed(EventIdGenerationError),
}
