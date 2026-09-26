// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Resolves the codec selected for one typed topic.

use std::sync::Arc;

use super::CodecRegistry;
use super::EventCodec;
use crate::model::Topic;

/// Resolves a topic codec before falling back to the facade registry.
///
/// The returned codec is owned so a subscription can retain the selected
/// codec for its entire lifetime. `None` means neither source has a codec.
pub(crate) fn resolve_codec<T: Send + Sync + 'static>(
    topic: &Topic<T>,
    registry: &CodecRegistry,
) -> Option<Arc<dyn EventCodec<T>>> {
    topic.codec().cloned().or_else(|| registry.get::<T>())
}
