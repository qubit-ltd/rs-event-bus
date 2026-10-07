// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Checks whether a provider can publish one topic with its configured codec.

use super::CodecRegistry;
use super::resolve_codec;
use crate::error::CapabilityError;
use crate::model::Topic;
use crate::spi::PayloadModes;

/// Checks codec availability without invoking codec or provider operations.
pub(crate) fn check_publish_codec<T: Send + Sync + 'static>(
    modes: PayloadModes,
    topic: &Topic<T>,
    registry: &CodecRegistry,
) -> Result<(), CapabilityError> {
    if modes == PayloadModes::Encoded && resolve_codec(topic, registry).is_none() {
        return Err(CapabilityError::CodecRequired);
    }
    Ok(())
}
