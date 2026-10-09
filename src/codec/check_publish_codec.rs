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

/// Checks that encoded publication has a codec registered for the topic.
///
/// This capability check only resolves codec configuration; it does not invoke
/// the codec or contact the provider. Raw publication does not require a codec.
///
/// # Parameters
///
/// * `modes` - Payload representations supported by the provider.
/// * `topic` - Topic whose configured codec is checked for encoded publication.
/// * `registry` - Codec registry used to resolve the topic's codec.
///
/// # Returns
///
/// Returns `Ok(())` when raw publication is supported or an encoded codec is
/// available for the topic.
///
/// # Errors
///
/// Returns [`CapabilityError::CodecRequired`] when encoded publication is
/// supported but no codec is registered for the topic.
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
