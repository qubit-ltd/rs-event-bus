// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Payload codec contracts and registration.

mod codec_registry;
mod event_codec;
mod resolve_codec;

pub use codec_registry::CodecRegistry;
pub use event_codec::EventCodec;
pub(crate) use resolve_codec::resolve_codec;
