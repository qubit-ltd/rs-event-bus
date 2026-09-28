// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Payload codec contracts and registration.

mod codec_boundary;
mod codec_registry;
mod decode_payload;
mod event_codec;
mod resolve_codec;

pub(crate) use codec_boundary::call_codec;
pub use codec_registry::CodecRegistry;
pub(crate) use decode_payload::decode_payload;
pub use event_codec::EventCodec;
pub(crate) use resolve_codec::resolve_codec;
