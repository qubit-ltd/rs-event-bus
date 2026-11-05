// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Payload codec contracts and registration.

pub(crate) use self::check_publish_codec::check_publish_codec;
pub(crate) use self::codec_boundary::call_codec;
pub use self::codec_registration_error::CodecRegistrationError;
pub use self::codec_registry::CodecRegistry;
pub(crate) use self::decode_payload::decode_payload;
pub use self::event_codec::EventCodec;
pub(crate) use self::resolve_codec::resolve_codec;

mod check_publish_codec;
mod codec_boundary;
mod codec_registration_error;
mod codec_registry;
mod decode_payload;
mod event_codec;
mod resolve_codec;

#[path = "internal/receive_failure_action.rs"]
mod receive_failure_action;
pub(crate) use self::receive_failure_action::ReceiveFailureAction;
pub(crate) use self::receive_failure_action::receive_failure_action;
