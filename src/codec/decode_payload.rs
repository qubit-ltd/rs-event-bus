// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Decodes provider payloads at the shared facade boundary.

use std::sync::Arc;

use crate::codec::EventCodec;
use crate::codec::call_codec;
use crate::error::CodecError;
use crate::spi::TransportPayload;

/// Decodes a native or codec-backed provider payload into a shared value.
///
/// Native payloads are downcast to the subscribed type. Encoded payloads use
/// the resolved topic codec, and codec panics are converted at the callback
/// boundary. The receiver owner retains the transport payload and its token.
///
/// # Type Parameters
/// - `T`: the subscribed application payload type.
///
/// # Parameters
/// - `codec`: the codec resolved for the topic, if encoded payloads are
///   supported.
/// - `payload`: borrowed provider payload to decode or downcast.
/// - `max_receive_bytes`: positive limit checked before any codec callback.
///
/// # Returns
/// A shared owner of the decoded application value.
///
/// # Errors
/// Returns [`CodecError`] when the native value has the wrong type, encoded
/// bytes have no codec, decoding fails, or the codec callback panics.
pub(crate) fn decode_payload<T: Send + Sync + 'static>(
    codec: Option<&Arc<dyn EventCodec<T>>>,
    payload: &TransportPayload,
    max_receive_bytes: std::num::NonZeroUsize,
) -> Result<Arc<T>, CodecError> {
    match payload {
        TransportPayload::Native(value) => {
            Arc::downcast::<T>(value.clone()).map_err(|_| CodecError::NativeTypeMismatch)
        }
        TransportPayload::Encoded(encoded) => {
            let actual = encoded.bytes().len();
            let limit = max_receive_bytes.get();
            if actual > limit {
                return Err(CodecError::PayloadTooLarge {
                    direction: crate::model::PayloadDirection::Receive,
                    actual,
                    limit,
                });
            }
            let codec = codec.ok_or_else(|| decode_error("encoded payload has no resolved codec"))?;
            call_codec("validate_metadata", || codec.validate_metadata(encoded))?;
            call_codec("decode", || codec.decode(encoded)).map(Arc::new)
        }
    }
}

/// Creates a decode error for a facade-detected payload mismatch.
///
/// # Parameters
/// - `message`: static explanation of the mismatch.
///
/// # Returns
/// A [`CodecError::Decode`] carrying an I/O source with the supplied message.
fn decode_error(message: &'static str) -> CodecError {
    CodecError::Decode {
        source: Box::new(std::io::Error::other(message)),
    }
}
