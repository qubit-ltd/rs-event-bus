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
pub(crate) fn decode_payload<T: Send + Sync + 'static>(
    codec: Option<&Arc<dyn EventCodec<T>>>,
    payload: TransportPayload,
) -> Result<Arc<T>, CodecError> {
    match payload {
        TransportPayload::Native(value) => {
            Arc::downcast::<T>(value).map_err(|_| decode_error("native payload type does not match subscribed topic"))
        }
        TransportPayload::Encoded(encoded) => {
            let codec = codec.ok_or_else(|| decode_error("encoded payload has no resolved codec"))?;
            call_codec("decode", || codec.decode(encoded.bytes())).map(Arc::new)
        }
    }
}

fn decode_error(message: &'static str) -> CodecError {
    CodecError::Decode {
        source: Box::new(std::io::Error::other(message)),
    }
}
