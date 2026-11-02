// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Example codec that reconstructs dead-letter events from encoded bytes.

use std::sync::Arc;
use std::time::Duration;
use std::time::UNIX_EPOCH;

use qubit_event_bus::CodecError;
use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::DeadLetterEvent;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::EncodedPayload;

const MAX_ENCODED_BYTES: usize = 1_048_576;
static CONTENT_TYPE: ContentType = ContentType::new_static("application/vnd.example.dead-letter");

/// Codec for the documentation fixture's private dead-letter byte format.
///
/// This format demonstrates external reconstruction and is not a wire protocol
/// for applications to adopt.
pub struct DeadLetterCodec;

impl EventCodec<DeadLetterEvent<String>> for DeadLetterCodec {
    fn content_type(&self) -> &ContentType {
        &CONTENT_TYPE
    }

    fn schema_id(&self) -> Option<&qubit_event_bus::model::SchemaId> {
        None
    }

    fn encode(&self, value: &DeadLetterEvent<String>) -> Result<Arc<[u8]>, CodecError> {
        let original = value.original_event();
        let timestamp = original
            .timestamp()
            .duration_since(UNIX_EPOCH)
            .map_err(encode_error)?;
        let mut bytes = Vec::new();
        put_text(&mut bytes, original.id().as_str())?;
        put_text(&mut bytes, original.topic().name())?;
        bytes.extend_from_slice(&timestamp.as_secs().to_be_bytes());
        bytes.extend_from_slice(&timestamp.subsec_nanos().to_be_bytes());
        let headers = original.headers();
        put_count(&mut bytes, headers.len())?;
        for (key, value) in headers {
            put_text(&mut bytes, key)?;
            put_text(&mut bytes, value)?;
        }
        match original.ordering_key() {
            Some(key) => {
                bytes.push(1);
                put_text(&mut bytes, key)?;
            }
            None => bytes.push(0),
        }
        put_text(&mut bytes, original.payload())?;
        put_text(&mut bytes, value.subscriber_id().as_str())?;
        put_text(&mut bytes, value.reason())?;
        if bytes.len() > MAX_ENCODED_BYTES {
            return Err(encode_error("encoded dead letter exceeds fixture limit"));
        }
        Ok(Arc::from(bytes))
    }

    fn decode(&self, payload: &EncodedPayload) -> Result<DeadLetterEvent<String>, CodecError> {
        let bytes = payload.bytes();
        if bytes.len() > MAX_ENCODED_BYTES {
            return Err(decode_error("encoded dead letter exceeds fixture limit"));
        }
        let mut cursor = Cursor { bytes, offset: 0 };
        let event_id = EventId::new(cursor.text()?).map_err(decode_error)?;
        let topic_name = cursor.text()?.to_owned();
        let seconds = u64::from_be_bytes(cursor.array()?);
        let nanoseconds = u32::from_be_bytes(cursor.array()?);
        if nanoseconds >= 1_000_000_000 {
            return Err(decode_error("invalid timestamp nanoseconds"));
        }
        let timestamp = UNIX_EPOCH
            .checked_add(Duration::new(seconds, nanoseconds))
            .ok_or_else(|| decode_error("timestamp is out of range"))?;
        let header_count = cursor.u32()? as usize;
        if header_count > 1024 {
            return Err(decode_error("too many headers"));
        }
        let mut headers = Vec::with_capacity(header_count);
        for _ in 0..header_count {
            headers.push((cursor.text()?.to_owned(), cursor.text()?.to_owned()));
        }
        let ordering_key = match cursor.byte()? {
            0 => None,
            1 => Some(cursor.text()?.to_owned()),
            _ => return Err(decode_error("invalid ordering-key flag")),
        };
        let event_payload = cursor.text()?.to_owned();
        let subscriber_id = SubscriberId::new(cursor.text()?).map_err(decode_error)?;
        let reason = cursor.text()?.to_owned();
        if cursor.offset != bytes.len() {
            return Err(decode_error("trailing bytes after dead letter"));
        }
        let mut builder = PublishRequest::builder()
            .topic(Topic::<String>::new(&topic_name).map_err(decode_error)?)
            .payload(event_payload)
            .event_id(event_id)
            .timestamp(timestamp)
            .headers(headers);
        if let Some(key) = ordering_key {
            builder = builder.ordering_key(key);
        }
        let (original_event, _) = builder.build().map_err(decode_error)?.into_parts();
        Ok(DeadLetterEvent::new(
            Arc::new(original_event),
            subscriber_id,
            reason.into_boxed_str(),
        ))
    }
}

fn put_count(bytes: &mut Vec<u8>, value: usize) -> Result<(), CodecError> {
    let value = u32::try_from(value).map_err(decode_error)?;
    bytes.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn put_text(bytes: &mut Vec<u8>, value: &str) -> Result<(), CodecError> {
    put_count(bytes, value.len())?;
    bytes.extend_from_slice(value.as_bytes());
    Ok(())
}

fn decode_error(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> CodecError {
    CodecError::Decode { source: error.into() }
}

fn encode_error(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> CodecError {
    CodecError::Encode { source: error.into() }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl Cursor<'_> {
    fn byte(&mut self) -> Result<u8, CodecError> {
        let byte = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| decode_error("truncated byte"))?;
        self.offset += 1;
        Ok(byte)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], CodecError> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or_else(|| decode_error("length overflow"))?;
        let field = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| decode_error("truncated integer"))?;
        self.offset = end;
        field.try_into().map_err(decode_error)
    }

    fn u32(&mut self) -> Result<u32, CodecError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn text(&mut self) -> Result<&str, CodecError> {
        let length = self.u32()? as usize;
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| decode_error("length overflow"))?;
        let field = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| decode_error("truncated text"))?;
        self.offset = end;
        std::str::from_utf8(field).map_err(decode_error)
    }
}
