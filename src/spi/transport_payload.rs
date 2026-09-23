//! Payload forms accepted by transport backends.

use super::EncodedPayload;
use std::any::Any;
use std::sync::Arc;

/// Type-erased native or encoded event payload.
#[non_exhaustive]
pub enum TransportPayload {
    /// In-process value that a facade can downcast to the topic payload type.
    Native(Arc<dyn Any + Send + Sync>),
    /// Serialized bytes and codec metadata for a portable transport.
    Encoded(EncodedPayload),
}
