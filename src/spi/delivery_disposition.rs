//! Terminal disposition for a received delivery.

/// Action applied to a delivery through its provider settlement token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DeliveryDisposition {
    /// Accept and remove/commit the delivery.
    Accept,
    /// Retry or release the delivery for later consumption.
    Retry,
    /// Reject the delivery without retry.
    Reject,
}
