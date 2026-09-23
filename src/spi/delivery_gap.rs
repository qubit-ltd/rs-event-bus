//! Metadata describing messages missed by a receiver.

/// Provider-reported loss or omission in a subscription's observed stream.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryGap {
    /// Human-readable provider-neutral explanation.
    pub reason: Box<str>,
    /// Optional count of messages known to have been missed.
    pub missed: Option<u64>,
}
