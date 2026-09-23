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

impl DeliveryGap {
    /// Creates a provider-reported gap with an optional known missed-message count.
    pub fn new(reason: impl Into<Box<str>>, missed: Option<u64>) -> Self {
        Self {
            reason: reason.into(),
            missed,
        }
    }
}
