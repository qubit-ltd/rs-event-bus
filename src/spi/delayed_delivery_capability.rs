//! Provider delayed-delivery capabilities.

/// Whether the provider natively delays message visibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DelayedDeliveryCapability {
    /// No native delayed delivery.
    None,
    /// Native delayed delivery is supported.
    Native,
}
