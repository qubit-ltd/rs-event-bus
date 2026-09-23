//! Provider message durability capabilities.

/// Whether messages survive subscriber downtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum DurabilityCapability {
    /// Messages are ephemeral.
    Ephemeral,
    /// Messages are durably retained.
    Durable,
}
