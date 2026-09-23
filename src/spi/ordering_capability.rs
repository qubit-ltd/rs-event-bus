//! Provider ordering capabilities.

/// Strongest message ordering scope guaranteed by a provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum OrderingCapability {
    /// No ordering guarantee.
    None,
    /// Ordering is maintained per subscription.
    PerSubscription,
    /// Ordering is maintained for each key.
    PerKey,
    /// Ordering is maintained within each partition.
    PerPartition,
}
