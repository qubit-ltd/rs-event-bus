//! Provider message settlement capabilities.

/// Settlement operations supported by a provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SettlementCapabilities {
    /// Messages cannot be settled.
    None,
    /// Only acknowledge/accept is supported.
    AcceptOnly,
    /// Accept, retry, and reject are supported.
    AcceptRetryReject,
}
