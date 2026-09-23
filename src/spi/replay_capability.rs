//! Provider replay capabilities.

/// Historical positions from which a provider can replay messages.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReplayCapability {
    /// Historical replay is unavailable.
    None,
    /// Replay can begin at a provider position.
    Position,
    /// Replay can begin at a timestamp.
    Timestamp,
}
