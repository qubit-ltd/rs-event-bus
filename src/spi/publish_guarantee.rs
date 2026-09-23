//! Provider publish acknowledgement guarantees.

/// Strongest guarantee represented by successful provider publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PublishGuarantee {
    /// No provider acknowledgement is guaranteed.
    FireAndForget,
    /// The provider accepted the message.
    Accepted,
    /// The provider confirmed publication.
    Confirmed,
    /// The message was durably stored.
    DurablyStored,
}
