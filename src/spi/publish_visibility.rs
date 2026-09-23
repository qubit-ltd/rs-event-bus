//! Visibility of provider publish admission results.

/// Whether publication can report individual destination admissions.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PublishVisibility {
    /// Individual destination results are opaque.
    Opaque,
    /// Individual destination admissions are reported.
    DestinationAdmissions,
}
