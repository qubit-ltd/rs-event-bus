//! Payload representations accepted by a provider.

/// Payload representations a provider can send and receive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PayloadModes {
    /// Native Rust values only.
    Native,
    /// Encoded bytes only.
    Encoded,
    /// Both native values and encoded bytes.
    NativeAndEncoded,
}
