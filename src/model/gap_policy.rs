/// Controls whether a subscription continues after the provider reports a delivery gap.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GapPolicy {
    /// Stop receiving so the caller can recover or replace the subscription.
    #[default]
    Stop,
    /// Emit a diagnostic and continue receiving subsequent messages.
    Continue,
}
