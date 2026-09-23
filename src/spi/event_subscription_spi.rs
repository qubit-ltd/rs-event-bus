//! Synchronous single-owner subscription contract.

use std::time::Duration;

use crate::error::SpiError;

use super::DeliveryDisposition;
use super::ReceiveOutcome;
use super::SettlementToken;

/// A backend receiver consumed by exactly one owner.
pub trait EventSubscriptionSpi: Send + 'static {
    /// Receives one message, gap, timeout, or closed outcome.
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError>;

    /// Applies a terminal disposition to a provider-issued settlement token.
    fn settle(
        &mut self,
        token: SettlementToken,
        disposition: DeliveryDisposition,
    ) -> Result<(), SpiError>;

    /// Closes this receiver and releases its resources.
    fn close(&mut self) -> Result<(), SpiError>;
}
