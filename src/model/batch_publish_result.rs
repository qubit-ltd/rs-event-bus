//! Ordered best-effort batch publication results.

use super::PublishAcknowledgement;
use super::PublishReceipt;
use crate::error::PublishError;

/// Each result corresponds to the request at the same input position.
pub struct BatchPublishResult {
    items: Vec<Result<PublishReceipt, PublishError>>,
}

impl BatchPublishResult {
    /// Preserves the exact input order of publication results.
    pub fn new(items: Vec<Result<PublishReceipt, PublishError>>) -> Self {
        Self { items }
    }
    /// Returns all per-request results in input order.
    pub fn items(&self) -> &[Result<PublishReceipt, PublishError>] {
        &self.items
    }
    /// Consumes the batch and returns its ordered results.
    pub fn into_items(self) -> Vec<Result<PublishReceipt, PublishError>> {
        self.items
    }
    /// Returns the number of input requests.
    pub fn total_count(&self) -> usize {
        self.items.len()
    }
    /// Counts publications accepted by a provider, including destination
    /// admissions.
    pub fn accepted_count(&self) -> usize {
        self.items
            .iter()
            .filter(|item| matches!(item, Ok(receipt) if !receipt.acknowledgement().is_dropped()))
            .count()
    }
    /// Counts events intentionally dropped by a publisher interceptor.
    pub fn dropped_count(&self) -> usize {
        self.items.iter().filter(|item| matches!(item, Ok(receipt) if matches!(receipt.acknowledgement(), PublishAcknowledgement::DroppedByInterceptor))).count()
    }
    /// Counts requests that failed before a successful publication receipt.
    pub fn failure_count(&self) -> usize {
        self.items.iter().filter(|item| item.is_err()).count()
    }
}
