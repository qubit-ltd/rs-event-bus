// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregated failures from closing facade-managed subscriptions.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use crate::error::SpiError;
use crate::model::SubscriberId;

/// One subscription close failure with its logical subscriber identity.
#[derive(Debug)]
pub struct SubscriptionCloseFailure {
    subscriber_id: SubscriberId,
    error: SpiError,
}

impl SubscriptionCloseFailure {
    /// Creates one ledger entry for a failed subscription close.
    pub(crate) fn new(subscriber_id: SubscriberId, error: SpiError) -> Self {
        Self { subscriber_id, error }
    }

    /// Returns the logical subscriber whose provider subscription failed to
    /// close.
    pub fn subscriber_id(&self) -> &SubscriberId {
        &self.subscriber_id
    }

    /// Returns the source-preserving provider error.
    pub fn error(&self) -> &SpiError {
        &self.error
    }
}

impl fmt::Display for SubscriptionCloseFailure {
    /// Formats the failed subscriber and its provider error.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "subscription {}: {}",
            self.subscriber_id.as_str(),
            self.error
        )
    }
}

impl Error for SubscriptionCloseFailure {
    /// Exposes the original provider close error as the source.
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.error)
    }
}

/// A stable, shareable collection of subscription close failures.
#[derive(Debug)]
pub struct SubscriptionCloseErrors {
    failures: Vec<Arc<SubscriptionCloseFailure>>,
}

impl SubscriptionCloseErrors {
    /// Returns the number of failed subscription closes.
    pub fn len(&self) -> usize {
        self.failures.len()
    }

    /// Returns whether no subscription close failed.
    pub fn is_empty(&self) -> bool {
        self.failures.is_empty()
    }

    /// Iterates over every failed subscription and its original provider error.
    pub fn iter(&self) -> impl Iterator<Item = &SubscriptionCloseFailure> {
        self.failures.iter().map(Arc::as_ref)
    }

    /// Builds an immutable close-error snapshot from the bus lifecycle ledger.
    pub(crate) fn from_failures(failures: Vec<Arc<SubscriptionCloseFailure>>) -> Self {
        Self { failures }
    }
}

impl fmt::Display for SubscriptionCloseErrors {
    /// Formats the total failure count and every affected subscriber.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} subscription close failure(s)", self.len())?;
        for failure in &self.failures {
            write!(formatter, "; {failure}")?;
        }
        Ok(())
    }
}

impl Error for SubscriptionCloseErrors {
    /// Exposes the first close failure while callers can inspect all via
    /// [`Self::iter`].
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.failures
            .first()
            .map(Arc::as_ref)
            .map(|failure| failure as &(dyn Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use super::SubscriptionCloseErrors;
    use super::SubscriptionCloseFailure;
    use crate::error::SpiError;
    use crate::model::SubscriberId;

    #[test]
    fn close_error_snapshot_exposes_each_failure_and_its_source() {
        let empty = SubscriptionCloseErrors::from_failures(Vec::new());
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.to_string(), "0 subscription close failure(s)");
        assert!(empty.source().is_none());

        let failure = Arc::new(SubscriptionCloseFailure::new(
            SubscriberId::new("worker-1").expect("valid subscriber ID"),
            SpiError::Operation {
                provider_id: "test-provider".into(),
                operation: "close_subscription",
                resource: Some("worker-1".into()),
                kind: "close_failed",
                retryable: Some(false),
                source: Box::new(std::io::Error::other("close failed")),
            },
        ));
        let errors = SubscriptionCloseErrors::from_failures(vec![failure]);
        assert!(!errors.is_empty());
        assert_eq!(errors.len(), 1);
        let failure = errors.iter().next().expect("one close failure");
        assert_eq!(failure.subscriber_id().as_str(), "worker-1");
        assert_eq!(failure.error().kind(), "close_failed");
        assert!(failure.to_string().contains("subscription worker-1:"));
        assert_eq!(
            failure.source().unwrap().to_string(),
            "provider test-provider failed close_subscription (close_failed): close failed"
        );
        assert!(
            errors
                .to_string()
                .contains("1 subscription close failure(s); subscription worker-1:")
        );
        assert_eq!(errors.source().unwrap().to_string(), failure.to_string());
    }
}
