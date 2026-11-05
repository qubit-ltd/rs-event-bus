// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded shared history for subscription close failures.

use std::sync::Arc;

use crate::error::SubscriptionCloseErrors;
use crate::error::SubscriptionCloseFailure;

const MAX_RETAINED_CLOSE_FAILURES: usize = 256;

/// Bus-owned close failure history with a bounded set of detailed samples.
pub(in crate::facade) struct CloseFailureLedger {
    /// Detailed failures retained up to the configured sample limit.
    failures: Vec<Arc<SubscriptionCloseFailure>>,
    /// Saturating count of every failure recorded, including omitted samples.
    total_count: u64,
    /// Cached summary invalidated whenever another failure is recorded.
    snapshot: Option<Arc<SubscriptionCloseErrors>>,
}

impl CloseFailureLedger {
    /// Creates an empty close failure history.
    pub(in crate::facade) fn new() -> Self {
        Self {
            failures: Vec::new(),
            total_count: 0,
            snapshot: None,
        }
    }

    /// Records one canonical close failure and returns the stale snapshot.
    ///
    /// The caller must drop the returned snapshot after releasing its ledger
    /// mutex so retained error sources are never destroyed under that lock.
    ///
    /// # Parameters
    /// - `failure`: canonical close failure to count and retain when capacity
    ///   remains.
    ///
    /// # Returns
    /// The previous cached snapshot, which is stale after this update.
    pub(in crate::facade) fn record(
        &mut self,
        failure: &Arc<SubscriptionCloseFailure>,
    ) -> Option<Arc<SubscriptionCloseErrors>> {
        self.total_count = self.total_count.saturating_add(1);
        if self.failures.len() < MAX_RETAINED_CLOSE_FAILURES {
            self.failures.push(Arc::clone(failure));
        }
        self.snapshot.take()
    }

    /// Returns the cached bounded summary, or builds one when needed.
    pub(in crate::facade) fn snapshot(&mut self) -> Option<Arc<SubscriptionCloseErrors>> {
        if self.total_count == 0 {
            return None;
        }
        if let Some(snapshot) = &self.snapshot {
            return Some(snapshot.clone());
        }
        let snapshot = Arc::new(SubscriptionCloseErrors::from_summary(
            self.failures.clone(),
            self.total_count,
        ));
        self.snapshot = Some(snapshot.clone());
        Some(snapshot)
    }
}

impl Default for CloseFailureLedger {
    /// Creates an empty failure ledger.
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::fmt;
    use std::io;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use crate::error::SpiError;
    use crate::error::SubscriptionCloseFailure;
    use crate::facade::internal::close_failure_ledger::CloseFailureLedger;
    use crate::model::SubscriberId;

    struct DropProbe(Arc<AtomicUsize>);

    impl fmt::Display for DropProbe {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("drop probe")
        }
    }

    impl fmt::Debug for DropProbe {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("DropProbe")
        }
    }

    impl Error for DropProbe {}

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn failure(index: usize, source: Box<dyn Error + Send + Sync>) -> Arc<SubscriptionCloseFailure> {
        Arc::new(SubscriptionCloseFailure::new(
            SubscriberId::new(format!("subscriber-{index}")).expect("valid subscriber ID"),
            SpiError::Operation {
                provider_id: "ledger-test".into(),
                operation: "close",
                resource: None,
                kind: "close_failed",
                retryable: Some(false),
                source,
            },
        ))
    }

    #[test]
    fn test_ledger_retains_only_256_samples_and_saturates_count() {
        let mut ledger = CloseFailureLedger::new();
        assert!(ledger.snapshot().is_none());
        for index in 0..257 {
            let failure = failure(index, Box::new(io::Error::other("close failed")));
            let stale_snapshot = ledger.record(&failure);
            drop(stale_snapshot);
        }
        let snapshot = ledger.snapshot().expect("history has failures");
        assert_eq!(snapshot.len(), 256);
        assert_eq!(snapshot.total_count(), 257);
        assert_eq!(snapshot.omitted_count(), 1);
        assert!(snapshot.is_truncated());
        assert_eq!(snapshot.iter().count(), 256);

        ledger.total_count = u64::MAX;
        let failure = failure(258, Box::new(io::Error::other("close failed")));
        let stale_snapshot = ledger.record(&failure);
        drop(stale_snapshot);
        let saturated = ledger.snapshot().expect("history remains present");
        assert_eq!(saturated.total_count(), u64::MAX);
        assert_eq!(saturated.omitted_count(), u64::MAX - 256);
    }

    #[test]
    fn test_overflow_failure_source_is_not_retained() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut ledger = CloseFailureLedger::new();
        for index in 0..256 {
            let failure = failure(index, Box::new(io::Error::other("retained")));
            let stale_snapshot = ledger.record(&failure);
            drop(stale_snapshot);
        }
        let overflow = failure(256, Box::new(DropProbe(drops.clone())));
        let stale_snapshot = ledger.record(&overflow);
        drop(stale_snapshot);
        drop(overflow);
        assert_eq!(drops.load(Ordering::Acquire), 1);
        assert_eq!(ledger.snapshot().expect("history exists").total_count(), 257);
    }
}
