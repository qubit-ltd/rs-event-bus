// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! A single cancelable asynchronous shutdown waker registration.
use std::task::Context;
use std::task::Poll;

use crate::facade::shutdown_coordinator::ShutdownCoordinator;
use crate::facade::shutdown_result::ShutdownResult;

/// Owns only a future's waker, leaving ticket/result ownership unchanged.
#[must_use = "the shutdown registration must stay alive while polling the ticket"]
pub(in crate::facade) struct ShutdownRegistration<'ticket> {
    /// Coordinator that owns the ticket result and registered wakers.
    coordinator: &'ticket ShutdownCoordinator,
    /// Exact ticket generation this observer is allowed to poll.
    generation: u64,
    /// Token for this observer's installed waker, absent before registration.
    token: Option<u64>,
}
impl<'ticket> ShutdownRegistration<'ticket> {
    /// Creates an unregistered observer for the ticket's exact generation.
    ///
    /// # Parameters
    /// - `coordinator`: coordinator that owns the ticket result and waker
    ///   registry.
    /// - `generation`: exact ticket generation this observer tracks.
    ///
    /// # Returns
    /// An unregistered guard that can install a waker when polled.
    #[inline]
    pub(in crate::facade) fn new(coordinator: &'ticket ShutdownCoordinator, generation: u64) -> Self {
        Self {
            coordinator,
            generation,
            token: None,
        }
    }
    /// Checks completion and atomically installs/updates this observer's waker.
    ///
    /// # Parameters
    /// - `cx`: task context whose waker is registered while the result is
    ///   pending.
    ///
    /// # Returns
    /// `Poll::Pending` while no result is ready, `Poll::Ready(Some(result))`
    /// when this generation has a shutdown result, or `Poll::Ready(None)`
    /// when the coordinator reports no result for this generation.
    pub(in crate::facade) fn poll(&mut self, cx: &Context<'_>) -> Poll<Option<ShutdownResult>> {
        self.coordinator.poll_result(self.generation, &mut self.token, cx)
    }
}
impl Drop for ShutdownRegistration<'_> {
    /// Unregisters this observer's waker when the future no longer needs it.
    fn drop(&mut self) {
        if let Some(token) = self.token {
            self.coordinator.unregister(self.generation, token);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Wake;
    use std::task::Waker;

    use super::ShutdownRegistration;
    use crate::facade::shutdown_coordinator::ShutdownCoordinator;
    use crate::spi::ShutdownMode;

    struct WakerLifetime {
        drops: Arc<AtomicUsize>,
        wakes: Arc<AtomicUsize>,
    }

    impl Drop for WakerLifetime {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl Wake for WakerLifetime {
        fn wake(self: Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_dropping_pending_registration_unregisters_its_waker() {
        let coordinator = ShutdownCoordinator::new();
        let (_, generation) = coordinator.begin(ShutdownMode::Immediate);
        let drops = Arc::new(AtomicUsize::new(0));
        let wakes = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(WakerLifetime {
            drops: Arc::clone(&drops),
            wakes: Arc::clone(&wakes),
        }));
        let mut registration = ShutdownRegistration::new(&coordinator, generation);
        let context = Context::from_waker(&waker);

        assert!(registration.poll(&context).is_pending());
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(wakes.load(Ordering::SeqCst), 0);

        drop(waker);
        drop(registration);

        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(wakes.load(Ordering::SeqCst), 0);
        coordinator.abort_start(generation, std::io::Error::other("test completion"));
        coordinator.release(generation);
    }
}
