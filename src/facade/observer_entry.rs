// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared ownership for synchronous and asynchronous diagnostic observers.

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use crate::internal::panic_payload::dispose_panic_payload;
use crate::pipeline::DiagnosticObserver;

/// Shared liveness and callback storage for a diagnostic observer.
pub(super) struct ObserverEntry {
    /// Whether later snapshots may select this callback.
    pub(super) active: AtomicBool,
    /// Thread-safe diagnostic callback shared by facade clones.
    pub(super) callback: Arc<DiagnosticObserver>,
}

impl ObserverEntry {
    /// Selects callbacks that are active after acquiring a strong ownership
    /// snapshot under the registry lock.
    ///
    /// The registry lock is released before liveness checks, callback cloning,
    /// and any resulting observer destruction.
    ///
    /// # Parameters
    /// - `registry`: weak entries owned by the bus observer registry.
    ///
    /// # Returns
    /// Callbacks selected for this dispatch and retained by strong references.
    pub(super) fn snapshot(registry: &Mutex<Vec<Weak<Self>>>) -> Vec<Arc<DiagnosticObserver>> {
        Self::active_callbacks(Self::snapshot_entries(registry))
    }

    /// Upgrades live observer entries while holding the registry lock.
    ///
    /// # Parameters
    /// - `registry`: weak observer entries whose live values are selected.
    ///
    /// # Returns
    /// Strong references to entries that were alive at the upgrade point.
    fn snapshot_entries(registry: &Mutex<Vec<Weak<Self>>>) -> Vec<Arc<Self>> {
        let mut entries = registry.lock().unwrap_or_else(PoisonError::into_inner);
        entries.retain(|entry| entry.strong_count() > 0);
        entries.iter().filter_map(Weak::upgrade).collect()
    }

    /// Clones callbacks whose shared liveness flag is active at snapshot time.
    ///
    /// # Parameters
    /// - `entries`: observer entries retained by a strong snapshot.
    ///
    /// # Returns
    /// Callback handles selected for dispatch. Later deactivation does not
    /// cancel callbacks already returned by this method.
    fn active_callbacks(entries: Vec<Arc<Self>>) -> Vec<Arc<DiagnosticObserver>> {
        let mut callbacks = Vec::new();
        for entry in entries {
            if entry.active.load(Ordering::Acquire) {
                callbacks.push(entry.callback.clone());
                drop(entry);
            } else if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(entry))) {
                dispose_panic_payload(payload);
            }
        }
        callbacks
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::Weak;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::thread;

    use crate::facade::AsyncEventBus;
    use crate::facade::DiagnosticObserverHandle;
    use crate::facade::EventBus;
    use crate::facade::observer_entry::ObserverEntry;
    use crate::local::LocalEventBusConfig;
    use crate::spi::ShutdownMode;

    struct ReentrantDrop {
        register: Box<dyn Fn() + Send + Sync>,
    }

    struct ReentrantPanicDrop {
        register: Box<dyn Fn() + Send + Sync>,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for ReentrantPanicDrop {
        fn drop(&mut self) {
            (self.register)();
            self.drops.fetch_add(1, Ordering::SeqCst);
            panic!("capture destructor panic");
        }
    }

    impl Drop for ReentrantDrop {
        fn drop(&mut self) {
            (self.register)();
        }
    }

    fn assert_snapshot_drop_reenters(
        observers: &Mutex<Vec<Weak<ObserverEntry>>>,
        register_original: impl FnOnce(ReentrantDrop, Arc<AtomicUsize>) -> DiagnosticObserverHandle,
        register_again: impl Fn() + Send + Sync + 'static,
    ) {
        let registered = Arc::new(AtomicUsize::new(0));
        let callback_calls = Arc::new(AtomicUsize::new(0));
        let drop_registered = registered.clone();
        let probe = ReentrantDrop {
            register: Box::new(move || {
                register_again();
                drop_registered.fetch_add(1, Ordering::SeqCst);
            }),
        };
        let callback_calls_clone = callback_calls.clone();
        let handle = register_original(probe, callback_calls_clone);
        let entries = ObserverEntry::snapshot_entries(observers);
        drop(handle);
        let callbacks = ObserverEntry::active_callbacks(entries);
        assert!(callbacks.is_empty(), "inactive observer must be excluded");
        assert_eq!(
            callback_calls.load(Ordering::SeqCst),
            0,
            "excluded callback must not run"
        );
        assert_eq!(
            registered.load(Ordering::SeqCst),
            1,
            "callback capture must drop outside registry lock"
        );
    }

    #[test]
    fn test_sync_snapshot_releases_inactive_capture_outside_registry_lock() {
        let bus = EventBus::local(LocalEventBusConfig::default()).expect("sync local bus");
        let bus_for_drop = bus.clone();
        assert_snapshot_drop_reenters(
            &bus.inner.observers,
            |probe, callback_calls| {
                bus.observe_diagnostics(move |_| {
                    let _probe = &probe;
                    callback_calls.fetch_add(1, Ordering::SeqCst);
                })
            },
            move || {
                let _handle = bus_for_drop.observe_diagnostics(|_| {});
            },
        );
        let _ = bus.shutdown(ShutdownMode::Immediate).expect("sync shutdown");
    }

    #[test]
    fn test_async_snapshot_releases_inactive_capture_outside_registry_lock() {
        let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::default())).expect("async local bus");
        let bus_for_drop = bus.clone();
        assert_snapshot_drop_reenters(
            &bus.inner.observers,
            |probe, callback_calls| {
                bus.observe_diagnostics(move |_| {
                    let _probe = &probe;
                    callback_calls.fetch_add(1, Ordering::SeqCst);
                })
            },
            move || {
                let _handle = bus_for_drop.observe_diagnostics(|_| {});
            },
        );
        let _ = block_on(bus.shutdown(ShutdownMode::Immediate)).expect("async shutdown");
    }

    #[test]
    fn test_snapshot_filter_contains_inactive_observer_capture_drop_panic() {
        let bus = EventBus::local(LocalEventBusConfig::default()).expect("sync local bus");
        let drops = Arc::new(AtomicUsize::new(0));
        let bus_for_drop = bus.clone();
        let probe = ReentrantPanicDrop {
            register: Box::new(move || {
                let _handle = bus_for_drop.observe_diagnostics(|_| {});
            }),
            drops: drops.clone(),
        };
        let inactive = bus.observe_diagnostics(move |_| {
            let _probe = &probe;
        });
        let calls = Arc::new(Mutex::new(Vec::new()));
        let first_calls = calls.clone();
        let first = bus.observe_diagnostics(move |_| {
            first_calls.lock().unwrap().push("first");
        });
        let second_calls = calls.clone();
        let second = bus.observe_diagnostics(move |_| {
            second_calls.lock().unwrap().push("second");
        });

        let entries = ObserverEntry::snapshot_entries(&bus.inner.observers);
        drop(inactive);
        let callbacks = ObserverEntry::active_callbacks(entries);
        let diagnostic = crate::pipeline::Diagnostic::InternalFailure {
            origin: "test".into(),
            message: "test".into(),
        };
        for callback in callbacks {
            callback(&diagnostic);
        }

        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(*calls.lock().unwrap(), ["first", "second"]);
        drop((first, second));
        let _ = bus.shutdown(ShutdownMode::Immediate).expect("sync shutdown");
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
        let mut context = Context::from_waker(&waker);
        let mut future = pin!(future);
        loop {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            thread::park();
        }
    }

    struct ThreadWake(thread::Thread);

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }
}
