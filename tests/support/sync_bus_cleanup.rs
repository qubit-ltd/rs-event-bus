use std::time::Duration;

use qubit_event_bus::error::ShutdownError;
use qubit_event_bus::facade::EventBus;
use qubit_event_bus::facade::ShutdownReport;
use qubit_event_bus::spi::ShutdownMode;

/// Ensures synchronous facade tests release controlled gates before shutdown.
pub(crate) struct SyncBusCleanup {
    bus: Option<EventBus>,
    releases: Vec<Box<dyn FnOnce() + Send>>,
}

impl SyncBusCleanup {
    pub(crate) fn new(bus: EventBus) -> Self {
        Self {
            bus: Some(bus),
            releases: Vec::new(),
        }
    }

    pub(crate) fn release_before_shutdown(&mut self, release: impl FnOnce() + Send + 'static) {
        self.releases.push(Box::new(release));
    }

    pub(crate) fn finish(mut self) -> Result<ShutdownReport, ShutdownError> {
        self.release_gates();
        self.bus
            .take()
            .expect("cleanup guard owns its bus until finish")
            .shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(2),
            })
    }

    fn release_gates(&mut self) {
        for release in self.releases.drain(..) {
            release();
        }
    }
}

impl Drop for SyncBusCleanup {
    fn drop(&mut self) {
        self.release_gates();
        if let Some(bus) = self.bus.take() {
            if let Err(error) = bus.shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(2),
            }) {
                eprintln!("best-effort sync test bus cleanup failed: {error}");
            }
        }
    }
}
