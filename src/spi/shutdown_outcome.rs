//! Result of closing an SPI backend.

/// Summary of provider shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ShutdownOutcome {
    /// Shutdown completed without abandoning known work.
    Complete,
    /// Shutdown completed after the grace period with work abandoned.
    TimedOut,
}
