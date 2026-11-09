// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::io;

/// Measurement lifecycle selected for an isolated local-threads sample.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Scenario {
    /// Cancel every subscription before timing bus shutdown.
    SyncCancelShutdown,
    /// Keep every subscription alive and uncancelled during bus shutdown.
    SyncDirectShutdown,
    /// Drive async receive runners while shutting down the bus.
    Async,
}

impl Scenario {
    /// Parses the child-process scenario selector.
    pub(crate) fn parse(arguments: &[String]) -> Result<Self, &'static str> {
        match arguments {
            [] => Ok(Self::SyncCancelShutdown),
            [mode] if mode == "--async" => Ok(Self::Async),
            [mode] if mode == "--direct-shutdown" => Ok(Self::SyncDirectShutdown),
            _ => Err("expected no mode, --async, or --direct-shutdown"),
        }
    }

    /// Returns the stable scenario label used in benchmark output.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::SyncCancelShutdown => "sync",
            Self::SyncDirectShutdown => "sync-direct-shutdown",
            Self::Async => "async",
        }
    }

    /// Identifies the measured shutdown boundary without changing legacy values.
    pub(crate) const fn duration_kind(self) -> &'static str {
        match self {
            Self::SyncCancelShutdown => "cancel_shutdown_ns",
            Self::SyncDirectShutdown => "shutdown_only_ns",
            Self::Async => "async_receive_shutdown_ns",
        }
    }
}

/// Reports whether a child sample is a successful measurement.
pub(crate) fn sample_succeeded(timed_out: bool, status: &str) -> bool {
    !timed_out && status == "success"
}

/// Runs every measured sample and reports whether the completed batch failed.
pub(crate) fn run_measured_batch<F>(samples: usize, mut sample: F) -> io::Result<()>
where
    F: FnMut(usize) -> io::Result<bool>,
{
    let mut failed = 0;
    for iteration in 1..=samples {
        if !sample(iteration)? {
            failed += 1;
        }
    }
    if failed == 0 {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{failed} of {samples} measured samples failed"
        )))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn scenario_routing_keeps_legacy_modes_and_adds_direct_shutdown() {
        use super::Scenario;
        assert_eq!(Scenario::parse(&[]), Ok(Scenario::SyncCancelShutdown));
        assert_eq!(Scenario::parse(&["--async".into()]), Ok(Scenario::Async));
        assert_eq!(
            Scenario::parse(&["--direct-shutdown".into()]),
            Ok(Scenario::SyncDirectShutdown)
        );
        assert_eq!(Scenario::SyncCancelShutdown.label(), "sync");
        assert_eq!(Scenario::SyncDirectShutdown.label(), "sync-direct-shutdown");
        assert_eq!(Scenario::Async.label(), "async");
        assert_eq!(Scenario::SyncDirectShutdown.duration_kind(), "shutdown_only_ns");
        assert_eq!(Scenario::SyncCancelShutdown.duration_kind(), "cancel_shutdown_ns");
    }

    #[test]
    fn unknown_or_conflicting_scenario_arguments_fail() {
        use super::Scenario;
        assert!(Scenario::parse(&["--mystery".into()]).is_err());
        assert!(Scenario::parse(&["--async".into(), "--direct-shutdown".into()]).is_err());
    }

    #[test]
    fn scenario_labels_and_units_keep_each_batch_distinct() {
        use super::Scenario;
        let rows = [
            (Scenario::SyncCancelShutdown, 16),
            (Scenario::SyncDirectShutdown, 16),
            (Scenario::Async, 16),
            (Scenario::SyncDirectShutdown, 64),
        ];
        assert_eq!(
            rows.iter()
                .filter(|(scenario, count)| *scenario == Scenario::SyncDirectShutdown && *count == 16)
                .count(),
            1
        );
        assert_ne!(
            Scenario::SyncDirectShutdown.duration_kind(),
            Scenario::SyncCancelShutdown.duration_kind()
        );
    }

    #[test]
    fn direct_shutdown_keeps_all_handles_until_return_then_marks_them_cancelled() {
        use qubit_event_bus::EventBus;
        use qubit_event_bus::model::SubscribeRequest;
        use qubit_event_bus::model::Topic;
        use qubit_event_bus::spi::ShutdownMode;

        let bus = EventBus::local(Default::default()).expect("local bus");
        let topic = Topic::<u32>::new("direct-shutdown-test").expect("topic");
        let subscriptions = (0..3)
            .map(|index| {
                let subscriber_id = format!("direct-{index}");
                bus.subscribe(
                    SubscribeRequest::new(&subscriber_id, topic.clone()).expect("request"),
                    |_| {},
                )
                .expect("subscription")
            })
            .collect::<Vec<_>>();

        assert!(subscriptions.iter().all(|subscription| !subscription.is_cancelled()));
        let report = bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
        assert_eq!(report.outcome, qubit_event_bus::spi::ShutdownOutcome::Complete);
        assert!(subscriptions.iter().all(|subscription| subscription.is_cancelled()));
    }

    #[test]
    fn legacy_sync_mode_cancels_handles_before_shutdown() {
        use qubit_event_bus::EventBus;
        use qubit_event_bus::model::SubscribeRequest;
        use qubit_event_bus::model::Topic;
        use qubit_event_bus::spi::ShutdownMode;

        let bus = EventBus::local(Default::default()).expect("local bus");
        let topic = Topic::<u32>::new("legacy-close-test").expect("topic");
        let subscription = bus
            .subscribe(SubscribeRequest::new("legacy-close", topic).expect("request"), |_| {})
            .expect("subscription");

        subscription.cancel().expect("cancel");
        assert!(subscription.is_cancelled());
        let report = bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
        assert_eq!(report.outcome, qubit_event_bus::spi::ShutdownOutcome::Complete);
        assert!(subscription.is_cancelled());
    }

    #[test]
    fn test_successful_samples_run_in_order() {
        let mut iterations = Vec::new();

        let result = super::run_measured_batch(7, |iteration| {
            iterations.push(iteration);
            Ok(true)
        });

        assert!(result.is_ok());
        assert_eq!(iterations, [1, 2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn test_failed_sample_does_not_stop_batch() {
        for failed_iteration in [1, 4, 7] {
            let mut iterations = Vec::new();
            let result = super::run_measured_batch(7, |iteration| {
                iterations.push(iteration);
                Ok(iteration != failed_iteration)
            });

            let error = result.expect_err("a failed measured sample must fail the batch");
            assert_eq!(error.kind(), std::io::ErrorKind::Other);
            assert!(error.to_string().contains("1 of 7 measured samples failed"));
            assert_eq!(iterations, [1, 2, 3, 4, 5, 6, 7]);
        }
    }

    #[test]
    fn test_sample_success_requires_non_timeout_success_status() {
        let cases = [
            (false, "success", true),
            (true, "timeout", false),
            (false, "panic", false),
            (false, "child_failed", false),
            (false, "subscribe_failed", false),
            (false, "receiver_failed", false),
        ];

        for (timed_out, status, expected) in cases {
            assert_eq!(
                super::sample_succeeded(timed_out, status),
                expected,
                "{timed_out}, {status}"
            );
        }
    }

    #[test]
    fn test_sample_io_error_propagates_and_stops_batch() {
        let mut iterations = Vec::new();
        let result = super::run_measured_batch(7, |iteration| {
            iterations.push(iteration);
            if iteration == 3 {
                Err(std::io::Error::from_raw_os_error(7))
            } else {
                Ok(true)
            }
        });

        let error = result.expect_err("sample I/O failure must propagate");
        assert_eq!(error.raw_os_error(), Some(7));
        assert_eq!(iterations, [1, 2, 3]);
    }
}
