// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::EventBus;
use qubit_event_bus::NotificationOutcome;
use qubit_event_bus::NotificationPublisher;
use qubit_event_bus::TryPublishError;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::ShutdownMode as OperationShutdownMode;
use qubit_operation::ShutdownWait;

struct ObserverRelease(Option<mpsc::Sender<()>>);

impl ObserverRelease {
    fn release(&mut self) -> Result<(), mpsc::SendError<()>> {
        self.0.take().expect("observer release already used").send(())
    }
}

impl Drop for ObserverRelease {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bus = EventBus::local(Default::default())?;
    let topic = Topic::<String>::new("notification.lifecycle")?;
    let subscription = bus.subscribe(SubscribeRequest::new("observer", topic.clone())?, |_| {})?;
    let (seen_tx, seen_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(Some(release_rx));
    let publisher = Arc::new(NotificationPublisher::new(
        bus.clone(),
        topic,
        NonZeroUsize::new(2).expect("nonzero capacity"),
        move |outcome| {
            let first = seen_tx.send(outcome).is_ok();
            if first {
                if let Some(release) = release_rx.lock().expect("release lock").take() {
                    let _ = release.recv();
                }
            }
        },
    )?);
    // This is declared after `publisher`, so it releases the observer before
    // dropping the publisher if any assertion or operation below returns early.
    let mut observer_release = ObserverRelease(Some(release_tx));

    let manager = OperationManager::new(ManagerOptions::default())?;
    publisher.try_publish("first".to_owned())?;
    match seen_rx.recv_timeout(Duration::from_secs(5))? {
        NotificationOutcome::Published(receipt) => {
            receipt.check_admission(AdmissionRequirement::ProviderOrDestinationAccepted)?;
        }
        outcome => return Err(std::io::Error::other(format!("unexpected notification outcome: {outcome:?}")).into()),
    }
    publisher.try_publish("second".to_owned())?;

    let closing = Arc::clone(&publisher);
    let mut first = manager
        .try_submit(OperationOptions::default(), move |_| {
            closing.close_with_timeout(Duration::ZERO)
        })
        .map_err(|error| error.to_string())?;
    assert!(matches!(
        first.wait_outcome()?,
        OperationOutcome::Failed(error) if error.kind() == std::io::ErrorKind::TimedOut
    ));
    assert!(first.wait_settlement()?.all_released());
    assert!(
        matches!(publisher.try_publish("late".to_owned()), Err(TryPublishError::Closed(payload)) if payload == "late")
    );
    assert!(matches!(seen_rx.try_recv(), Err(mpsc::TryRecvError::Empty)));

    observer_release.release()?;
    let closing = Arc::clone(&publisher);
    let mut retry = manager
        .try_submit(OperationOptions::default(), move |_| {
            closing.close_with_timeout(Duration::from_secs(5))
        })
        .map_err(|error| error.to_string())?;
    assert!(matches!(retry.wait_outcome()?, OperationOutcome::Succeeded(())));
    assert!(retry.wait_settlement()?.all_released());
    match seen_rx.recv_timeout(Duration::from_secs(5))? {
        NotificationOutcome::Published(receipt) => {
            receipt.check_admission(AdmissionRequirement::ProviderOrDestinationAccepted)?;
        }
        outcome => return Err(std::io::Error::other(format!("unexpected notification outcome: {outcome:?}")).into()),
    }
    assert!(matches!(seen_rx.try_recv(), Err(mpsc::TryRecvError::Disconnected)));

    let direct = Arc::new(NotificationPublisher::new(
        bus.clone(),
        Topic::<String>::new("notification.lifecycle.direct")?,
        NonZeroUsize::new(1).expect("nonzero capacity"),
        |_| {},
    )?);
    manager.close_admission();
    let rejected = manager.try_submit(OperationOptions::default(), {
        let direct = Arc::clone(&direct);
        move |_| direct.close_with_timeout(Duration::from_secs(5))
    });
    assert!(rejected.is_err());
    direct.close_with_timeout(Duration::from_secs(5))?;

    subscription.cancel()?;
    let report = bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(5),
    })?;
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    let shutdown = manager.request_shutdown(OperationShutdownMode::Drain);
    assert!(matches!(
        shutdown.wait_for(Duration::from_secs(5))?,
        ShutdownWait::Completed(_)
    ));
    Ok(())
}
