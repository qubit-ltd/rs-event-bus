// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::future::Future;
use std::sync::Arc;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::DeliveryError;
use qubit_event_bus::WaitOutcome;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::AdmissionRequirement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::ShutdownMode as OperationShutdownMode;
use qubit_operation::ShutdownWait;

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        thread::park();
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let budget = Duration::from_secs(5);
    let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::default()))?;
    let topic = Topic::<String>::new("subscription.lifecycle")?;
    let (sender, receiver) = mpsc::channel();
    let subscription = block_on(bus.subscribe(SubscribeRequest::new("audit", topic.clone())?))?;
    let runner = thread::spawn(move || {
        block_on(subscription.run(move |delivery| {
            let _ = sender.send(delivery.payload().clone());
            async { Ok::<(), DeliveryError>(()) }
        }))
    });

    let receipt = block_on(bus.publish_checked(
        PublishRequest::new(topic.clone(), "event-1".to_owned())?,
        AdmissionRequirement::ProviderOrDestinationAccepted,
    ))?;
    receipt.check_admission(AdmissionRequirement::ProviderOrDestinationAccepted)?;
    assert_eq!(receiver.recv_timeout(budget)?, "event-1");
    assert!(matches!(
        block_on(bus.wait_for_received_deliveries(&topic, Some(budget)))?,
        WaitOutcome::Idle
    ));

    let manager = OperationManager::new(ManagerOptions::default())?;
    let closing = bus.clone();
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |_| {
            block_on(closing.shutdown(qubit_event_bus::spi::ShutdownMode::Graceful { timeout: budget }))
        })
        .map_err(|error| error.to_string())?;
    let report = match operation.wait_outcome()? {
        OperationOutcome::Succeeded(report) => report,
        OperationOutcome::Failed(error) => return Err(error.into()),
        other => return Err(std::io::Error::other(format!("unexpected operation outcome: {other:?}")).into()),
    };
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
    assert!(report.provider_may_have_abandoned_deliveries);
    runner.join().expect("subscription runner panicked")?;
    assert!(operation.wait_settlement()?.all_released());

    let shutdown = manager.request_shutdown(OperationShutdownMode::Drain);
    assert!(matches!(shutdown.wait_for(budget)?, ShutdownWait::Completed(_)));
    Ok(())
}
