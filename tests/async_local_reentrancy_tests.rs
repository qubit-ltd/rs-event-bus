// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::env::var;
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;

mod support;

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("probe operation unexpectedly pending"),
    }
}

struct Reenter {
    bus: AsyncEventBus,
    calls: AtomicUsize,
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let topic = Topic::<String>::new("review.reentrant").unwrap();
        let request = PublishRequest::new(topic, "after-close".to_owned()).unwrap();
        assert!(ready(self.bus.publish(request)).is_err());
    }
}

#[test]
fn test_shutdown_waker_can_reenter_publish() {
    let test_name = "test_shutdown_waker_can_reenter_publish";
    let Ok(case) = var("QUBIT_EVENT_BUS_ISOLATED_CASE") else {
        for case in ["immediate", "graceful"] {
            support::isolated_process::run_case(test_name, case);
        }
        return;
    };
    let spi = Arc::new(AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new()).unwrap());
    let bus = AsyncEventBus::from_spi(ProviderId::new("local").unwrap(), spi.clone()).unwrap();
    let topic = Topic::<String>::new("review.reentrant").unwrap();
    let mut subscription = ready(bus.subscribe(SubscribeRequest::new("consumer", topic).unwrap())).unwrap();
    let reenter = Arc::new(Reenter {
        bus: bus.clone(),
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(reenter.clone());
    let mut runner = Box::pin(subscription.run(|_| async { Ok(()) }));
    assert!(runner.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    let mode = match case.as_str() {
        "immediate" => ShutdownMode::Immediate,
        "graceful" => ShutdownMode::Graceful {
            timeout: Duration::from_secs(1),
        },
        _ => panic!("unknown isolated case"),
    };
    assert_eq!(ShutdownOutcome::Complete, ready(spi.shutdown(mode)).unwrap());
    assert!(reenter.calls.load(Ordering::SeqCst) >= 1);
    drop(runner);
    let report = ready(bus.shutdown(ShutdownMode::Immediate)).unwrap();
    assert_eq!(report.outcome, ShutdownOutcome::Complete);
    assert_eq!(report.known_abandoned_deliveries, 0);
    assert!(report.provider_may_have_abandoned_deliveries);
}
