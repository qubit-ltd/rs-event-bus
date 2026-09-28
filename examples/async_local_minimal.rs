// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

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
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::ShutdownMode;

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bus = block_on(AsyncEventBus::local(LocalEventBusConfig::default()))?;
    let topic = Topic::<String>::new("orders.created")?;
    let (sender, receiver) = mpsc::channel();
    let subscription = block_on(bus.subscribe(SubscribeRequest::new("audit", topic.clone())?))?;
    let runner = thread::spawn(move || {
        let mut subscription = subscription;
        block_on(subscription.run(move |delivery| {
            let _ = sender.send(delivery.payload().clone());
            async { Ok::<(), qubit_event_bus::DeliveryError>(()) }
        }))
    });

    block_on(bus.publish(PublishRequest::new(topic, "order-42".to_owned())?))?;
    assert_eq!(receiver.recv_timeout(Duration::from_secs(2))?, "order-42");
    block_on(bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(2),
    }))?;
    runner.join().expect("subscription runner should exit")?;
    Ok(())
}
