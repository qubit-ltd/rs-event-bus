// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures repeated pending receive polls and real local delivery cycles.

use std::any::TypeId;
use std::future::Future;
use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::local::AsyncLocalEventBusSpi;
use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;

const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const OPERATIONS: usize = 20_000;

#[derive(Default)]
struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let spi = AsyncLocalEventBusSpi::new(&LocalEventBusConfig::new())?;
    let topic = TopicAddress::new("bench.async-receive-waiter")?;
    let mut receive = futures_free_receive(&spi, topic.clone()).await?;
    let first = Arc::new(CountWake::default());
    let second = Arc::new(CountWake::default());
    let first_waker = Waker::from(first.clone());
    let second_waker = Waker::from(second.clone());
    let mut first_context = Context::from_waker(&first_waker);
    let mut second_context = Context::from_waker(&second_waker);

    let mut pending = Box::pin(receive.receive(Duration::MAX));
    for (scenario, alternating) in [("same_waker", false), ("alternating_wakers", true)] {
        let mut samples = Vec::with_capacity(SAMPLES);
        for sample in 0..WARMUPS + SAMPLES {
            let started = Instant::now();
            for index in 0..OPERATIONS {
                let context = if alternating && index % 2 == 1 {
                    &mut second_context
                } else {
                    &mut first_context
                };
                assert!(pending.as_mut().poll(context).is_pending());
            }
            let elapsed = started.elapsed().as_nanos();
            if sample >= WARMUPS {
                samples.push(elapsed);
            }
        }
        println!(
            "scenario={scenario} operations_per_sample={OPERATIONS} samples_ns={samples:?} waker_callbacks={}",
            first.0.load(Ordering::Relaxed) + second.0.load(Ordering::Relaxed)
        );
    }
    drop(pending);

    let mut samples = Vec::with_capacity(SAMPLES);
    for sample in 0..WARMUPS + SAMPLES {
        let messages = (0..OPERATIONS)
            .map(|index| {
                Ok(OutboundMessage::new(
                    topic.clone(),
                    EventId::new(format!("event-{sample}-{index}"))?,
                    SystemTime::now(),
                    Headers::new(),
                    None,
                    None,
                    TransportPayload::Native(Arc::new(index)),
                ))
            })
            .collect::<Result<Vec<_>, ConfigurationError>>()?;
        let started = Instant::now();
        for event in messages {
            let mut pending = Box::pin(receive.receive(Duration::MAX));
            assert!(pending.as_mut().poll(&mut first_context).is_pending());
            let _acknowledgement = black_box(spi.publish(black_box(event)).await?);
            let outcome = pending.await?;
            let ReceiveOutcome::Message(mut message) = outcome else {
                return Err("publish did not produce a receive message".into());
            };
            let token = message
                .take_settlement()
                .ok_or("local receive did not provide settlement")?;
            receive.settle(&token, DeliveryDisposition::Accept).await?;
        }
        let elapsed = started.elapsed().as_nanos();
        if sample >= WARMUPS {
            samples.push(elapsed);
        }
    }
    println!(
        "scenario=publish_receive_settle operations_per_sample={OPERATIONS} samples_ns={samples:?} waker_callbacks={}",
        first.0.load(Ordering::Relaxed) + second.0.load(Ordering::Relaxed)
    );
    drop(receive);
    let _shutdown = black_box(futures_free_block_on(spi.shutdown(ShutdownMode::Immediate))?);
    Ok(())
}

async fn futures_free_receive(
    spi: &AsyncLocalEventBusSpi,
    topic: TopicAddress,
) -> Result<Box<dyn AsyncEventSubscriptionSpi>, Box<dyn std::error::Error>> {
    spi.subscribe(SpiSubscriptionRequest::new(
        Id::new(1),
        topic,
        SubscriberId::new("waiter-bench")?,
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::default(),
        TypeId::of::<usize>(),
    ))
    .await
    .map_err(Into::into)
}

fn futures_free_block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(CountWake::default()));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        std::thread::yield_now();
    }
}

fn main() {
    if let Err(error) = futures_free_block_on(run()) {
        eprintln!("async receive waiter benchmark failed: {error}");
        std::process::exit(1);
    }
}
