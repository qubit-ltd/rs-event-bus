// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures the cost of reported destination rejections through sync and async
//! facades.

use std::future::Future;
use std::hint::black_box;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread::yield_now;
use std::time::Instant;

use qubit_event_bus::AsyncEventBus;
use qubit_event_bus::EventBus;
use qubit_event_bus::SpiError;
use qubit_event_bus::model::AdmissionOutcome;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::AdmissionSummary;
use qubit_event_bus::model::DestinationAdmission;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;

const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const ITERATIONS: usize = 1_000;
const DESTINATIONS: [usize; 3] = [1, 16, 128];

fn capabilities() -> EventBusCapabilities {
    EventBusCapabilities::new(
        PayloadModes::Native,
        SettlementCapabilities::None,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Ephemeral,
        SubscriptionModes::EPHEMERAL,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::DestinationAdmissions,
    )
}

fn acknowledgement(destinations: usize, partial: bool) -> PublishAcknowledgement {
    let admissions = (0..destinations)
        .map(|index| {
            let status = if partial && index % 2 == 0 {
                AdmissionStatus::Accepted
            } else {
                AdmissionStatus::Rejected("capacity".into())
            };
            DestinationAdmission::new(
                qubit_id::Id::new(index as u64 + 1),
                SubscriberId::new(format!("subscriber-{index}")).expect("valid subscriber id"),
                status,
            )
        })
        .collect();
    PublishAcknowledgement::DestinationAdmissions(admissions)
}

struct SyncSpi(PublishAcknowledgement);

impl EventBusSpi for SyncSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        capabilities()
    }
    fn publish(&self, _: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        Ok(self.0.clone())
    }
    fn subscribe(
        &self,
        request: SpiSubscriptionRequest,
    ) -> Result<Box<dyn qubit_event_bus::spi::EventSubscriptionSpi>, SpiError> {
        Err(SpiError::Operation {
            provider_id: "rejection-bench".into(),
            operation: "subscribe",
            resource: Some(request.topic().as_str().into()),
            kind: "unsupported_test_operation",
            retryable: Some(false),
            source: Box::new(std::io::Error::other("benchmark does not subscribe")),
        })
    }
    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

struct AsyncSpi(PublishAcknowledgement);

impl AsyncEventBusSpi for AsyncSpi {
    fn capabilities(&self) -> EventBusCapabilities {
        capabilities()
    }
    fn publish<'a>(&'a self, _: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move { Ok(self.0.clone()) })
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn qubit_event_bus::spi::AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move {
            Err(SpiError::Operation {
                provider_id: "rejection-bench".into(),
                operation: "subscribe",
                resource: Some(request.topic().as_str().into()),
                kind: "unsupported_test_operation",
                retryable: Some(false),
                source: Box::new(std::io::Error::other("benchmark does not subscribe")),
            })
        })
    }
    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => yield_now(),
        }
    }
}

fn expected_counts(destinations: usize, partial: bool) -> (usize, usize) {
    if partial {
        (destinations.div_ceil(2), destinations / 2)
    } else {
        (0, destinations)
    }
}

fn run_sync(destinations: usize, partial: bool, observer_count: usize) -> Vec<u128> {
    let expected = acknowledgement(destinations, partial);
    let bus = EventBus::from_spi(
        ProviderId::new("rejection-bench").unwrap(),
        Arc::new(SyncSpi(expected.clone())),
    )
    .expect("valid capabilities");
    let observed = Arc::new(AtomicUsize::new(0));
    let _handles = (0..observer_count)
        .map(|_| {
            let observed = observed.clone();
            bus.observe_diagnostics(move |diagnostic| {
                black_box(diagnostic);
                observed.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect::<Vec<_>>();
    let topic = Topic::<u64>::new("bench.rejection").unwrap();
    let mut requests = (0..WARMUPS + SAMPLES)
        .map(|batch| {
            (0..ITERATIONS)
                .map(|value| PublishRequest::new(topic.clone(), (batch * ITERATIONS + value) as u64).unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let validation_request = PublishRequest::new(topic.clone(), u64::MAX).unwrap();
    let (accepted, rejected) = expected_counts(destinations, partial);
    let summary = AdmissionSummary {
        accepted,
        filtered: 0,
        rejected,
    };
    let expected_outcome = if accepted == 0 {
        AdmissionOutcome::NoneAccepted(summary)
    } else if rejected == 0 {
        AdmissionOutcome::Accepted(summary)
    } else {
        AdmissionOutcome::PartiallyAccepted(summary)
    };
    let validation = bus.publish(validation_request).expect("validation publish succeeds");
    assert_eq!(validation.admission_outcome(), expected_outcome);
    assert_eq!(observed.load(Ordering::Relaxed), observer_count * rejected);
    observed.store(0, Ordering::Relaxed);
    let mut samples = Vec::with_capacity(SAMPLES);
    for (batch, request_batch) in requests.iter_mut().enumerate() {
        observed.store(0, Ordering::Relaxed);
        let started = Instant::now();
        for request in std::mem::take(request_batch) {
            let receipt = bus.publish(black_box(request)).expect("publish succeeds");
            let _ = black_box(receipt.admission_outcome());
        }
        let elapsed = started.elapsed().as_nanos();
        assert_eq!(
            observed.load(Ordering::Relaxed),
            observer_count * rejected * ITERATIONS,
            "observer receives one diagnostic per rejected destination"
        );
        if batch >= WARMUPS {
            samples.push(elapsed);
        }
    }
    samples
}

fn run_async(destinations: usize, partial: bool, observer_count: usize) -> Vec<u128> {
    let expected = acknowledgement(destinations, partial);
    let bus = AsyncEventBus::from_spi(
        ProviderId::new("rejection-bench").unwrap(),
        Arc::new(AsyncSpi(expected.clone())),
    )
    .expect("valid capabilities");
    let observed = Arc::new(AtomicUsize::new(0));
    let _handles = (0..observer_count)
        .map(|_| {
            let observed = observed.clone();
            bus.observe_diagnostics(move |diagnostic| {
                black_box(diagnostic);
                observed.fetch_add(1, Ordering::Relaxed);
            })
        })
        .collect::<Vec<_>>();
    let topic = Topic::<u64>::new("bench.rejection").unwrap();
    let mut requests = (0..WARMUPS + SAMPLES)
        .map(|batch| {
            (0..ITERATIONS)
                .map(|value| PublishRequest::new(topic.clone(), (batch * ITERATIONS + value) as u64).unwrap())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let validation_request = PublishRequest::new(topic.clone(), u64::MAX).unwrap();
    let (accepted, rejected) = expected_counts(destinations, partial);
    let summary = AdmissionSummary {
        accepted,
        filtered: 0,
        rejected,
    };
    let expected_outcome = if accepted == 0 {
        AdmissionOutcome::NoneAccepted(summary)
    } else if rejected == 0 {
        AdmissionOutcome::Accepted(summary)
    } else {
        AdmissionOutcome::PartiallyAccepted(summary)
    };
    let validation = block_on(bus.publish(validation_request)).expect("validation publish succeeds");
    assert_eq!(validation.admission_outcome(), expected_outcome);
    assert_eq!(observed.load(Ordering::Relaxed), observer_count * rejected);
    observed.store(0, Ordering::Relaxed);
    let mut samples = Vec::with_capacity(SAMPLES);
    for (batch, request_batch) in requests.iter_mut().enumerate() {
        observed.store(0, Ordering::Relaxed);
        let started = Instant::now();
        for request in std::mem::take(request_batch) {
            let receipt = block_on(bus.publish(black_box(request))).expect("publish succeeds");
            let _ = black_box(receipt.admission_outcome());
        }
        let elapsed = started.elapsed().as_nanos();
        assert_eq!(observed.load(Ordering::Relaxed), observer_count * rejected * ITERATIONS);
        if batch >= WARMUPS {
            samples.push(elapsed);
        }
    }
    samples
}

fn emit(flavor: &str, destinations: usize, partial: bool, observer_count: usize, samples: Vec<u128>) {
    let rejected = expected_counts(destinations, partial).1;
    println!(
        "case={flavor} destinations={destinations} partial={partial} observers={observer_count} rejected={rejected} raw_sample_ns={samples:?} iterations={ITERATIONS} boundary=facade_publish_and_receipt_validation"
    );
}

fn main() {
    for destinations in DESTINATIONS {
        for partial in [false, true] {
            if destinations == 1 && partial {
                continue;
            }
            for observer_count in [0, 1] {
                emit(
                    "sync",
                    destinations,
                    partial,
                    observer_count,
                    run_sync(destinations, partial, observer_count),
                );
                emit(
                    "async",
                    destinations,
                    partial,
                    observer_count,
                    run_async(destinations, partial, observer_count),
                );
            }
        }
    }
}
