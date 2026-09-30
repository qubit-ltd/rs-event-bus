// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures encoded facade publication and checks allocation reuse on retries.

use std::hint::black_box;
use std::io::Error;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::facade::EventBus;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishEffect;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::Topic;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::SubscriptionModes;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_retry::BackoffPolicy;
use qubit_retry::RetryPolicy;

const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const SIZES: [usize; 3] = [1_024, 65_536, 1_048_576];
const FAILURES: [usize; 3] = [0, 1, 3];

/// Counts payload encodes while preserving the supplied byte sequence.
struct BenchCodec {
    content_type: ContentType,
    calls: Arc<AtomicUsize>,
}

impl EventCodec<Vec<u8>> for BenchCodec {
    /// Returns the configured byte-stream media type.
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }

    /// Reports that the benchmark codec has no schema identifier.
    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }

    /// Copies one benchmark payload into shared encoded bytes.
    ///
    /// # Parameters
    /// - `value`: payload bytes to encode.
    ///
    /// # Returns
    /// Shared encoded bytes with the same content as `value`.
    ///
    /// # Errors
    /// This implementation does not fail while encoding.
    fn encode(&self, value: &Vec<u8>) -> Result<Arc<[u8]>, CodecError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::from(value.as_slice()))
    }

    /// Copies encoded bytes back into a benchmark payload.
    ///
    /// # Parameters
    /// - `payload`: encoded bytes and codec metadata.
    ///
    /// # Returns
    /// An owned vector containing the payload bytes.
    ///
    /// # Errors
    /// This implementation does not fail while copying bytes.
    fn decode(&self, payload: &EncodedPayload) -> Result<Vec<u8>, CodecError> {
        Ok(payload.bytes().to_vec())
    }
}

/// Records encoded allocation addresses and simulates retryable publish
/// failures.
struct BenchSpi {
    capabilities: EventBusCapabilities,
    failures_left: AtomicUsize,
    attempts: AtomicUsize,
    byte_addresses: Mutex<Vec<usize>>,
}

impl EventBusSpi for BenchSpi {
    /// Returns the fixed benchmark transport capabilities.
    fn capabilities(&self) -> EventBusCapabilities {
        self.capabilities
    }

    /// Records an encoded payload address and returns configured transient
    /// failures before accepting the message.
    ///
    /// # Parameters
    /// - `message`: outbound message being measured.
    ///
    /// # Returns
    /// An accepted acknowledgement after the configured failures.
    ///
    /// # Errors
    /// Returns a retryable synthetic error while failures remain.
    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        if let TransportPayload::Encoded(payload) = message.payload() {
            self.byte_addresses
                .lock()
                .unwrap()
                .push(payload.bytes().as_ptr() as usize);
        }
        if self
            .failures_left
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| left.checked_sub(1))
            .is_ok()
        {
            return Err(SpiError::Publish {
                provider_id: "bench".into(),
                resource: None,
                kind: "transient",
                retryable: Some(true),
                effect: PublishEffect::NotAccepted,
                source: Box::new(Error::other("synthetic retry")),
            });
        }
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        })
    }

    /// Rejects subscriptions because this benchmark only measures publish.
    ///
    /// # Parameters
    /// - `_`: unused subscription request.
    ///
    /// # Returns
    /// This method never returns a receiver.
    ///
    /// # Errors
    /// Always returns an unsupported-operation SPI error.
    fn subscribe(&self, _: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        Err(SpiError::Operation {
            provider_id: "bench".into(),
            operation: "subscribe",
            resource: None,
            kind: "unsupported",
            retryable: Some(false),
            source: Box::new(Error::other("benchmark SPI has no receiver")),
        })
    }

    /// Reports that the benchmark provider is immediately idle.
    ///
    /// # Parameters
    /// - `_`: unused topic and timeout.
    ///
    /// # Returns
    /// `Some(true)` because this provider does not retain work.
    ///
    /// # Errors
    /// This benchmark implementation does not fail.
    fn wait_for_topic_idle(&self, _: &TopicAddress, _: Option<Duration>) -> Result<Option<bool>, SpiError> {
        Ok(Some(true))
    }

    /// Completes benchmark provider shutdown immediately.
    ///
    /// # Parameters
    /// - `_`: unused shutdown mode.
    ///
    /// # Returns
    /// `ShutdownOutcome::Complete`.
    ///
    /// # Errors
    /// This benchmark implementation does not fail.
    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

/// Runs one measured publication and verifies retries reuse one encoded
/// allocation.
///
/// # Parameters
/// - `bytes`: Payload size encoded by the codec.
/// - `failures`: Number of retryable provider failures before acceptance.
///
/// # Returns
/// Elapsed nanoseconds, provider attempts, codec invocations, and
/// allocation-sharing result.
fn sample(bytes: usize, failures: usize) -> (u128, usize, usize, bool) {
    let codec_calls = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(BenchSpi {
        capabilities: EventBusCapabilities::new(
            PayloadModes::Encoded,
            SettlementCapabilities::None,
            OrderingCapability::None,
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            SubscriptionModes::EPHEMERAL,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        ),
        failures_left: AtomicUsize::new(failures),
        attempts: AtomicUsize::new(0),
        byte_addresses: Mutex::new(Vec::new()),
    });
    let bus = EventBus::from_spi(ProviderId::new("bench").unwrap(), provider.clone()).unwrap();
    let topic = Topic::new("bench.encoded")
        .unwrap()
        .with_shared_codec(Arc::new(BenchCodec {
            content_type: ContentType::APPLICATION_OCTET_STREAM,
            calls: codec_calls.clone(),
        }));
    let options = PublishOptions::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts((failures + 1) as u32)
                .backoff(BackoffPolicy::fixed(Duration::ZERO))
                .build()
                .unwrap(),
        )
        .build();
    let request = PublishRequest::new(topic, vec![0x5a; bytes])
        .unwrap()
        .with_options(options);
    let start = Instant::now();
    let _ = bus.publish(black_box(request)).unwrap();
    let elapsed = start.elapsed().as_nanos();
    let addresses = provider.byte_addresses.lock().unwrap();
    let attempts = provider.attempts.load(Ordering::Acquire);
    let encodes = codec_calls.load(Ordering::Acquire);
    assert_eq!(attempts, failures + 1);
    assert_eq!(encodes, 1);
    assert!(!addresses.is_empty());
    let shared = addresses.iter().all(|address| *address == addresses[0]);
    assert!(shared, "provider retries must share the encoded byte allocation");
    (elapsed, attempts, encodes, shared)
}

/// Selects a nearest-rank percentile from sorted duration samples.
///
/// # Parameters
/// - `sorted`: Durations sorted from smallest to largest.
/// - `percentile`: Requested percentile from 1 through 100.
///
/// # Returns
/// The duration at the nearest-rank index.
fn percentile(sorted: &[u128], percentile: usize) -> u128 {
    sorted[((sorted.len() * percentile).div_ceil(100)).saturating_sub(1)]
}

/// Prints aggregate statistics for one payload size and retry count.
///
/// # Parameters
/// - `bytes`: Payload size encoded by the codec.
/// - `failures`: Number of retryable provider failures before acceptance.
fn run(bytes: usize, failures: usize) {
    for _ in 0..WARMUPS {
        let _ = sample(bytes, failures);
    }
    let mut elapsed = Vec::with_capacity(SAMPLES);
    let mut attempts = 0;
    let mut encodes = 0;
    let mut shared = false;
    for _ in 0..SAMPLES {
        let (sample_ns, sample_attempts, sample_encodes, sample_shared) = sample(bytes, failures);
        elapsed.push(sample_ns);
        attempts = sample_attempts;
        encodes = sample_encodes;
        shared = sample_shared;
    }
    elapsed.sort_unstable();
    println!(
        "encoded_sync,{bytes},{failures},{attempts},{encodes},{shared},{},{}",
        percentile(&elapsed, 50),
        percentile(&elapsed, 95),
    );
}

fn main() {
    println!("scenario,bytes,failures,attempts,encodes,shared,median_ns,p95_ns");
    for bytes in SIZES {
        for failures in FAILURES {
            run(bytes, failures);
        }
    }
}
