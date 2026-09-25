// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Repeatable local SPI hot-path measurements without benchmark dependencies.

use std::any::TypeId;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;

use qubit_event_bus::local::LocalEventBusConfig;
use qubit_event_bus::local::LocalEventBusProvider;
use qubit_event_bus::model::AdmissionStatus;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderOptions;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::StartPosition;
use qubit_event_bus::model::SubscriberId;
use qubit_event_bus::model::SubscriptionDurability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::OrderingKey;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;
use qubit_spi::ServiceProvider;

const EVENTS: usize = 1000;
const WARMUPS: usize = 2;
const SAMPLES: usize = 7;
const DELAY: Duration = Duration::from_secs(3600);

/// Creates a fresh SPI with bounded queues; setup is outside every timed call.
fn create(capacity: usize) -> Arc<dyn EventBusSpi> {
    let config = qubit_event_bus::EventBusConfig::default()
        .with_provider_options(LocalEventBusConfig::new().queue_capacity(capacity).provider_options());
    LocalEventBusProvider.create_configured(&config).unwrap()
}

/// Creates a fixed, typed subscription request without starting a facade
/// worker.
fn request(id: u64, topic: &str) -> SpiSubscriptionRequest {
    SpiSubscriptionRequest::new(
        Id::new(id),
        TopicAddress::new(topic).unwrap(),
        SubscriberId::new(format!("subscriber-{id}")).unwrap(),
        None,
        SubscriptionDurability::Ephemeral,
        StartPosition::New,
        ProviderOptions::new(),
        TypeId::of::<u32>(),
    )
}

/// Builds one deterministic transport message outside the measured call.
fn outbound(topic: &str, id: usize, key: Option<&str>, delay: Option<Duration>) -> OutboundMessage {
    OutboundMessage::new(
        TopicAddress::new(topic).unwrap(),
        EventId::new(format!("event-{id}")).unwrap(),
        SystemTime::UNIX_EPOCH,
        Headers::new(),
        key.map(|key| OrderingKey::new(key).unwrap()),
        delay,
        TransportPayload::Native(Arc::new(id as u32)),
    )
}

/// Checks all destinations to fail the run if capacity or routing distorts a
/// sample.
fn assert_accepted(result: PublishAcknowledgement, expected: usize) {
    let PublishAcknowledgement::DestinationAdmissions(admissions) = result else {
        panic!("local provider returned an unexpected acknowledgement");
    };
    assert_eq!(admissions.len(), expected, "wrong destination count");
    assert!(
        admissions
            .iter()
            .all(|entry| matches!(entry.status(), AdmissionStatus::Accepted)),
        "local provider rejected or filtered a destination"
    );
}

/// Receives and accepts one queued event outside a publish measurement.
fn consume(receiver: &mut dyn EventSubscriptionSpi) {
    let ReceiveOutcome::Message(mut message) = receiver.receive(Duration::ZERO).unwrap() else {
        panic!("expected a ready message");
    };
    let token = message.take_settlement().expect("local message has a settlement token");
    receiver.settle(&token, DeliveryDisposition::Accept).unwrap();
}

/// Reduces independent operation timings to sum and nearest-rank p95.
fn summarize(mut durations: Vec<u64>) -> (u128, u64) {
    let total = durations.iter().map(|duration| u128::from(*duration)).sum();
    durations.sort_unstable();
    let rank = (durations.len() * 95).div_ceil(100);
    (total, durations[rank - 1])
}

/// Publishes prebuilt messages, timing only each SPI publish call.
fn publish_sample(topics: usize, subscribers: usize, target: usize) -> (u128, u64) {
    let bus = create(1);
    let mut receivers = Vec::with_capacity(topics * subscribers);
    for topic in 0..topics {
        let name = format!("topic-{topic}");
        for subscriber in 0..subscribers {
            receivers.push((
                topic,
                bus.subscribe(request((topic * subscribers + subscriber + 1) as u64, &name))
                    .unwrap(),
            ));
        }
    }
    let target_name = format!("topic-{target}");
    let messages = (0..EVENTS)
        .map(|id| outbound(&target_name, id, None, None))
        .collect::<Vec<_>>();
    let mut timings = Vec::with_capacity(EVENTS);
    for message in messages {
        let message = black_box(message);
        let started = Instant::now();
        let result = bus.publish(message);
        let elapsed = started.elapsed();
        assert_accepted(result.unwrap(), subscribers);
        timings.push(elapsed.as_nanos() as u64);
        for (topic, receiver) in &mut receivers {
            if *topic == target {
                consume(receiver.as_mut());
            }
        }
    }
    bus.shutdown(ShutdownMode::Immediate).unwrap();
    summarize(timings)
}

/// Times receive alone at a stable queue depth, then settles and refills.
fn receive_sample(depth: usize, ready_keys: usize) -> (u128, u64) {
    let bus = create(depth.max(1));
    let mut receiver = bus.subscribe(request(1, "receive-topic")).unwrap();
    if depth > 0 {
        let blocked_prefix = depth - ready_keys;
        assert_accepted(
            bus.publish(outbound("receive-topic", 0, Some("blocked"), Some(DELAY)))
                .unwrap(),
            1,
        );
        for id in 1..blocked_prefix {
            assert_accepted(
                bus.publish(outbound("receive-topic", id, Some("blocked"), None))
                    .unwrap(),
                1,
            );
        }
        for id in blocked_prefix..depth {
            let key = format!("ready-{}", id - blocked_prefix);
            assert_accepted(bus.publish(outbound("receive-topic", id, Some(&key), None)).unwrap(), 1);
        }
    }
    let mut timings = Vec::with_capacity(EVENTS);
    for id in 0..EVENTS {
        let started = Instant::now();
        let outcome = receiver.receive(Duration::ZERO);
        let elapsed = started.elapsed();
        timings.push(elapsed.as_nanos() as u64);
        if depth == 0 {
            assert!(matches!(outcome.unwrap(), ReceiveOutcome::TimedOut));
        } else {
            let ReceiveOutcome::Message(mut message) = outcome.unwrap() else {
                panic!("expected a ready message at depth {depth}");
            };
            let key = message
                .ordering_key()
                .expect("ready message has an ordering key")
                .as_str()
                .to_owned();
            assert!(
                key.strip_prefix("ready-")
                    .and_then(|suffix| suffix.parse::<usize>().ok())
                    .is_some_and(|index| index < ready_keys),
                "received a blocked or unknown ordering key"
            );
            let token = message.take_settlement().expect("local message has a settlement token");
            receiver.settle(&token, DeliveryDisposition::Accept).unwrap();
            assert_accepted(
                bus.publish(outbound("receive-topic", depth + id, Some(&key), None))
                    .unwrap(),
                1,
            );
        }
    }
    bus.shutdown(ShutdownMode::Immediate).unwrap();
    summarize(timings)
}

/// Times a full publish, receive, and settlement cycle as a diagnostic
/// scenario.
fn end_to_end_sample() -> (u128, u64) {
    let bus = create(1);
    let mut receiver = bus.subscribe(request(1, "end-to-end")).unwrap();
    let messages = (0..EVENTS)
        .map(|id| outbound("end-to-end", id, None, None))
        .collect::<Vec<_>>();
    let mut timings = Vec::with_capacity(EVENTS);
    for message in messages {
        let started = Instant::now();
        let result = bus.publish(black_box(message)).unwrap();
        assert_accepted(result, 1);
        consume(receiver.as_mut());
        timings.push(started.elapsed().as_nanos() as u64);
    }
    bus.shutdown(ShutdownMode::Immediate).unwrap();
    summarize(timings)
}

/// Runs warmups and seven fresh-bus samples, writing only CSV data to stdout.
fn run(name: &str, mut sample: impl FnMut() -> (u128, u64)) {
    for _ in 0..WARMUPS {
        black_box(sample());
    }
    for iteration in 1..=SAMPLES {
        let (elapsed_ns, p95_ns) = sample();
        println!("{name},{iteration},{EVENTS},{elapsed_ns},{p95_ns}");
    }
}

/// Selects publish, receive, or all scenarios; invalid input exits with usage.
fn main() {
    let mut args = std::env::args().skip(1).filter(|arg| arg != "--bench");
    let selection = args.next().unwrap_or_else(|| "all".to_owned());
    if !matches!(selection.as_str(), "publish" | "receive" | "all") || args.next().is_some() {
        eprintln!("usage: local_scale [publish|receive|all]");
        std::process::exit(2);
    }
    eprintln!(
        "available_parallelism={}",
        std::thread::available_parallelism().map_or(0, std::num::NonZeroUsize::get)
    );
    println!("scenario,iteration,events,elapsed_ns,p95_ns");
    if selection == "publish" || selection == "all" {
        for (topics, subscribers, target) in [(1, 1, 0), (1, 16, 0), (1, 128, 0), (32, 16, 0), (32, 16, 31)] {
            run(&format!("publish_t{topics}_s{subscribers}_target{target}"), || {
                publish_sample(topics, subscribers, target)
            });
        }
        run("end_to_end_t1_s1", end_to_end_sample);
    }
    if selection == "receive" || selection == "all" {
        for depth in [0, 512, 1024] {
            for ready_keys in [1, 16] {
                run(&format!("receive_depth{depth}_keys{ready_keys}"), || {
                    receive_sample(depth, ready_keys)
                });
            }
        }
    }
}
