// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::time::Duration;

use qubit_event_bus::codec::EventCodec;
use qubit_event_bus::error::CapabilityError;
use qubit_event_bus::error::CodecError;
use qubit_event_bus::error::ConfigurationError;
use qubit_event_bus::error::DeliveryAttemptError;
use qubit_event_bus::error::DeliveryError;
use qubit_event_bus::error::LifecycleError;
use qubit_event_bus::error::PublishAttemptError;
use qubit_event_bus::error::PublishError;
use qubit_event_bus::error::ShutdownError;
use qubit_event_bus::error::SpiError;
use qubit_event_bus::error::SubscribeError;
use qubit_event_bus::facade::EventBus;
use qubit_event_bus::facade::EventBusFacadeConfig;
use qubit_event_bus::facade::Subscription;
use qubit_event_bus::facade::SyncDeliverySchedulerConfig;
use qubit_event_bus::model::AckMode;
use qubit_event_bus::model::AsyncSubscriberNext;
use qubit_event_bus::model::ContentType;
use qubit_event_bus::model::DeadLetterPolicy;
use qubit_event_bus::model::Delivery;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::FailureDirective;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::OrderingPolicy;
use qubit_event_bus::model::ProviderId;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::model::PublishOptions;
use qubit_event_bus::model::PublishRequest;
use qubit_event_bus::model::SchemaId;
use qubit_event_bus::model::SubscribeOptions;
use qubit_event_bus::model::SubscribeRequest;
use qubit_event_bus::model::SubscriberNext;
use qubit_event_bus::model::Topic;
use qubit_event_bus::pipeline::Diagnostic;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DeliveryDisposition;
use qubit_event_bus::spi::DurabilityCapability;
use qubit_event_bus::spi::EncodedPayload;
use qubit_event_bus::spi::EventBusCapabilities;
use qubit_event_bus::spi::EventBusSpi;
use qubit_event_bus::spi::EventSubscriptionSpi;
use qubit_event_bus::spi::InboundMessage;
use qubit_event_bus::spi::OrderingCapability;
use qubit_event_bus::spi::OutboundMessage;
use qubit_event_bus::spi::PayloadModes;
use qubit_event_bus::spi::PublishGuarantee;
use qubit_event_bus::spi::PublishVisibility;
use qubit_event_bus::spi::ReceiveOutcome;
use qubit_event_bus::spi::ReplayCapability;
use qubit_event_bus::spi::SettlementCapabilities;
use qubit_event_bus::spi::SettlementToken;
use qubit_event_bus::spi::ShutdownMode;
use qubit_event_bus::spi::ShutdownOutcome;
use qubit_event_bus::spi::SpiFuture;
use qubit_event_bus::spi::SpiSubscriptionRequest;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_id::Id;
use qubit_retry::AttemptFailure;
use qubit_retry::BackoffPolicy;
use qubit_retry::RetryCancellationToken;
use qubit_retry::RetryContext;
use qubit_retry::RetryDecision;
use qubit_retry::RetryErrorReason;
use qubit_retry::RetryPolicy;

type SharedQueue = Arc<(Mutex<QueueState>, Condvar)>;

struct ReceiveGate {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

struct SpiCallGate {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}

#[derive(Default)]
struct QueueState {
    messages: VecDeque<InboundMessage>,
    closed: bool,
}

#[derive(Default)]
struct TestBackendState {
    queues: Vec<(Id, SharedQueue)>,
    shutdown_calls: usize,
    shutdown_modes: Vec<ShutdownMode>,
    close_calls: usize,
    settlement_calls: usize,
    settlement_dispositions: Vec<DeliveryDisposition>,
    fail_next_settle: bool,
    fail_settle_always: bool,
    published_topics: Vec<Box<str>>,
}

struct TestBackend {
    state: Arc<Mutex<TestBackendState>>,
    shutdown: AtomicBool,
    publish_calls: AtomicUsize,
    fail_publish_call: AtomicUsize,
    settlement_capability: std::sync::atomic::AtomicUsize,
    ordering_capability: AtomicUsize,
    subscribe_calls: AtomicUsize,
    close_delay_ms: Arc<AtomicUsize>,
    close_panics: Arc<AtomicBool>,
    close_fails: Arc<AtomicBool>,
    receive_gate: Arc<Mutex<Option<ReceiveGate>>>,
    publish_gate: Arc<Mutex<Option<SpiCallGate>>>,
    subscribe_gate: Arc<Mutex<Option<SpiCallGate>>>,
    close_gate: Arc<Mutex<Option<SpiCallGate>>>,
    shutdown_gate: Arc<Mutex<Option<SpiCallGate>>>,
    received_messages: Arc<AtomicUsize>,
}

impl TestBackend {
    fn new() -> Self {
        Self {
            state: Arc::default(),
            shutdown: AtomicBool::new(false),
            publish_calls: AtomicUsize::new(0),
            fail_publish_call: AtomicUsize::new(0),
            settlement_capability: std::sync::atomic::AtomicUsize::new(2),
            ordering_capability: AtomicUsize::new(3),
            subscribe_calls: AtomicUsize::new(0),
            close_delay_ms: Arc::new(AtomicUsize::new(0)),
            close_panics: Arc::new(AtomicBool::new(false)),
            close_fails: Arc::new(AtomicBool::new(false)),
            receive_gate: Arc::new(Mutex::new(None)),
            publish_gate: Arc::default(),
            subscribe_gate: Arc::default(),
            close_gate: Arc::default(),
            shutdown_gate: Arc::default(),
            received_messages: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn publish_calls(&self) -> usize {
        self.publish_calls.load(Ordering::Acquire)
    }

    fn fail_publish_call(&self, call: usize) {
        self.fail_publish_call.store(call, Ordering::Release);
    }

    fn set_settlement_capability(&self, capability: SettlementCapabilities) {
        self.settlement_capability.store(
            match capability {
                SettlementCapabilities::None => 0,
                SettlementCapabilities::AcceptOnly => 1,
                SettlementCapabilities::AcceptRetryReject => 2,
                _ => 2,
            },
            Ordering::Release,
        );
    }

    fn set_ordering_capability(&self, capability: OrderingCapability) {
        let value = match capability {
            OrderingCapability::None => 0,
            OrderingCapability::PerPartition => 1,
            OrderingCapability::PerKey => 2,
            OrderingCapability::PerSubscription => 3,
            _ => unreachable!("test only uses known ordering capabilities"),
        };
        self.ordering_capability.store(value, Ordering::Release);
    }

    fn set_close_delay(&self, delay: Duration) {
        self.close_delay_ms.store(delay.as_millis() as usize, Ordering::Release);
    }

    fn set_close_panics(&self, value: bool) {
        self.close_panics.store(value, Ordering::Release);
    }

    fn set_close_fails(&self, value: bool) {
        self.close_fails.store(value, Ordering::Release);
    }

    fn gate_next_receive(&self) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *self.receive_gate.lock().expect("receive gate lock") = Some(ReceiveGate {
            entered: entered_tx,
            release: release_rx,
        });
        (entered_rx, release_tx)
    }

    fn gate_next_publish(&self) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *self.publish_gate.lock().expect("publish gate lock") = Some(SpiCallGate {
            entered: entered_tx,
            release: release_rx,
        });
        (entered_rx, release_tx)
    }

    fn gate_next_subscribe(&self) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *self.subscribe_gate.lock().expect("subscribe gate lock") = Some(SpiCallGate {
            entered: entered_tx,
            release: release_rx,
        });
        (entered_rx, release_tx)
    }

    fn gate_next_close(&self) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *self.close_gate.lock().expect("close gate lock") = Some(SpiCallGate {
            entered: entered_tx,
            release: release_rx,
        });
        (entered_rx, release_tx)
    }

    fn gate_next_shutdown(&self) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        *self.shutdown_gate.lock().expect("shutdown gate lock") = Some(SpiCallGate {
            entered: entered_tx,
            release: release_rx,
        });
        (entered_rx, release_tx)
    }

    fn wait_at_gate(gate: &Mutex<Option<SpiCallGate>>, name: &str) {
        let gate = gate.lock().expect("SPI call gate lock").take();
        if let Some(gate) = gate {
            gate.entered.send(()).expect("test receiver remains alive");
            gate.release.recv().unwrap_or_else(|_| panic!("release gated {name}"));
        }
    }

    fn shutdown_calls(&self) -> usize {
        self.state.lock().expect("test state lock").shutdown_calls
    }

    fn shutdown_modes(&self) -> Vec<ShutdownMode> {
        self.state.lock().expect("test state lock").shutdown_modes.clone()
    }

    fn close_calls(&self) -> usize {
        self.state.lock().expect("test state lock").close_calls
    }

    fn received_messages(&self) -> usize {
        self.received_messages.load(Ordering::Acquire)
    }

    fn close_receivers(&self) {
        let queues = self.state.lock().expect("test state lock").queues.clone();
        for (_, queue) in queues {
            let (lock, ready) = &*queue;
            lock.lock().expect("queue lock").closed = true;
            ready.notify_all();
        }
    }

    fn settlement_calls(&self) -> usize {
        self.state.lock().expect("test state lock").settlement_calls
    }

    fn fail_next_settle(&self) {
        self.state.lock().expect("test state lock").fail_next_settle = true;
    }

    fn fail_all_settles(&self) {
        self.state.lock().expect("test state lock").fail_settle_always = true;
    }

    fn settlement_dispositions(&self) -> Vec<DeliveryDisposition> {
        self.state
            .lock()
            .expect("test state lock")
            .settlement_dispositions
            .clone()
    }

    fn published_topics(&self) -> Vec<Box<str>> {
        self.state.lock().expect("test state lock").published_topics.clone()
    }

    fn enqueue_marked(&self, subscription_id: Id) {
        let queues = self.state.lock().expect("test state lock").queues.clone();
        let (_, queue) = queues
            .into_iter()
            .find(|(id, _)| *id == subscription_id)
            .expect("subscription queue");
        let mut headers = Headers::new();
        headers.insert("x-qubit-event-bus-dead-letter".into(), "v1".into());
        let message = InboundMessage::new(
            TopicAddress::new("sync.events").expect("valid topic"),
            EventId::new("marked-event").expect("valid event ID"),
            std::time::SystemTime::UNIX_EPOCH,
            headers,
            None,
            TransportPayload::Native(Arc::new(String::from("marked"))),
            Some(SettlementToken::new(subscription_id, "marked-dead-letter")),
            Default::default(),
        );
        let (lock, ready) = &*queue;
        lock.lock().expect("queue lock").messages.push_back(message);
        ready.notify_one();
    }

    fn enqueue_encoded(&self, payload: EncodedPayload) {
        let queues = self.state.lock().expect("test state lock").queues.clone();
        for (subscription_id, queue) in queues {
            let encoded = EncodedPayload::new(
                Arc::<[u8]>::from(payload.bytes()),
                payload.content_type().clone(),
                payload.schema_id().cloned(),
            );
            let message = InboundMessage::new(
                TopicAddress::new("sync.events").expect("valid topic"),
                EventId::new("codec-panic-event").expect("valid event ID"),
                std::time::SystemTime::UNIX_EPOCH,
                Headers::new(),
                None,
                TransportPayload::Encoded(encoded),
                Some(SettlementToken::new(subscription_id, "codec-panic-token")),
                Default::default(),
            );
            let (lock, ready) = &*queue;
            lock.lock().expect("queue lock").messages.push_back(message);
            ready.notify_one();
        }
    }
}

impl EventBusSpi for TestBackend {
    fn capabilities(&self) -> EventBusCapabilities {
        EventBusCapabilities::new(
            PayloadModes::Native,
            match self.settlement_capability.load(Ordering::Acquire) {
                0 => SettlementCapabilities::None,
                1 => SettlementCapabilities::AcceptOnly,
                _ => SettlementCapabilities::AcceptRetryReject,
            },
            match self.ordering_capability.load(Ordering::Acquire) {
                0 => OrderingCapability::None,
                1 => OrderingCapability::PerPartition,
                2 => OrderingCapability::PerKey,
                _ => OrderingCapability::PerSubscription,
            },
            DelayedDeliveryCapability::None,
            DurabilityCapability::Ephemeral,
            false,
            ReplayCapability::None,
            PublishGuarantee::Accepted,
            PublishVisibility::Opaque,
        )
    }

    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        let call = self.publish_calls.fetch_add(1, Ordering::AcqRel) + 1;
        Self::wait_at_gate(&self.publish_gate, "publish");
        if self.fail_publish_call.load(Ordering::Acquire) == call {
            return Err(test_spi_error("publish"));
        }
        let state = self.state.lock().expect("test state lock");
        drop(state);
        self.state
            .lock()
            .expect("test state lock")
            .published_topics
            .push(message.topic().as_str().into());
        let state = self.state.lock().expect("test state lock");
        for (subscription_id, queue) in &state.queues {
            let settlement = match self.settlement_capability.load(Ordering::Acquire) {
                0 => None,
                _ => Some(SettlementToken::new(*subscription_id, "test-token")),
            };
            let inbound = InboundMessage::new(
                message.topic().clone(),
                message.id().clone(),
                message.timestamp(),
                message.headers().clone(),
                message.ordering_key().cloned(),
                clone_transport_payload(message.payload()),
                settlement,
                Default::default(),
            );
            let (lock, ready) = &**queue;
            lock.lock().expect("queue lock").messages.push_back(inbound);
            ready.notify_one();
        }
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        })
    }

    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        self.subscribe_calls.fetch_add(1, Ordering::AcqRel);
        Self::wait_at_gate(&self.subscribe_gate, "subscribe");
        let queue = Arc::new((Mutex::new(QueueState::default()), Condvar::new()));
        self.state
            .lock()
            .expect("test state lock")
            .queues
            .push((request.subscription_id(), queue.clone()));
        Ok(Box::new(TestSubscription {
            id: request.subscription_id(),
            queue,
            backend_state: self.state.clone(),
            close_delay_ms: self.close_delay_ms.clone(),
            close_panics: self.close_panics.clone(),
            close_fails: self.close_fails.clone(),
            receive_gate: self.receive_gate.clone(),
            received_messages: self.received_messages.clone(),
            close_gate: self.close_gate.clone(),
        }))
    }

    fn shutdown(&self, mode: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Self::wait_at_gate(&self.shutdown_gate, "shutdown");
        self.shutdown.store(true, Ordering::Release);
        let mut state = self.state.lock().expect("test state lock");
        state.shutdown_calls += 1;
        state.shutdown_modes.push(mode);
        Ok(ShutdownOutcome::Complete)
    }
}

fn clone_transport_payload(payload: &TransportPayload) -> TransportPayload {
    match payload {
        TransportPayload::Native(value) => TransportPayload::Native(value.clone()),
        TransportPayload::Encoded(_) => panic!("native-only test provider received encoded data"),
        _ => panic!("test provider received an unsupported payload mode"),
    }
}

struct TestSubscription {
    id: Id,
    queue: SharedQueue,
    backend_state: Arc<Mutex<TestBackendState>>,
    close_delay_ms: Arc<AtomicUsize>,
    close_panics: Arc<AtomicBool>,
    close_fails: Arc<AtomicBool>,
    receive_gate: Arc<Mutex<Option<ReceiveGate>>>,
    received_messages: Arc<AtomicUsize>,
    close_gate: Arc<Mutex<Option<SpiCallGate>>>,
}

impl EventSubscriptionSpi for TestSubscription {
    fn receive(&mut self, timeout: Duration) -> Result<ReceiveOutcome, SpiError> {
        let (lock, ready) = &*self.queue;
        let mut state = lock.lock().expect("queue lock");
        while state.messages.is_empty() && !state.closed && !timeout.is_zero() {
            let (next, timed_out) = ready.wait_timeout(state, timeout).expect("queue lock while waiting");
            state = next;
            if timed_out.timed_out() {
                break;
            }
        }
        if let Some(message) = state.messages.pop_front() {
            self.received_messages.fetch_add(1, Ordering::AcqRel);
            drop(state);
            let gate = self.receive_gate.lock().expect("receive gate lock").take();
            if let Some(gate) = gate {
                gate.entered.send(()).expect("test receiver remains alive");
                gate.release.recv().expect("release gated receive");
            }
            return Ok(ReceiveOutcome::Message(message));
        }
        if state.closed {
            return Ok(ReceiveOutcome::Closed);
        }
        Ok(ReceiveOutcome::TimedOut)
    }

    fn settle(&mut self, token: &SettlementToken, disposition: DeliveryDisposition) -> Result<(), SpiError> {
        if !token.belongs_to(self.id) {
            return Err(test_spi_error("settle"));
        }
        let mut state = self.backend_state.lock().expect("test state lock");
        state.settlement_calls += 1;
        if state.fail_settle_always || std::mem::take(&mut state.fail_next_settle) {
            return Err(test_spi_error("settle"));
        }
        state.settlement_dispositions.push(disposition);
        Ok(())
    }

    fn close(&mut self) -> Result<(), SpiError> {
        let delay = self.close_delay_ms.load(Ordering::Acquire);
        if delay != 0 {
            std::thread::sleep(Duration::from_millis(delay as u64));
        }
        self.backend_state.lock().expect("test state lock").close_calls += 1;
        let (lock, ready) = &*self.queue;
        lock.lock().expect("queue lock").closed = true;
        ready.notify_all();
        TestBackend::wait_at_gate(&self.close_gate, "close");
        if self.close_panics.load(Ordering::Acquire) {
            panic!("synthetic provider close panic");
        }
        if self.close_fails.load(Ordering::Acquire) {
            return Err(test_spi_error("close"));
        }
        Ok(())
    }
}

fn test_spi_error(operation: &'static str) -> SpiError {
    SpiError::Operation {
        provider_id: "sync-test".into(),
        operation,
        resource: None,
        kind: "test_error",
        retryable: Some(false),
        source: Box::new(std::io::Error::other("test SPI error")),
    }
}

fn create_bus() -> (EventBus, Arc<TestBackend>) {
    let backend = Arc::new(TestBackend::new());
    let bus = EventBus::from_spi(
        ProviderId::new("sync-test").expect("valid provider ID"),
        backend.clone(),
    );
    (bus, backend)
}

fn create_bus_with_scheduler(max_in_flight: usize, handler_queue_capacity: usize) -> (EventBus, Arc<TestBackend>) {
    let backend = Arc::new(TestBackend::new());
    let config = EventBusFacadeConfig::new().with_sync_delivery_scheduler(
        SyncDeliverySchedulerConfig::new(max_in_flight, handler_queue_capacity).expect("valid scheduler limits"),
    );
    let bus = EventBus::with_config(
        ProviderId::new("sync-test").expect("valid provider ID"),
        backend.clone(),
        config,
    );
    (bus, backend)
}

fn topic() -> Topic<String> {
    Topic::new("sync.events").expect("valid topic")
}

fn request(payload: String) -> PublishRequest<String> {
    PublishRequest::new(topic(), payload).expect("OS random source available")
}

fn request_with_key(payload: &str, ordering_key: &str) -> PublishRequest<String> {
    PublishRequest::builder()
        .topic(topic())
        .payload(payload.to_owned())
        .ordering_key(ordering_key)
        .build()
        .expect("valid keyed publish request")
}

#[derive(Default)]
struct HandlerGate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl HandlerGate {
    fn wait(&self) {
        let mut released = self.released.lock().expect("handler gate lock");
        while !*released {
            released = self.changed.wait(released).expect("handler gate lock while waiting");
        }
    }

    fn release(&self) {
        *self.released.lock().expect("handler gate lock") = true;
        self.changed.notify_all();
    }
}

#[test]
fn sync_per_key_capability_is_checked_before_spi_subscribe() {
    for (capability, accepted) in [
        (OrderingCapability::None, false),
        (OrderingCapability::PerPartition, false),
        (OrderingCapability::PerKey, true),
        (OrderingCapability::PerSubscription, true),
    ] {
        let (bus, backend) = create_bus();
        backend.set_ordering_capability(capability);
        let options = SubscribeOptions::<String>::builder()
            .ordering_policy(OrderingPolicy::PerKey)
            .build();
        let result = bus.subscribe(
            SubscribeRequest::new("keyed", topic())
                .expect("valid subscriber")
                .with_options(options),
            |_: Delivery<String>| Ok::<(), DeliveryError>(()),
        );
        if accepted {
            let subscription = result.expect("provider supports per-key delivery");
            assert_eq!(backend.subscribe_calls.load(Ordering::Acquire), 1);
            subscription.cancel().expect("subscription cancels");
        } else {
            assert!(matches!(
                result,
                Err(SubscribeError::Capability(CapabilityError::Unsupported {
                    capability: "ordering.per_key"
                }))
            ));
            assert_eq!(backend.subscribe_calls.load(Ordering::Acquire), 0);
        }
    }

    let (bus, backend) = create_bus();
    backend.set_ordering_capability(OrderingCapability::None);
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("unordered", topic()).expect("valid subscriber"),
            |_: Delivery<String>| Ok::<(), DeliveryError>(()),
        )
        .expect("default unordered subscription works without ordering capability");
    assert_eq!(backend.subscribe_calls.load(Ordering::Acquire), 1);
    subscription.cancel().expect("subscription cancels");
}

#[test]
fn publish_and_best_effort_batch_use_the_provider_spi_in_input_order() {
    let (bus, backend) = create_bus();
    let one = bus.publish(request("one".into())).expect("first publish");
    assert_eq!(one.provider_id().as_str(), "sync-test");

    let batch = bus.publish_all([request("two".into()), request("three".into())]);
    assert_eq!(batch.total_count(), 2);
    assert_eq!(batch.accepted_count(), 2);
    assert!(batch.items().iter().all(Result::is_ok));
    assert_eq!(backend.publish_calls(), 3);
}

#[test]
fn shutdown_waits_for_a_publish_admitted_before_shutdown() {
    let (bus, backend) = create_bus();
    let (publish_entered, release_publish) = backend.gate_next_publish();
    let publish_bus = bus.clone();
    let (publish_tx, publish_rx) = mpsc::channel();
    let publish_thread = std::thread::spawn(move || {
        publish_tx
            .send(publish_bus.publish(request("gated".into())))
            .expect("publish result receiver");
    });
    publish_entered
        .recv_timeout(Duration::from_secs(2))
        .expect("publish entered SPI");

    let shutdown_bus = bus.clone();
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let shutdown_thread = std::thread::spawn(move || {
        shutdown_tx
            .send(shutdown_bus.shutdown(ShutdownMode::Immediate))
            .expect("shutdown result receiver");
    });
    assert!(shutdown_rx.recv_timeout(Duration::from_millis(100)).is_err());
    assert_eq!(
        backend.shutdown_calls(),
        0,
        "provider shutdown must wait for admitted publish"
    );

    release_publish.send(()).expect("release publish SPI call");
    publish_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("publish completes")
        .expect("publish succeeds");
    shutdown_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("shutdown completes")
        .expect("shutdown succeeds");
    publish_thread.join().expect("publish thread exits");
    shutdown_thread.join().expect("shutdown thread exits");
    assert_eq!(backend.shutdown_calls(), 1);
}

#[test]
fn graceful_shutdown_deadline_includes_an_admitted_blocking_publish() {
    let (bus, backend) = create_bus();
    let (publish_entered, release_publish) = backend.gate_next_publish();
    let publish_bus = bus.clone();
    let publish_thread = std::thread::spawn(move || publish_bus.publish(request("gated".into())));
    publish_entered
        .recv_timeout(Duration::from_secs(2))
        .expect("publish entered SPI");

    let shutdown_bus = bus.clone();
    let grace = Duration::from_millis(30);
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let shutdown_thread = std::thread::spawn(move || {
        shutdown_tx
            .send(shutdown_bus.shutdown(ShutdownMode::Graceful { timeout: grace }))
            .expect("shutdown result receiver");
    });
    let timed_out_before_release = shutdown_rx
        .recv_timeout(Duration::from_millis(250))
        .map(|result| matches!(result, Err(ShutdownError::TimedOut { timeout }) if timeout == grace))
        .unwrap_or(false);

    release_publish.send(()).expect("release publish SPI call");
    publish_thread
        .join()
        .expect("publish thread exits")
        .expect("admitted publish completes");
    if !timed_out_before_release {
        let _ = shutdown_rx.recv_timeout(Duration::from_secs(2));
    }
    shutdown_thread.join().expect("shutdown thread exits");
    bus.shutdown(ShutdownMode::Immediate)
        .expect("the background shutdown can be joined after publish release");
    assert!(
        timed_out_before_release,
        "Graceful timeout must include time waiting for an admitted SPI publish"
    );
    assert_eq!(1, backend.shutdown_calls());
}

#[test]
fn graceful_shutdown_deadline_includes_provider_shutdown() {
    let (bus, backend) = create_bus();
    let (shutdown_entered, release_shutdown) = backend.gate_next_shutdown();
    let shutdown_bus = bus.clone();
    let grace = Duration::from_millis(30);
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let shutdown_thread = std::thread::spawn(move || {
        shutdown_tx
            .send(shutdown_bus.shutdown(ShutdownMode::Graceful { timeout: grace }))
            .expect("shutdown result receiver");
    });
    shutdown_entered
        .recv_timeout(Duration::from_secs(2))
        .expect("provider shutdown entered");

    let first_result = shutdown_rx.recv_timeout(Duration::from_millis(250)).ok();
    let timed_out_before_release = matches!(
        first_result,
        Some(Err(ShutdownError::TimedOut { timeout })) if timeout == grace
    );
    release_shutdown.send(()).expect("release provider shutdown");
    if first_result.is_none() {
        let _ = shutdown_rx.recv_timeout(Duration::from_secs(2));
    }
    shutdown_thread.join().expect("shutdown thread exits");
    bus.shutdown(ShutdownMode::Immediate)
        .expect("the completed coordinator outcome is observable later");
    assert!(
        timed_out_before_release,
        "the caller deadline must include provider shutdown"
    );
    assert_eq!(1, backend.shutdown_calls());
}

#[test]
fn graceful_shutdown_deadline_includes_subscription_close() {
    let (bus, backend) = create_bus();
    let (close_entered, release_close) = backend.gate_next_close();
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("close-deadline", topic()).expect("valid ID"),
            |_| (),
        )
        .expect("subscription starts");
    let shutdown_bus = bus.clone();
    let grace = Duration::from_millis(30);
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let shutdown_thread = std::thread::spawn(move || {
        shutdown_tx
            .send(shutdown_bus.shutdown(ShutdownMode::Graceful { timeout: grace }))
            .expect("shutdown result receiver");
    });
    close_entered
        .recv_timeout(Duration::from_secs(2))
        .expect("subscription close entered");

    let first_result = shutdown_rx.recv_timeout(Duration::from_millis(250)).ok();
    let timed_out_before_release = matches!(
        first_result,
        Some(Err(ShutdownError::TimedOut { timeout })) if timeout == grace
    );
    release_close.send(()).expect("release subscription close");
    if first_result.is_none() {
        let _ = shutdown_rx.recv_timeout(Duration::from_secs(2));
    }
    shutdown_thread.join().expect("shutdown thread exits");
    bus.shutdown(ShutdownMode::Immediate)
        .expect("the completed coordinator outcome is observable later");
    assert!(
        timed_out_before_release,
        "the caller deadline must include receiver close"
    );
    assert_eq!(1, backend.shutdown_calls());
}

#[test]
fn immediate_shutdown_strengthens_a_timed_out_graceful_attempt() {
    let (bus, backend) = create_bus();
    let (started_tx, started_rx) = mpsc::channel();
    let handler_gate = Arc::new(HandlerGate::default());
    let handler_gate_for_callback = handler_gate.clone();
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("upgrade-mode", topic()).expect("valid ID"),
            move |_| {
                started_tx.send(()).expect("handler observer remains alive");
                handler_gate_for_callback.wait();
            },
        )
        .expect("subscription starts");
    bus.publish(request("blocked".into())).expect("publish starts handler");
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("handler is blocked");

    let grace_bus = bus.clone();
    let grace = Duration::from_millis(30);
    let (grace_tx, grace_rx) = mpsc::channel();
    let grace_thread = std::thread::spawn(move || {
        grace_tx
            .send(grace_bus.shutdown(ShutdownMode::Graceful { timeout: grace }))
            .expect("graceful result receiver");
    });
    assert!(matches!(
        grace_rx.recv_timeout(Duration::from_secs(2)),
        Ok(Err(ShutdownError::TimedOut { timeout })) if timeout == grace
    ));

    let immediate_bus = bus.clone();
    let (immediate_tx, immediate_rx) = mpsc::channel();
    let immediate_thread = std::thread::spawn(move || {
        immediate_tx
            .send(immediate_bus.shutdown(ShutdownMode::Immediate))
            .expect("immediate result receiver");
    });
    assert!(immediate_rx.recv_timeout(Duration::from_millis(50)).is_err());
    handler_gate.release();
    immediate_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("immediate shutdown completes after active handler")
        .expect("immediate shutdown succeeds");
    grace_thread.join().expect("graceful caller exits after timeout");
    immediate_thread.join().expect("immediate caller exits");
    assert_eq!([ShutdownMode::Immediate], backend.shutdown_modes().as_slice());
}

#[test]
fn shutdown_from_publish_interceptor_returns_would_deadlock_instead_of_waiting_for_its_permit() {
    let (bus, backend) = create_bus();
    let callback_bus = bus.clone();
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let options = PublishOptions::<String>::builder()
        .interceptor(move |envelope| {
            shutdown_tx
                .send(callback_bus.shutdown(ShutdownMode::Immediate))
                .expect("shutdown result receiver");
            Ok(Some(envelope))
        })
        .build();
    let publish_bus = bus.clone();
    let (publish_tx, publish_rx) = mpsc::channel();
    let publish_thread = std::thread::spawn(move || {
        publish_tx
            .send(publish_bus.publish(request("reentrant".into()).with_options(options)))
            .expect("publish result receiver");
    });

    assert!(matches!(
        shutdown_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("callback shutdown must return"),
        Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock {
            operation: "shutdown"
        }))
    ));
    publish_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("publish completes")
        .expect("publish succeeds");
    publish_thread.join().expect("publish thread exits");
    assert_eq!(
        backend.shutdown_calls(),
        0,
        "reentrant shutdown must not close provider"
    );
    bus.shutdown(ShutdownMode::Immediate)
        .expect("shutdown outside callback");
}

#[test]
fn shutdown_waits_for_an_admitted_subscribe_and_closes_its_late_receiver() {
    let (bus, backend) = create_bus();
    let (subscribe_entered, release_subscribe) = backend.gate_next_subscribe();
    let subscribe_bus = bus.clone();
    let (subscribe_tx, subscribe_rx) = mpsc::channel();
    let subscribe_thread = std::thread::spawn(move || {
        let result = subscribe_bus.subscribe(
            SubscribeRequest::new("gated-subscribe", topic()).expect("valid ID"),
            |_| (),
        );
        subscribe_tx.send(result).expect("subscribe result receiver");
    });
    subscribe_entered
        .recv_timeout(Duration::from_secs(2))
        .expect("subscribe entered SPI");

    let shutdown_bus = bus.clone();
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let shutdown_thread = std::thread::spawn(move || {
        shutdown_tx
            .send(shutdown_bus.shutdown(ShutdownMode::Immediate))
            .expect("shutdown result receiver");
    });
    assert!(shutdown_rx.recv_timeout(Duration::from_millis(100)).is_err());
    assert_eq!(
        backend.shutdown_calls(),
        0,
        "provider shutdown must wait for admitted subscribe"
    );

    release_subscribe.send(()).expect("release subscribe SPI call");
    let subscription = subscribe_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("subscribe completes")
        .expect("admitted subscribe succeeds");
    shutdown_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("shutdown completes")
        .expect("shutdown succeeds");
    subscribe_thread.join().expect("subscribe thread exits");
    shutdown_thread.join().expect("shutdown thread exits");
    assert!(subscription.is_cancelled());
    assert_eq!(backend.close_calls(), 1, "late-created receiver is closed exactly once");
    assert_eq!(backend.shutdown_calls(), 1);
}

#[test]
fn terminal_publish_retry_error_retains_reason_attempt_and_spi_source() {
    let (bus, backend) = create_bus();
    backend.fail_publish_call(1);
    let options = PublishOptions::<String>::builder()
        .retry_policy(RetryPolicy::builder().max_attempts(1).build().unwrap())
        .retry_rule(|_: &AttemptFailure<PublishAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .build();
    let request = PublishRequest::builder()
        .topic(topic())
        .payload("terminal".to_owned())
        .options(options)
        .build()
        .unwrap();

    let error = bus.publish(request).unwrap_err();
    let PublishError::Retry(retry) = error else {
        panic!("terminal retry state must remain typed at the facade boundary");
    };
    assert!(matches!(retry.reason(), RetryErrorReason::Exhausted { .. }));
    let failure = retry.last_failure().expect("last attempt is retained");
    assert!(matches!(failure, AttemptFailure::Error(_)));
    assert_eq!(retry.context().attempts(), 1);
    let attempt_error = retry.last_error().expect("typed attempt error is retained");
    assert_eq!(attempt_error.kind(), "test_error");
    let mut source = std::error::Error::source(attempt_error);
    let mut found_spi_source = false;
    while let Some(error) = source {
        if error.to_string() == "test SPI error" {
            found_spi_source = true;
            break;
        }
        source = error.source();
    }
    assert!(
        found_spi_source,
        "terminal retry must retain the provider error source chain"
    );
}

#[test]
fn panicking_codec_requeues_the_provider_message_instead_of_losing_its_token() {
    let (bus, backend) = create_bus();
    let panic_was_observed = Arc::new(AtomicBool::new(false));
    let observed = panic_was_observed.clone();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::InternalFailure { origin, .. } if origin.as_ref() == "delivery_worker") {
            observed.store(true, Ordering::Release);
        }
    });
    let codec = PanickingCodec {
        content_type: ContentType::new("text/plain").expect("valid MIME type"),
    };
    let encoded_topic = Topic::new_with_codec("sync.events", codec).expect("valid codec topic");
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("codec-panic", encoded_topic.clone()).expect("valid ID"),
            |_: Delivery<String>| -> () { panic!("codec panic must prevent handler invocation") },
        )
        .expect("subscription starts");
    backend.enqueue_encoded(EncodedPayload::new(
        Arc::<[u8]>::from(&b"payload"[..]),
        ContentType::new("text/plain").expect("valid MIME type"),
        None,
    ));
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !panic_was_observed.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(
        panic_was_observed.load(Ordering::Acquire),
        "codec panic should be diagnosed"
    );
    bus.wait_for_received_deliveries(&encoded_topic, Some(Duration::from_secs(2)))
        .expect("panic path settles");
    assert_eq!(backend.settlement_dispositions(), [DeliveryDisposition::Retry]);
    subscription.cancel().expect("cancel subscription");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn panicking_custom_retry_rule_requeues_instead_of_rejecting_delivery() {
    let (bus, backend) = create_bus();
    let options = SubscribeOptions::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(2)
                .build()
                .expect("valid retry policy"),
        )
        .retry_rule(|_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| panic!("synthetic retry rule panic"))
        .build();
    let (handler_entered_tx, handler_entered_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("retry-rule-panic", topic())
                .expect("valid ID")
                .with_options(options),
            move |_| {
                handler_entered_tx
                    .send(())
                    .expect("handler entry observer remains alive");
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("handler failure")),
                })
            },
        )
        .expect("subscription starts");
    bus.publish(request("preserve-retry-token".into()))
        .expect("publish succeeds");
    handler_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("handler starts");
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("retry rule panic settles");
    assert_eq!(backend.settlement_dispositions(), [DeliveryDisposition::Retry]);
    subscription.cancel().expect("cancel subscription");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

struct PanickingCodec {
    content_type: ContentType,
}

impl EventCodec<String> for PanickingCodec {
    fn content_type(&self) -> &ContentType {
        &self.content_type
    }
    fn schema_id(&self) -> Option<&SchemaId> {
        None
    }
    fn encode(&self, _: &String) -> Result<Arc<[u8]>, CodecError> {
        Ok(Arc::<[u8]>::from([]))
    }
    fn decode(&self, _: &[u8]) -> Result<String, CodecError> {
        panic!("synthetic codec panic")
    }
}

#[test]
fn cancelling_subscriber_retry_terminates_its_flow_without_stopping_other_subscriptions() {
    let (bus, backend) = create_bus();
    let token = RetryCancellationToken::new();
    let (first_failed_tx, first_failed_rx) = mpsc::channel();
    let (independent_tx, independent_rx) = mpsc::channel();
    let (terminal_failure_tx, terminal_failure_rx) = mpsc::channel();
    let attempts = Arc::new(AtomicUsize::new(0));
    let failure_observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::DeliveryFailed { subscriber_id, .. } = diagnostic {
            terminal_failure_tx.send(subscriber_id.as_str().to_owned()).unwrap();
        }
    });
    let attempts_by_cancelled_handler = attempts.clone();
    let options = SubscribeOptions::<String>::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(4)
                .backoff(BackoffPolicy::fixed(Duration::from_secs(60)))
                .build()
                .unwrap(),
        )
        .retry_rule(|_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .retry_cancellation_token(token.clone())
        .error_handler(move |_, _| {
            let _ = first_failed_tx.send(());
            FailureDirective::Retry
        })
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("cancel-retry-lane", topic())
                .expect("valid subscriber ID")
                .with_options(options),
            move |delivery: Delivery<String>| {
                attempts_by_cancelled_handler.fetch_add(1, Ordering::AcqRel);
                if delivery.payload() == "first" {
                    Err(DeliveryError::Handler {
                        source: Box::new(std::io::Error::other("retry")),
                    })
                } else {
                    Ok(())
                }
            },
        )
        .unwrap();
    let attempts_by_independent_handler = Arc::new(AtomicUsize::new(0));
    let independent_attempts = attempts_by_independent_handler.clone();
    let independent_subscription = bus
        .subscribe(
            SubscribeRequest::new("independent-subscriber", topic()).expect("valid subscriber ID"),
            move |delivery: Delivery<String>| {
                independent_attempts.fetch_add(1, Ordering::AcqRel);
                independent_tx.send(delivery.payload().clone()).unwrap();
            },
        )
        .unwrap();
    let publish = |value: &str| {
        PublishRequest::builder()
            .topic(topic())
            .payload(value.to_owned())
            .ordering_key("same-key")
            .build()
            .unwrap()
    };
    bus.publish(publish("first")).unwrap();
    first_failed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(independent_rx.recv_timeout(Duration::from_secs(2)).unwrap(), "first");
    std::thread::sleep(Duration::from_millis(50));
    token.cancel();
    assert_eq!(
        terminal_failure_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        "cancel-retry-lane"
    );
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .unwrap();
    assert!(
        token.is_cancelled(),
        "external cancellation must remain observable on caller-owned token"
    );
    bus.publish(publish("second")).unwrap();
    assert_eq!(independent_rx.recv_timeout(Duration::from_secs(2)).unwrap(), "second");
    assert_eq!(
        terminal_failure_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        "cancel-retry-lane"
    );
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .unwrap();
    assert_eq!(
        attempts.load(Ordering::Acquire),
        1,
        "a cancelled shared token rejects later attempts before invoking the handler"
    );
    assert_eq!(attempts_by_independent_handler.load(Ordering::Acquire), 2);
    assert_eq!(
        backend
            .settlement_dispositions()
            .iter()
            .filter(|d| **d == DeliveryDisposition::Reject)
            .count(),
        2,
        "the cancelled subscription terminally rejects its failed and later delivery"
    );
    subscription.cancel().unwrap();
    independent_subscription.cancel().unwrap();
    bus.shutdown(ShutdownMode::Immediate).unwrap();
    drop(failure_observer);
}

#[test]
fn synchronous_facade_rejects_async_subscriber_interceptors_before_provider_subscription() {
    let (bus, backend) = create_bus();
    let options = SubscribeOptions::<String>::builder()
        .async_interceptor(|_, _| Box::pin(async { Ok(()) }) as SpiFuture<'static, Result<(), DeliveryError>>)
        .build();
    let request = SubscribeRequest::new("async-only-interceptor", topic())
        .expect("valid subscriber ID")
        .with_options(options);

    let error = match bus.subscribe(request, |_| ()) {
        Ok(_) => panic!("sync facade must reject async middleware"),
        Err(error) => error,
    };
    match error {
        SubscribeError::Configuration(ConfigurationError::InvalidField { field, .. }) => {
            assert_eq!(field, "async_subscriber_interceptor");
        }
        other => panic!("expected runtime-model configuration error, got {other}"),
    }
    assert!(backend.state.lock().expect("test state lock").queues.is_empty());
}

#[test]
fn subscription_worker_processes_and_settles_spi_messages_until_cancelled() {
    let (bus, backend) = create_bus();
    let (handled_tx, handled_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("sync-subscriber", topic()).expect("valid subscriber ID"),
            move |delivery: Delivery<String>| {
                handled_tx
                    .send(delivery.payload().clone())
                    .expect("test receiver remains alive");
                Ok(())
            },
        )
        .expect("subscription starts");

    bus.publish(request("hello".into())).expect("publish to SPI");
    assert_eq!(
        handled_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("handler invoked"),
        "hello"
    );
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("handler and settlement complete");
    assert_eq!(backend.settlement_calls(), 1);

    subscription.cancel().expect("cancel closes SPI subscription");
    assert!(subscription.is_cancelled());
    assert_eq!(backend.close_calls(), 1);
}

#[test]
fn sync_settlement_failure_retries_same_token_without_rerunning_handler() {
    let (bus, backend) = create_bus();
    let handled = Arc::new(AtomicUsize::new(0));
    let handled_by_callback = handled.clone();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("sync-settlement-retry", topic()).expect("valid subscriber ID"),
            move |_: Delivery<String>| {
                handled_by_callback.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .expect("subscription starts");

    backend.fail_next_settle();
    backend.enqueue_marked(subscription.id());
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while backend.settlement_calls() < 2 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }

    assert_eq!(
        backend.settlement_calls(),
        2,
        "settlement must retry after provider error"
    );
    assert_eq!(handled.load(Ordering::SeqCst), 1, "retry must not invoke handler again");
    assert_eq!(
        backend.settlement_calls(),
        2,
        "same token is retried after provider error"
    );
    assert_eq!(backend.settlement_dispositions(), [DeliveryDisposition::Accept]);
    subscription.cancel().expect("cancel closes SPI subscription");
}

#[test]
fn sync_cancellation_stops_permanent_settlement_retry_and_closes_receiver() {
    let (bus, backend) = create_bus();
    let handled = Arc::new(AtomicUsize::new(0));
    let handled_by_callback = handled.clone();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("sync-permanent-settlement-failure", topic()).expect("valid subscriber ID"),
            move |_: Delivery<String>| {
                handled_by_callback.fetch_add(1, Ordering::SeqCst);
                Ok(())
            },
        )
        .expect("subscription starts");
    backend.fail_all_settles();
    backend.enqueue_marked(subscription.id());

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while backend.settlement_calls() == 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(backend.settlement_calls() > 0, "delivery reaches provider settlement");
    subscription
        .cancel()
        .expect("cancellation performs a final attempt, releases task, then closes receiver");
    assert_eq!(backend.close_calls(), 1);
    assert_eq!(handled.load(Ordering::SeqCst), 1);
}

#[test]
fn per_key_scheduler_runs_other_keys_concurrently_and_keeps_same_key_serial() {
    let (bus, backend) = create_bus_with_scheduler(3, 8);
    let options = SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let first_gate = Arc::new(HandlerGate::default());
    let first_gate_by_handler = first_gate.clone();
    let release_first = first_gate.clone();
    let (first_started_tx, first_started_rx) = mpsc::channel();
    let (key_b_tx, key_b_rx) = mpsc::channel();
    let (key_a_second_tx, key_a_second_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("per-key-scheduler", topic())
                .expect("valid ID")
                .with_options(options),
            move |delivery: Delivery<String>| match delivery.payload().as_str() {
                "a-first" => {
                    first_started_tx.send(()).expect("first-handler receiver remains alive");
                    first_gate_by_handler.wait();
                    Ok(())
                }
                "a-second" => {
                    key_a_second_tx
                        .send(())
                        .expect("same-key completion receiver remains alive");
                    Ok(())
                }
                "b" => {
                    key_b_tx
                        .send(())
                        .expect("different-key completion receiver remains alive");
                    Ok(())
                }
                other => panic!("unexpected payload {other}"),
            },
        )
        .expect("subscription starts");

    bus.publish(request_with_key("a-first", "account-a"))
        .expect("publish first A");
    first_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first key-A handler starts");
    bus.publish(request_with_key("a-second", "account-a"))
        .expect("publish queued A");
    bus.publish(request_with_key("b", "account-b"))
        .expect("publish independent B");

    key_b_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("different key progresses while key A is blocked");
    assert!(
        key_a_second_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "same key remains serialized"
    );
    release_first.release();
    key_a_second_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("same-key task runs after prior settlement");
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("all keyed work completes");
    subscription.cancel().expect("cancel");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
    assert_eq!(backend.settlement_calls(), 3);
}

#[test]
fn global_max_in_flight_includes_queued_deliveries_before_admission() {
    let (bus, backend) = create_bus_with_scheduler(2, 1);
    let options = SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let first_gate = Arc::new(HandlerGate::default());
    let second_gate = Arc::new(HandlerGate::default());
    let first_gate_by_handler = first_gate.clone();
    let second_gate_by_handler = second_gate.clone();
    let (first_started_tx, first_started_rx) = mpsc::channel();
    let (second_started_tx, second_started_rx) = mpsc::channel();
    let (other_key_tx, other_key_rx) = mpsc::channel();
    let (done_tx, done_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("global-in-flight", topic())
                .expect("valid ID")
                .with_options(options),
            move |delivery: Delivery<String>| {
                match delivery.payload().as_str() {
                    "a-first" => {
                        first_started_tx.send(()).expect("handler-start receiver remains alive");
                        first_gate_by_handler.wait();
                    }
                    "a-second" => {
                        second_started_tx
                            .send(())
                            .expect("handler-start receiver remains alive");
                        second_gate_by_handler.wait();
                    }
                    "b" => other_key_tx.send(()).expect("other-key receiver remains alive"),
                    other => panic!("unexpected payload {other}"),
                }
                done_tx.send(()).expect("completion receiver remains alive");
            },
        )
        .expect("subscription starts");

    bus.publish(request_with_key("a-first", "same-key"))
        .expect("publish first key-A delivery");
    first_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first key-A handler starts");
    bus.publish(request_with_key("a-second", "same-key"))
        .expect("publish same-key queued delivery");
    bus.publish(request_with_key("b", "other-key"))
        .expect("publish other-key delivery");
    let receive_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while backend.received_messages() < 3 && std::time::Instant::now() < receive_deadline {
        std::thread::yield_now();
    }
    assert_eq!(
        backend.received_messages(),
        3,
        "the third delivery occupies the bounded coordinator handoff"
    );
    assert!(
        other_key_rx.recv_timeout(Duration::from_millis(100)).is_err(),
        "queued work consumes an in-flight permit"
    );

    first_gate.release();
    second_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("queued same-key handler starts next");
    other_key_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("terminal first delivery releases its permit while second key-A delivery remains active");
    second_gate.release();
    for _ in 0..3 {
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("each accepted delivery completes");
    }
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("all admitted work settles");
    subscription.cancel().expect("cancel");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn saturated_scheduler_holds_only_one_received_handoff_and_loses_no_messages() {
    let (bus, backend) = create_bus_with_scheduler(3, 1);
    let options = SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let gate = Arc::new(HandlerGate::default());
    let gate_by_handler = gate.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_by_handler = calls.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("bounded-handoff", topic())
                .expect("valid ID")
                .with_options(options),
            move |_| {
                let index = calls_by_handler.fetch_add(1, Ordering::AcqRel);
                if index == 0 {
                    gate_by_handler.wait();
                }
                done_tx.send(()).expect("completion receiver remains alive");
            },
        )
        .expect("subscription starts");
    bus.publish(request_with_key("active", "same-key"))
        .expect("publish active");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while calls.load(Ordering::Acquire) == 0 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(
        calls.load(Ordering::Acquire),
        1,
        "first delivery occupies the only worker"
    );
    for payload in ["queued", "handoff", "provider-buffered"] {
        bus.publish(request_with_key(payload, "same-key"))
            .expect("publish under saturation");
    }
    let receive_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while backend.received_messages() < 3 && std::time::Instant::now() < receive_deadline {
        std::thread::yield_now();
    }
    assert_eq!(
        backend.received_messages(),
        3,
        "one queued message and one pending handoff are bounded"
    );
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        backend.received_messages(),
        3,
        "coordinator pauses receive while its handoff is occupied"
    );

    gate.release();
    for _ in 0..4 {
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("each provider message reaches the handler");
    }
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("all messages settle after capacity returns");
    assert_eq!(calls.load(Ordering::Acquire), 4);
    assert_eq!(backend.settlement_calls(), 4);
    subscription.cancel().expect("cancel");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn zero_handler_queue_capacity_allows_only_direct_handoff_to_an_idle_key_lane() {
    let (bus, backend) = create_bus_with_scheduler(2, 0);
    let options = SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let first_gate = Arc::new(HandlerGate::default());
    let first_gate_by_handler = first_gate.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_by_handler = calls.clone();
    let (second_started_tx, second_started_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("no-handler-queue", topic())
                .expect("valid ID")
                .with_options(options),
            move |_| {
                if calls_by_handler.fetch_add(1, Ordering::AcqRel) == 0 {
                    first_gate_by_handler.wait();
                } else {
                    second_started_tx
                        .send(())
                        .expect("second-handler receiver remains alive");
                }
            },
        )
        .expect("subscription starts");

    bus.publish(request_with_key("first", "same-key"))
        .expect("publish first");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while calls.load(Ordering::Acquire) == 0 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(
        calls.load(Ordering::Acquire),
        1,
        "first handler occupies its ordering key"
    );
    bus.publish(request_with_key("second", "same-key"))
        .expect("publish second");
    let receive_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while backend.received_messages() < 2 && std::time::Instant::now() < receive_deadline {
        std::thread::yield_now();
    }
    assert_eq!(
        backend.received_messages(),
        2,
        "the coordinator keeps one non-admitted handoff"
    );
    assert_eq!(
        calls.load(Ordering::Acquire),
        1,
        "zero queue capacity does not start a queued handler"
    );

    first_gate.release();
    second_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("second handler starts after its key is free");
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("both messages settle");
    assert_eq!(calls.load(Ordering::Acquire), 2);
    assert_eq!(backend.settlement_calls(), 2);
    subscription.cancel().expect("cancel");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn cancel_requeues_admitted_waiting_jobs_before_waiting_for_active_handler() {
    let (bus, backend) = create_bus_with_scheduler(2, 2);
    let options = SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let first_gate = Arc::new(HandlerGate::default());
    let first_gate_by_handler = first_gate.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_by_handler = calls.clone();
    let subscription = Arc::new(
        bus.subscribe(
            SubscribeRequest::new("cancel-queued-handler", topic())
                .expect("valid ID")
                .with_options(options),
            move |_| {
                if calls_by_handler.fetch_add(1, Ordering::AcqRel) == 0 {
                    first_gate_by_handler.wait();
                }
            },
        )
        .expect("subscription starts"),
    );
    bus.publish(request_with_key("active", "same-key"))
        .expect("publish active");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while calls.load(Ordering::Acquire) == 0 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    bus.publish(request_with_key("queued", "same-key"))
        .expect("publish queued");
    let receive_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while backend.received_messages() < 2 && std::time::Instant::now() < receive_deadline {
        std::thread::yield_now();
    }
    assert_eq!(backend.received_messages(), 2);

    let cancel_subscription = subscription.clone();
    let (cancelled_tx, cancelled_rx) = mpsc::channel();
    let cancel_thread = std::thread::spawn(move || {
        cancelled_tx
            .send(cancel_subscription.cancel())
            .expect("cancel result receiver remains alive");
    });
    let settlement_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while backend.settlement_calls() == 0 && std::time::Instant::now() < settlement_deadline {
        std::thread::yield_now();
    }
    assert_eq!(backend.settlement_dispositions(), [DeliveryDisposition::Retry]);
    assert_eq!(
        calls.load(Ordering::Acquire),
        1,
        "queued handler is requeued without starting"
    );

    first_gate.release();
    cancelled_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("cancel completes")
        .expect("cancel succeeds");
    cancel_thread.join().expect("cancel thread exits");
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert_eq!(backend.settlement_calls(), 2);
    subscription.cancel().expect("repeated cancel observes completion");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn graceful_shutdown_drains_admitted_jobs_and_requeues_unadmitted_handoff() {
    let (bus, backend) = create_bus_with_scheduler(2, 1);
    let options = SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let first_gate = Arc::new(HandlerGate::default());
    let first_gate_by_handler = first_gate.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_by_handler = calls.clone();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("graceful-admission-boundary", topic())
                .expect("valid ID")
                .with_options(options),
            move |_| {
                if calls_by_handler.fetch_add(1, Ordering::AcqRel) == 0 {
                    first_gate_by_handler.wait();
                }
            },
        )
        .expect("subscription starts");
    for payload in ["active", "admitted-queued", "unadmitted"] {
        bus.publish(request_with_key(payload, "same-key"))
            .expect("publish delivery");
        if payload == "active" {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            while calls.load(Ordering::Acquire) == 0 && std::time::Instant::now() < deadline {
                std::thread::yield_now();
            }
            assert_eq!(calls.load(Ordering::Acquire), 1, "first handler becomes active");
        }
    }
    let receive_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while backend.received_messages() < 3 && std::time::Instant::now() < receive_deadline {
        std::thread::yield_now();
    }
    assert_eq!(
        backend.received_messages(),
        3,
        "one job is admitted queued; one remains coordinator-pending"
    );

    let shutdown_bus = bus.clone();
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let shutdown_thread = std::thread::spawn(move || {
        shutdown_tx
            .send(shutdown_bus.shutdown(ShutdownMode::Graceful {
                timeout: Duration::from_secs(2),
            }))
            .expect("shutdown result receiver remains alive");
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !subscription.is_cancelled() && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(subscription.is_cancelled());
    first_gate.release();
    shutdown_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("shutdown completes")
        .expect("shutdown succeeds");
    shutdown_thread.join().expect("shutdown thread exits");
    assert_eq!(
        calls.load(Ordering::Acquire),
        2,
        "graceful mode drains both admitted handlers only"
    );
    let dispositions = backend.settlement_dispositions();
    assert_eq!(
        dispositions
            .iter()
            .filter(|item| **item == DeliveryDisposition::Accept)
            .count(),
        2
    );
    assert_eq!(
        dispositions
            .iter()
            .filter(|item| **item == DeliveryDisposition::Retry)
            .count(),
        1
    );
}

#[test]
fn immediate_shutdown_requeues_queued_deliveries_without_starting_handlers() {
    let (bus, backend) = create_bus_with_scheduler(3, 2);
    let options = SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let gate = Arc::new(HandlerGate::default());
    let gate_by_handler = gate.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_by_handler = calls.clone();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("immediate-scheduler", topic())
                .expect("valid ID")
                .with_options(options),
            move |_| {
                if calls_by_handler.fetch_add(1, Ordering::AcqRel) == 0 {
                    gate_by_handler.wait();
                }
            },
        )
        .expect("subscription starts");
    bus.publish(request_with_key("active", "same-key"))
        .expect("publish active");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while calls.load(Ordering::Acquire) == 0 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(calls.load(Ordering::Acquire), 1);
    bus.publish(request_with_key("queued-one", "same-key"))
        .expect("publish queued one");
    bus.publish(request_with_key("queued-two", "same-key"))
        .expect("publish queued two");
    let receive_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while backend.received_messages() < 3 && std::time::Instant::now() < receive_deadline {
        std::thread::yield_now();
    }
    assert_eq!(backend.received_messages(), 3, "both queued jobs are accepted");

    let shutdown_bus = bus.clone();
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let shutdown_thread = std::thread::spawn(move || {
        shutdown_tx
            .send(shutdown_bus.shutdown(ShutdownMode::Immediate))
            .expect("receiver remains alive");
    });
    let shutdown_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !subscription.is_cancelled() && std::time::Instant::now() < shutdown_deadline {
        std::thread::yield_now();
    }
    assert!(subscription.is_cancelled());
    gate.release();
    shutdown_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("shutdown completes")
        .expect("shutdown succeeds");
    shutdown_thread.join().expect("shutdown thread exits");
    assert_eq!(calls.load(Ordering::Acquire), 1, "queued handlers never start");
    let dispositions = backend.settlement_dispositions();
    assert_eq!(
        dispositions
            .iter()
            .filter(|item| **item == DeliveryDisposition::Accept)
            .count(),
        1
    );
    assert_eq!(
        dispositions
            .iter()
            .filter(|item| **item == DeliveryDisposition::Retry)
            .count(),
        2
    );
}

/// Verifies manual mode accepts only explicit positive acknowledgements.
#[test]
fn manual_acknowledgement_accepts_only_explicit_acknowledgements() {
    let (bus, backend) = create_bus();
    let options = SubscribeOptions::<String>::builder().ack_mode(AckMode::Manual).build();
    let (handled_tx, handled_rx) = mpsc::channel();
    let ack_tx = handled_tx.clone();
    let pending_tx = handled_tx.clone();
    let nack_tx = handled_tx.clone();
    let acknowledged = bus
        .subscribe(
            SubscribeRequest::new("manual-ack", topic())
                .expect("valid ID")
                .with_options(options.clone()),
            move |delivery: Delivery<String>| {
                delivery.acknowledgement().ack().expect("first ACK succeeds");
                ack_tx.send("acked").expect("test receiver remains alive");
            },
        )
        .expect("ACK subscription starts");
    let unacknowledged = bus
        .subscribe(
            SubscribeRequest::new("manual-pending", topic())
                .expect("valid ID")
                .with_options(options),
            move |_| {
                pending_tx.send("pending").expect("test receiver remains alive");
            },
        )
        .expect("pending subscription starts");
    let nack_options = SubscribeOptions::<String>::builder()
        .ack_mode(AckMode::Manual)
        .error_handler(|_, _| FailureDirective::Requeue)
        .build();
    let negatively_acknowledged = bus
        .subscribe(
            SubscribeRequest::new("manual-nack", topic())
                .expect("valid ID")
                .with_options(nack_options),
            move |delivery: Delivery<String>| {
                delivery.acknowledgement().nack().expect("first NACK succeeds");
                nack_tx.send("nacked").expect("test receiver remains alive");
            },
        )
        .expect("NACK subscription starts");

    bus.publish(request("manual".into())).expect("publish");
    let mut handled = [
        handled_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        handled_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
        handled_rx.recv_timeout(Duration::from_secs(2)).unwrap(),
    ];
    handled.sort_unstable();
    assert_eq!(handled, ["acked", "nacked", "pending"]);
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("manual ACKs settle");
    let dispositions = backend.settlement_dispositions();
    assert_eq!(dispositions.len(), 3);
    assert!(dispositions.contains(&DeliveryDisposition::Accept));
    assert!(dispositions.contains(&DeliveryDisposition::Reject));
    assert!(dispositions.contains(&DeliveryDisposition::Retry));
    acknowledged.cancel().expect("cancel ACK subscription");
    unacknowledged.cancel().expect("cancel pending subscription");
    negatively_acknowledged.cancel().expect("cancel NACK subscription");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

/// Verifies subscriber middleware and error callbacks surround every retry
/// attempt.
#[test]
fn subscriber_interceptor_and_error_handler_wrap_each_failed_retry_attempt() {
    let (bus, backend) = create_bus();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let before = calls.clone();
    let after = calls.clone();
    let error = calls.clone();
    let handler_calls = Arc::new(AtomicUsize::new(0));
    let handler_counter = handler_calls.clone();
    let handler_log = calls.clone();
    let (completed_tx, completed_rx) = mpsc::channel();
    let options = SubscribeOptions::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(2)
                .build()
                .expect("valid retry policy"),
        )
        .retry_rule(|_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .interceptor(move |delivery, next| {
            before.lock().unwrap().push("before");
            let result = next(delivery);
            after.lock().unwrap().push("after");
            result
        })
        .error_handler(move |_, _| {
            error.lock().unwrap().push("error");
            FailureDirective::Retry
        })
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("middleware-retry", topic())
                .expect("valid ID")
                .with_options(options),
            move |_| {
                let attempt = handler_counter.fetch_add(1, Ordering::AcqRel);
                handler_log
                    .lock()
                    .unwrap()
                    .push(if attempt == 0 { "handler-1" } else { "handler-2" });
                if attempt == 0 {
                    Err(DeliveryError::Handler {
                        source: Box::new(std::io::Error::other("retry")),
                    })
                } else {
                    completed_tx.send(()).expect("test receiver remains alive");
                    Ok(())
                }
            },
        )
        .expect("subscription starts");

    bus.publish(request("retry-middleware".into())).expect("publish");
    completed_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("second attempt completes");
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("retry completes");
    assert_eq!(
        *calls.lock().unwrap(),
        ["before", "handler-1", "after", "error", "before", "handler-2", "after"]
    );
    assert_eq!(handler_calls.load(Ordering::Acquire), 2);
    let dispositions = backend.settlement_dispositions();
    assert_eq!(
        dispositions
            .iter()
            .filter(|item| **item == DeliveryDisposition::Accept)
            .count(),
        1
    );
    assert_eq!(
        dispositions
            .iter()
            .filter(|item| **item == DeliveryDisposition::Retry)
            .count(),
        0
    );
    subscription.cancel().expect("cancel");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn facade_subscriber_middleware_wraps_typed_middleware_and_filter_bypasses_both() {
    let backend = Arc::new(TestBackend::new());
    let calls = Arc::new(Mutex::new(Vec::new()));
    let global_calls = calls.clone();
    let config = EventBusFacadeConfig::new().subscriber_interceptor(
        move |delivery: Delivery<String>, next: SubscriberNext<String>| {
            global_calls.lock().unwrap().push("global-before");
            let result = next(delivery);
            global_calls.lock().unwrap().push("global-after");
            result
        },
    );
    let bus = EventBus::with_config(ProviderId::new("sync-test").unwrap(), backend, config);
    let typed_calls = calls.clone();
    let options = SubscribeOptions::<String>::builder()
        .interceptor(move |delivery, next| {
            typed_calls.lock().unwrap().push("typed-before");
            let result = next(delivery);
            typed_calls.lock().unwrap().push("typed-after");
            result
        })
        .build();
    let (done_tx, done_rx) = mpsc::channel();
    let handler_calls = calls.clone();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("global-chain", topic())
                .expect("valid subscriber ID")
                .with_options(options),
            move |_| {
                handler_calls.lock().unwrap().push("handler");
                done_tx.send(()).unwrap();
            },
        )
        .unwrap();
    bus.publish(request("ordered".into())).unwrap();
    done_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .unwrap();
    assert_eq!(
        *calls.lock().unwrap(),
        [
            "global-before",
            "typed-before",
            "handler",
            "typed-after",
            "global-after"
        ]
    );

    let filtered_calls = calls.clone();
    let (filtered_tx, filtered_rx) = mpsc::channel();
    let filtered = SubscribeOptions::<String>::builder()
        .filter(move |_| {
            filtered_tx.send(()).unwrap();
            false
        })
        .build();
    let filtered_subscription = bus
        .subscribe(
            SubscribeRequest::new("global-filtered", topic())
                .expect("valid subscriber ID")
                .with_options(filtered),
            move |_| {
                filtered_calls.lock().unwrap().push("filtered-handler");
            },
        )
        .unwrap();
    bus.publish(request("filtered".into())).unwrap();
    filtered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    for _ in 0..100 {
        if calls.lock().unwrap().len() >= 10 {
            break;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        *calls.lock().unwrap(),
        [
            "global-before",
            "typed-before",
            "handler",
            "typed-after",
            "global-after",
            "global-before",
            "typed-before",
            "handler",
            "typed-after",
            "global-after"
        ]
    );
    subscription.cancel().unwrap();
    filtered_subscription.cancel().unwrap();
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

#[test]
fn sync_facade_rejects_async_global_subscriber_middleware() {
    let config = EventBusFacadeConfig::new().async_subscriber_interceptor(
        |_delivery: Delivery<String>, _next: AsyncSubscriberNext<String>| {
            Box::pin(async { Ok(()) }) as SpiFuture<'static, Result<(), DeliveryError>>
        },
    );
    let (bus, backend) = create_bus_with_config(config);
    let result = bus.subscribe(
        SubscribeRequest::new("bad-sync-global", topic()).expect("valid subscriber ID"),
        |_| {},
    );
    assert!(matches!(
        result,
        Err(SubscribeError::Configuration(ConfigurationError::InvalidField {
            field: "async_subscriber_interceptor",
            ..
        }))
    ));
    assert_eq!(backend.state.lock().unwrap().queues.len(), 0);
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}

fn create_bus_with_config(config: EventBusFacadeConfig) -> (EventBus, Arc<TestBackend>) {
    let backend = Arc::new(TestBackend::new());
    let bus = EventBus::with_config(ProviderId::new("sync-test").unwrap(), backend.clone(), config);
    (bus, backend)
}

#[test]
fn concurrent_cancel_callers_both_wait_for_worker_close() {
    let (bus, backend) = create_bus();
    backend.set_close_delay(Duration::from_millis(200));
    let subscription = Arc::new(
        bus.subscribe(
            SubscribeRequest::new("concurrent-cancel", topic()).expect("valid ID"),
            |_| (),
        )
        .expect("subscription starts"),
    );
    let barrier = Arc::new(Barrier::new(3));
    let (finished_tx, finished_rx) = mpsc::channel();
    let mut callers = Vec::new();
    for _ in 0..2 {
        let subscription = subscription.clone();
        let barrier = barrier.clone();
        let finished_tx = finished_tx.clone();
        callers.push(std::thread::spawn(move || {
            barrier.wait();
            let started = std::time::Instant::now();
            let result = subscription.cancel();
            finished_tx
                .send((started.elapsed(), result))
                .expect("receiver remains alive");
        }));
    }
    barrier.wait();
    let first = finished_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first cancellation completes");
    let second = finished_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("second cancellation completes");
    for caller in callers {
        caller.join().expect("cancel caller thread exits");
    }
    assert!(first.0 >= Duration::from_millis(150));
    assert!(second.0 >= Duration::from_millis(150));
    assert!(first.1.is_ok());
    assert!(second.1.is_ok());
    assert_eq!(backend.close_calls(), 1);
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn close_panic_still_notifies_concurrent_cancel_waiters() {
    let (bus, backend) = create_bus();
    backend.set_close_panics(true);
    let subscription = Arc::new(
        bus.subscribe(SubscribeRequest::new("close-panic", topic()).expect("valid ID"), |_| ())
            .expect("subscription starts"),
    );
    let barrier = Arc::new(Barrier::new(3));
    let (finished_tx, finished_rx) = mpsc::channel();
    let mut callers = Vec::new();
    for _ in 0..2 {
        let subscription = subscription.clone();
        let barrier = barrier.clone();
        let finished_tx = finished_tx.clone();
        callers.push(std::thread::spawn(move || {
            barrier.wait();
            finished_tx.send(subscription.cancel()).expect("receiver remains alive");
        }));
    }
    barrier.wait();
    let first = finished_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first caller completes");
    let second = finished_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("second caller completes");
    for caller in callers {
        caller.join().expect("cancel caller thread exits");
    }
    assert!(first.is_err() || second.is_err());
    assert_eq!(backend.close_calls(), 1);
    let _ = bus.shutdown(ShutdownMode::Immediate);
}

#[test]
fn one_bus_worker_can_request_cancel_for_a_different_subscription_without_joining() {
    let (bus, _) = create_bus();
    let handlers_started = Arc::new(Barrier::new(2));
    let (release_b_tx, release_b_rx) = mpsc::channel();
    let release_b_rx = Arc::new(Mutex::new(release_b_rx));
    let (b_started_tx, b_started_rx) = mpsc::channel();
    let handlers_started_by_b = handlers_started.clone();
    let b_subscription = bus
        .subscribe(
            SubscribeRequest::new("cancel-target", topic()).expect("valid ID"),
            move |_| {
                b_started_tx.send(()).expect("test receiver remains alive");
                handlers_started_by_b.wait();
                release_b_rx.lock().expect("release lock").recv().expect("release B");
            },
        )
        .expect("target subscription starts");
    let b_subscription = Arc::new(Mutex::new(Some(b_subscription)));
    let b_subscription_for_a = b_subscription.clone();
    let (a_started_tx, a_started_rx) = mpsc::channel();
    let (a_done_tx, a_done_rx) = mpsc::channel();
    let handlers_started_by_a = handlers_started.clone();
    let a_subscription = bus
        .subscribe(
            SubscribeRequest::new("cancel-caller", topic()).expect("valid ID"),
            move |_| {
                a_started_tx.send(()).expect("test receiver remains alive");
                handlers_started_by_a.wait();
                let result = b_subscription_for_a
                    .lock()
                    .expect("subscription lock")
                    .as_ref()
                    .expect("target subscription remains present")
                    .cancel();
                a_done_tx.send(result).expect("test receiver remains alive");
            },
        )
        .expect("caller subscription starts");

    bus.publish(request("cancel-worker".into())).expect("publish");
    b_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("target handler starts");
    a_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("caller handler starts");
    a_done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("worker cancellation request returns")
        .expect("cancel succeeds");
    release_b_tx.send(()).expect("release target handler");
    b_subscription
        .lock()
        .expect("subscription lock")
        .take()
        .expect("target subscription remains present")
        .cancel()
        .expect("target already joined");
    a_subscription.cancel().expect("cancel caller");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn workers_canceling_each_other_do_not_form_a_join_cycle() {
    let (bus, _) = create_bus();
    let a_handle = Arc::new(Mutex::new(None::<Subscription>));
    let b_handle = Arc::new(Mutex::new(None::<Subscription>));
    let barrier = Arc::new(Barrier::new(2));
    let (a_done_tx, a_done_rx) = mpsc::channel();
    let (b_done_tx, b_done_rx) = mpsc::channel();

    let b_for_a = b_handle.clone();
    let barrier_for_a = barrier.clone();
    let a = bus
        .subscribe(
            SubscribeRequest::new("cycle-a", topic()).expect("valid ID"),
            move |_| {
                barrier_for_a.wait();
                let result = b_for_a
                    .lock()
                    .expect("B handle lock")
                    .as_ref()
                    .expect("B handle initialized")
                    .cancel();
                a_done_tx.send(result).expect("receiver remains alive");
            },
        )
        .expect("A starts");
    *a_handle.lock().expect("A handle lock") = Some(a);

    let a_for_b = a_handle.clone();
    let b = bus
        .subscribe(
            SubscribeRequest::new("cycle-b", topic()).expect("valid ID"),
            move |_| {
                barrier.wait();
                let result = a_for_b
                    .lock()
                    .expect("A handle lock")
                    .as_ref()
                    .expect("A handle initialized")
                    .cancel();
                b_done_tx.send(result).expect("receiver remains alive");
            },
        )
        .expect("B starts");
    *b_handle.lock().expect("B handle lock") = Some(b);

    bus.publish(request("cancel-cycle".into())).expect("publish");
    a_done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("A does not wait to join B")
        .expect("A cancellation succeeds");
    b_done_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("B does not wait to join A")
        .expect("B cancellation succeeds");
    a_handle
        .lock()
        .expect("A handle lock")
        .take()
        .expect("A handle")
        .cancel()
        .expect("external A cancel waits");
    b_handle
        .lock()
        .expect("B handle lock")
        .take()
        .expect("B handle")
        .cancel()
        .expect("external B cancel waits");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn handler_can_reenter_publish_without_a_facade_lock_deadlock() {
    let (bus, _) = create_bus();
    let nested_bus = bus.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("reentrant", topic()).expect("valid subscriber ID"),
            move |_| {
                let result = nested_bus.publish(request("nested".into()));
                done_tx.send(result.is_ok()).expect("test receiver remains alive");
                Ok(())
            },
        )
        .expect("subscription starts");

    bus.publish(request("outer".into())).expect("outer publish");
    assert!(
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("handler can reenter")
    );
    subscription.cancel().expect("cancel subscription");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn blocking_lifecycle_calls_from_own_worker_fail_instead_of_deadlocking() {
    let (bus, _) = create_bus();
    let idle_bus = bus.clone();
    let shutdown_bus = bus.clone();
    let immediate_shutdown_bus = bus.clone();
    let (idle_tx, idle_rx) = mpsc::channel();
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let (immediate_shutdown_tx, immediate_shutdown_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("deadlock-guard", topic()).expect("valid subscriber ID"),
            move |_| {
                idle_tx
                    .send(idle_bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(1))))
                    .expect("test receiver remains alive");
                shutdown_tx
                    .send(shutdown_bus.shutdown(ShutdownMode::Graceful {
                        timeout: Duration::from_secs(1),
                    }))
                    .expect("test receiver remains alive");
                immediate_shutdown_tx
                    .send(immediate_shutdown_bus.shutdown(ShutdownMode::Immediate))
                    .expect("test receiver remains alive");
                Ok(())
            },
        )
        .expect("subscription starts");

    bus.publish(request("deadlock-check".into())).expect("publish");
    assert!(matches!(
        idle_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("idle check returned"),
        Err(LifecycleError::WouldDeadlock { .. })
    ));
    assert!(matches!(
        shutdown_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("shutdown returned"),
        Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock { .. }))
    ));
    assert!(matches!(
        immediate_shutdown_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("immediate shutdown returned"),
        Err(ShutdownError::Lifecycle(LifecycleError::WouldDeadlock { .. }))
    ));
    subscription.cancel().expect("cancel subscription");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown outside worker");
}

#[test]
fn immediate_shutdown_waits_for_active_delivery_and_settlement_before_provider_close() {
    let (bus, backend) = create_bus();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (queued_handler_tx, queued_handler_rx) = mpsc::channel();
    let release_rx = Arc::new(Mutex::new(release_rx));
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_by_handler = calls.clone();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("immediate-drain", topic())
                .expect("valid ID")
                .with_options(
                    SubscribeOptions::builder()
                        .ordering_policy(OrderingPolicy::PerKey)
                        .build(),
                ),
            move |_| {
                if calls_by_handler.fetch_add(1, Ordering::AcqRel) == 0 {
                    started_tx.send(()).expect("test receiver remains alive");
                    release_rx
                        .lock()
                        .expect("release lock")
                        .recv()
                        .expect("release handler");
                } else {
                    queued_handler_tx.send(()).expect("test receiver remains alive");
                }
            },
        )
        .expect("subscription starts");
    bus.publish(request_with_key("active-delivery", "same-key"))
        .expect("publish");
    started_rx.recv_timeout(Duration::from_secs(2)).expect("handler starts");
    bus.publish(request_with_key("queued-delivery", "same-key"))
        .expect("queue second message");

    let shutdown_bus = bus.clone();
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let shutdown_worker = std::thread::spawn(move || {
        shutdown_tx
            .send(shutdown_bus.shutdown(ShutdownMode::Immediate))
            .expect("receiver remains alive");
    });
    assert!(shutdown_rx.recv_timeout(Duration::from_millis(100)).is_err());
    assert_eq!(backend.shutdown_calls(), 0);
    assert_eq!(backend.close_calls(), 0);

    release_tx.send(()).expect("release handler");
    assert_eq!(
        shutdown_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("shutdown completes")
            .expect("shutdown succeeds"),
        ShutdownOutcome::Complete
    );
    shutdown_worker.join().expect("shutdown thread exits");
    let dispositions = backend.settlement_dispositions();
    assert_eq!(
        dispositions
            .iter()
            .filter(|item| **item == DeliveryDisposition::Accept)
            .count(),
        1
    );
    assert_eq!(
        dispositions
            .iter()
            .filter(|item| **item == DeliveryDisposition::Retry)
            .count(),
        1
    );
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert!(queued_handler_rx.try_recv().is_err());
    assert_eq!(backend.close_calls(), 1);
    assert_eq!(backend.shutdown_calls(), 1);
    assert!(subscription.is_cancelled());
}

#[test]
fn immediate_shutdown_returns_subscription_close_error_after_shutting_down_provider() {
    let (bus, backend) = create_bus();
    backend.set_close_fails(true);
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("immediate-close-error", topic()).expect("valid ID"),
            |_| (),
        )
        .expect("subscription starts");

    let error = bus
        .shutdown(ShutdownMode::Immediate)
        .expect_err("close failure is returned");
    assert_close_failures(&error, &["immediate-close-error"]);
    assert_eq!(backend.close_calls(), 1);
    assert_eq!(backend.shutdown_calls(), 1);
}

#[test]
fn shutdown_reports_close_error_after_worker_naturally_exits_and_repeats_terminal_error() {
    let (bus, backend) = create_bus();
    backend.set_close_fails(true);
    let _subscription = bus
        .subscribe(
            SubscribeRequest::new("natural-close-failure", topic()).expect("valid ID"),
            |_| (),
        )
        .expect("subscription starts");
    backend.close_receivers();
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while backend.close_calls() == 0 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert_eq!(backend.close_calls(), 1, "worker naturally observed receiver closure");

    let first = bus
        .shutdown(ShutdownMode::Immediate)
        .expect_err("natural close failure persists");
    let second = bus
        .shutdown(ShutdownMode::Immediate)
        .expect_err("repeated shutdown returns same terminal error");
    assert_close_failures(&first, &["natural-close-failure"]);
    assert_close_failures(&second, &["natural-close-failure"]);
    let (ShutdownError::SubscriptionClose(first_errors), ShutdownError::SubscriptionClose(second_errors)) =
        (&first, &second)
    else {
        unreachable!("close failure assertions validate the variants above");
    };
    assert!(Arc::ptr_eq(first_errors, second_errors));
    assert_eq!(backend.shutdown_calls(), 1);
}

#[test]
fn natural_provider_close_waits_for_admitted_key_lane_jobs_and_releases_scheduler_slots() {
    let (bus, backend) = create_bus_with_scheduler(2, 2);
    let options = SubscribeOptions::builder()
        .ordering_policy(OrderingPolicy::PerKey)
        .build();
    let first_gate = Arc::new(HandlerGate::default());
    let first_gate_by_handler = first_gate.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_by_handler = calls.clone();
    let (second_done_tx, second_done_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("natural-close-with-admitted-work", topic())
                .expect("valid ID")
                .with_options(options),
            move |_| {
                if calls_by_handler.fetch_add(1, Ordering::AcqRel) == 0 {
                    first_gate_by_handler.wait();
                } else {
                    second_done_tx.send(()).expect("second handler receiver remains alive");
                }
            },
        )
        .expect("subscription starts");
    bus.publish(request_with_key("active", "same-key"))
        .expect("publish active");
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while calls.load(Ordering::Acquire) == 0 && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    bus.publish(request_with_key("queued", "same-key"))
        .expect("publish queued");
    let receive_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while backend.received_messages() < 2 && std::time::Instant::now() < receive_deadline {
        std::thread::yield_now();
    }
    assert_eq!(backend.received_messages(), 2);
    backend.close_receivers();
    first_gate.release();
    second_done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("admitted queued handler completes");
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("both admitted deliveries settle");
    let close_deadline = std::time::Instant::now() + Duration::from_secs(2);
    while backend.close_calls() == 0 && std::time::Instant::now() < close_deadline {
        std::thread::yield_now();
    }
    assert_eq!(backend.close_calls(), 1, "natural close waits for scheduler completion");
    assert_eq!(calls.load(Ordering::Acquire), 2);
    assert_eq!(backend.settlement_calls(), 2);
    bus.shutdown(ShutdownMode::Immediate)
        .expect("shutdown joins idle scheduler workers");
    assert!(
        !subscription.is_cancelled(),
        "provider close is distinct from caller cancellation"
    );
}

#[test]
fn concurrent_cancel_and_shutdown_both_observe_subscription_close_failure() {
    let (bus, backend) = create_bus();
    backend.set_close_fails(true);
    backend.set_close_delay(Duration::from_millis(200));
    let subscription = Arc::new(
        bus.subscribe(
            SubscribeRequest::new("racing-close-failure", topic()).expect("valid ID"),
            |_| (),
        )
        .expect("subscription starts"),
    );
    let shutdown_bus = bus.clone();
    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let shutdown_thread = std::thread::spawn(move || {
        shutdown_tx
            .send(shutdown_bus.shutdown(ShutdownMode::Immediate))
            .expect("receiver remains alive");
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !subscription.is_cancelled() && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    assert!(subscription.is_cancelled(), "shutdown snapshots and stops the worker");
    let cancel_error = subscription.cancel().expect_err("cancel observes close failure");
    let shutdown_error = shutdown_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("shutdown completes")
        .expect_err("shutdown observes the same close failure");
    shutdown_thread.join().expect("shutdown worker exits");
    assert_lifecycle_close_failures(&cancel_error, &["racing-close-failure"]);
    assert_close_failures(&shutdown_error, &["racing-close-failure"]);
    let (LifecycleError::SubscriptionClose(cancel_errors), ShutdownError::SubscriptionClose(shutdown_errors)) =
        (&cancel_error, &shutdown_error)
    else {
        unreachable!("close failure assertions validate the variants above");
    };
    assert!(std::ptr::eq(
        cancel_errors.iter().next().expect("one close failure"),
        shutdown_errors.iter().next().expect("one close failure"),
    ));
}

#[test]
fn shutdown_aggregates_close_failures_from_all_subscriptions() {
    let (bus, backend) = create_bus();
    backend.set_close_fails(true);
    let _first = bus
        .subscribe(
            SubscribeRequest::new("first-close-failure", topic()).expect("valid ID"),
            |_| (),
        )
        .expect("first subscription starts");
    let _second = bus
        .subscribe(
            SubscribeRequest::new("second-close-failure", topic()).expect("valid ID"),
            |_| (),
        )
        .expect("second subscription starts");

    let error = bus
        .shutdown(ShutdownMode::Immediate)
        .expect_err("close failures are reported");
    assert_close_failures(&error, &["first-close-failure", "second-close-failure"]);
    assert_eq!(backend.close_calls(), 2);
    assert_eq!(backend.shutdown_calls(), 1);
}

fn assert_close_failures(error: &ShutdownError, expected: &[&str]) {
    let ShutdownError::SubscriptionClose(errors) = error else {
        panic!("expected aggregated subscription close error, got {error:?}");
    };
    let mut actual: Vec<_> = errors.iter().map(|failure| failure.subscriber_id().as_str()).collect();
    let mut expected = expected.to_vec();
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected);
    assert!(errors.iter().all(|failure| failure.error().operation() == "close"));
    assert!(
        errors
            .iter()
            .all(|failure| std::error::Error::source(failure.error()).is_some())
    );
    assert_eq!(errors.len(), expected.len());
}

fn assert_lifecycle_close_failures(error: &LifecycleError, expected: &[&str]) {
    let LifecycleError::SubscriptionClose(errors) = error else {
        panic!("expected aggregated subscription close error, got {error:?}");
    };
    let actual: Vec<_> = errors.iter().map(|failure| failure.subscriber_id().as_str()).collect();
    assert_eq!(actual, expected);
    assert!(errors.iter().all(|failure| failure.error().operation() == "close"));
    assert!(
        errors
            .iter()
            .all(|failure| std::error::Error::source(failure.error()).is_some())
    );
}

fn immediate_shutdown_receive_race(capability: SettlementCapabilities) -> (Vec<DeliveryDisposition>, usize, bool) {
    let (bus, backend) = create_bus();
    backend.set_settlement_capability(capability);
    let (diagnostic_tx, diagnostic_rx) = mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if let Diagnostic::SettlementUnavailable { requested, .. } = diagnostic {
            diagnostic_tx
                .send(*requested)
                .expect("diagnostic receiver remains alive");
        }
    });
    let (handler_started_tx, handler_started_rx) = mpsc::channel();
    let (release_handler_tx, release_handler_rx) = mpsc::channel();
    let release_handler_rx = Arc::new(Mutex::new(release_handler_rx));
    let (queued_handler_tx, queued_handler_rx) = mpsc::channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_by_handler = calls.clone();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("receive-shutdown-race", topic()).expect("valid ID"),
            move |_| {
                if calls_by_handler.fetch_add(1, Ordering::AcqRel) == 0 {
                    handler_started_tx
                        .send(())
                        .expect("handler-start receiver remains alive");
                    release_handler_rx
                        .lock()
                        .expect("handler release lock")
                        .recv()
                        .expect("release first handler");
                } else {
                    queued_handler_tx
                        .send(())
                        .expect("queued-handler receiver remains alive");
                }
            },
        )
        .expect("subscription starts");
    bus.publish(request("first".into())).expect("publish first delivery");
    handler_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("first handler starts");
    let (receive_entered_rx, release_receive_tx) = backend.gate_next_receive();
    bus.publish(request("already-received".into()))
        .expect("publish queued delivery");
    release_handler_tx.send(()).expect("release first handler");
    receive_entered_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("worker pauses after receiving second message");

    let shutdown_bus = bus.clone();
    let (shutdown_done_tx, shutdown_done_rx) = mpsc::channel();
    let shutdown_thread = std::thread::spawn(move || {
        shutdown_done_tx
            .send(shutdown_bus.shutdown(ShutdownMode::Immediate))
            .expect("shutdown receiver remains alive");
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while !subscription.is_cancelled() && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(
        subscription.is_cancelled(),
        "shutdown requests cancellation before returning"
    );
    release_receive_tx.send(()).expect("release gated receive");
    shutdown_done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("shutdown completes")
        .expect("shutdown succeeds");
    shutdown_thread.join().expect("shutdown thread exits");
    assert_eq!(calls.load(Ordering::Acquire), 1);
    assert!(queued_handler_rx.try_recv().is_err());
    (
        backend.settlement_dispositions(),
        backend.close_calls(),
        diagnostic_rx.try_recv().is_ok(),
    )
}

#[test]
fn immediate_shutdown_requeues_message_received_at_cancellation_boundary() {
    let (dispositions, close_calls, unavailable_diagnostic) =
        immediate_shutdown_receive_race(SettlementCapabilities::AcceptRetryReject);
    assert_eq!(dispositions.len(), 2);
    assert_eq!(
        dispositions
            .iter()
            .filter(|item| **item == DeliveryDisposition::Accept)
            .count(),
        1
    );
    assert_eq!(
        dispositions
            .iter()
            .filter(|item| **item == DeliveryDisposition::Retry)
            .count(),
        1
    );
    assert_eq!(close_calls, 1);
    assert!(!unavailable_diagnostic);
}

#[test]
fn immediate_shutdown_reports_unsettleable_message_received_at_cancellation_boundary() {
    let (dispositions, close_calls, unavailable_diagnostic) =
        immediate_shutdown_receive_race(SettlementCapabilities::None);
    assert!(dispositions.is_empty());
    assert_eq!(close_calls, 1);
    assert!(unavailable_diagnostic);
}

#[test]
fn shutdown_stops_admission_closes_subscriptions_and_propagates_provider_shutdown() {
    let (bus, backend) = create_bus();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("shutdown-case", topic()).expect("valid subscriber ID"),
            |_| Ok(()),
        )
        .expect("subscription starts");
    let outcome = bus
        .shutdown(ShutdownMode::Graceful {
            timeout: Duration::from_secs(2),
        })
        .expect("graceful shutdown");
    assert_eq!(outcome, ShutdownOutcome::Complete);
    assert_eq!(backend.shutdown_calls(), 1);
    assert_eq!(backend.close_calls(), 1);
    assert!(matches!(bus.publish(request("late".into())), Err(PublishError::Closed)));
    assert!(matches!(
        bus.subscribe(
            SubscribeRequest::new("late-subscriber", topic()).expect("valid ID"),
            |_| Ok(()),
        ),
        Err(SubscribeError::Closed)
    ));
    assert!(subscription.is_cancelled());
}

#[test]
fn drop_does_not_implicitly_cancel_subscription() {
    let (bus, backend) = create_bus();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("drop-case", topic()).expect("valid subscriber ID"),
            |_| Ok(()),
        )
        .expect("subscription starts");
    drop(subscription);
    assert_eq!(backend.close_calls(), 0);
    bus.shutdown(ShutdownMode::Graceful {
        timeout: Duration::from_secs(2),
    })
    .expect("bus owns worker lifetime");
    assert_eq!(backend.close_calls(), 1);
}

#[test]
fn diagnostic_observers_receive_terminal_delivery_failures_and_isolate_panics() {
    let (bus, _) = create_bus();
    let observed = Arc::new(AtomicUsize::new(0));
    let (diagnostic_tx, diagnostic_rx) = mpsc::channel();
    let panic_observer = bus.observe_diagnostics(|_| panic!("observer panic"));
    let observed_by_callback = observed.clone();
    let observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::DeliveryFailed { .. }) {
            observed_by_callback.fetch_add(1, Ordering::AcqRel);
            diagnostic_tx.send(()).expect("test receiver remains alive");
        }
    });
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("diagnostic-case", topic()).expect("valid subscriber ID"),
            |_| {
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("expected handler failure")),
                })
            },
        )
        .expect("subscription starts");

    bus.publish(request("diagnostic".into())).expect("publish");
    diagnostic_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("failed delivery reaches terminal diagnostic");
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("diagnosed delivery has completed");
    assert_eq!(observed.load(Ordering::Acquire), 1);

    drop((subscription, observer, panic_observer));
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn dropping_diagnostic_observer_releases_its_callback_capture() {
    let (bus, _) = create_bus();
    let captured = Arc::new(());
    let weak = Arc::downgrade(&captured);
    let handle = bus.observe_diagnostics(move |_| {
        let _keep_capture = &captured;
    });

    drop(handle);

    assert!(weak.upgrade().is_none());
}

#[test]
fn retry_directive_is_subject_to_qubit_retry_policy_and_abort_is_not_overridden() {
    let (bus, backend) = create_bus();
    let attempts = Arc::new(AtomicUsize::new(0));
    let error_callbacks = Arc::new(AtomicUsize::new(0));
    let attempts_by_handler = attempts.clone();
    let callbacks_by_handler = error_callbacks.clone();
    let (done_tx, done_rx) = mpsc::channel();
    let options = SubscribeOptions::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(2)
                .build()
                .expect("valid retry policy"),
        )
        .retry_rule(|_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .error_handler(move |_, _| {
            callbacks_by_handler.fetch_add(1, Ordering::AcqRel);
            FailureDirective::Retry
        })
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("retry-case", topic())
                .expect("valid ID")
                .with_options(options),
            move |_| {
                if attempts_by_handler.fetch_add(1, Ordering::AcqRel) == 0 {
                    Err(DeliveryError::Handler {
                        source: Box::new(std::io::Error::other("retry once")),
                    })
                } else {
                    done_tx.send(()).expect("test receiver remains alive");
                    Ok(())
                }
            },
        )
        .expect("subscription starts");

    bus.publish(request("retry".into())).expect("publish");
    done_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("second attempt succeeds");
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("retry flow completes");
    assert_eq!(attempts.load(Ordering::Acquire), 2);
    assert_eq!(error_callbacks.load(Ordering::Acquire), 1);
    assert_eq!(backend.settlement_dispositions(), [DeliveryDisposition::Accept]);
    subscription.cancel().expect("cancel");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");

    let (bus, backend) = create_bus();
    let attempts = Arc::new(AtomicUsize::new(0));
    let callbacks = Arc::new(AtomicUsize::new(0));
    let attempts_by_handler = attempts.clone();
    let callbacks_by_handler = callbacks.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let options = SubscribeOptions::builder()
        .retry_policy(
            RetryPolicy::builder()
                .max_attempts(3)
                .build()
                .expect("valid retry policy"),
        )
        .retry_rule(|_: &AttemptFailure<DeliveryAttemptError>, _: &RetryContext| RetryDecision::Retry)
        .error_handler(move |_, _| {
            callbacks_by_handler.fetch_add(1, Ordering::AcqRel);
            FailureDirective::Discard
        })
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("abort-case", topic())
                .expect("valid ID")
                .with_options(options),
            move |_| {
                entered_tx.send(()).expect("test receiver remains alive");
                attempts_by_handler.fetch_add(1, Ordering::AcqRel);
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("discard now")),
                })
            },
        )
        .expect("subscription starts");
    bus.publish(request("discard".into())).expect("publish");
    entered_rx.recv_timeout(Duration::from_secs(2)).expect("handler starts");
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("discard reaches terminal state");
    assert_eq!(attempts.load(Ordering::Acquire), 1);
    assert_eq!(callbacks.load(Ordering::Acquire), 1);
    assert_eq!(backend.settlement_dispositions(), [DeliveryDisposition::Reject]);
    subscription.cancel().expect("cancel");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn dead_letter_publish_failure_requeues_instead_of_rejecting_original_message() {
    let (bus, backend) = create_bus();
    backend.fail_publish_call(2);
    let (settled_tx, settled_rx) = mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::InternalFailure { origin, .. } if origin.as_ref() == "dead_letter_publish")
        {
            settled_tx.send(()).expect("test receiver remains alive");
        }
    });
    let options = SubscribeOptions::builder()
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(DeadLetterPolicy::topic("dead-letters").expect("valid dead-letter topic"))
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("dead-letter-case", topic())
                .expect("valid ID")
                .with_options(options),
            |_| {
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("handler failed")),
                })
            },
        )
        .expect("subscription starts");

    bus.publish(request("dead-letter-me".into()))
        .expect("original publish succeeds");
    settled_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("dead-letter failure diagnostic");
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("failed dead-letter attempt completes");
    assert_eq!(backend.publish_calls(), 2);
    assert_eq!(backend.published_topics(), ["sync.events".into()]);
    assert_eq!(backend.settlement_dispositions(), [DeliveryDisposition::Retry]);
    subscription.cancel().expect("cancel");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn inbound_dead_letter_marker_prevents_recursive_sync_dead_letter_publish() {
    let (bus, backend) = create_bus();
    let observed = Arc::new(AtomicBool::new(false));
    let observed_by_handler = observed.clone();
    let (handler_tx, handler_rx) = mpsc::channel();
    let options = SubscribeOptions::builder()
        .error_handler(|_, _| FailureDirective::DeadLetter)
        .dead_letter(DeadLetterPolicy::topic("dead-letters").expect("valid dead-letter topic"))
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("marked-dead-letter", topic())
                .expect("valid ID")
                .with_options(options),
            move |delivery: Delivery<String>| {
                observed_by_handler.store(delivery.context().is_dead_letter(), Ordering::SeqCst);
                handler_tx.send(()).expect("test receiver remains alive");
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("handler failed")),
                })
            },
        )
        .expect("subscription starts");
    backend.enqueue_marked(subscription.id());
    handler_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("marked handler runs");
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("marked delivery completes");
    assert!(observed.load(Ordering::SeqCst));
    assert_eq!(backend.publish_calls(), 0);
    assert_eq!(backend.settlement_dispositions(), [DeliveryDisposition::Reject]);
    subscription.cancel().expect("cancel");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn unsupported_failure_settlement_is_reported_without_calling_provider_settle() {
    let (bus, backend) = create_bus();
    backend.set_settlement_capability(SettlementCapabilities::AcceptOnly);
    let (diagnostic_tx, diagnostic_rx) = mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(diagnostic, Diagnostic::SettlementUnavailable { .. }) {
            diagnostic_tx.send(()).expect("test receiver remains alive");
        }
    });
    let options = SubscribeOptions::builder()
        .error_handler(|_, _| FailureDirective::Requeue)
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("unsupported-settlement", topic())
                .expect("valid ID")
                .with_options(options),
            |_| {
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("handler failed")),
                })
            },
        )
        .expect("subscription starts");
    bus.publish(request("unsettleable".into())).expect("publish");
    diagnostic_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("settlement limitation diagnostic");
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("terminal handling completes");
    assert_eq!(backend.settlement_calls(), 0);
    subscription.cancel().expect("cancel");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn unsupported_reject_settlement_is_reported_without_calling_provider_settle() {
    let (bus, backend) = create_bus();
    backend.set_settlement_capability(SettlementCapabilities::None);
    let (diagnostic_tx, diagnostic_rx) = mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(
            diagnostic,
            Diagnostic::SettlementUnavailable {
                requested: DeliveryDisposition::Reject,
                ..
            }
        ) {
            diagnostic_tx.send(()).expect("test receiver remains alive");
        }
    });
    let options = SubscribeOptions::builder()
        .error_handler(|_, _| FailureDirective::Discard)
        .build();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("unsupported-reject", topic())
                .expect("valid ID")
                .with_options(options),
            |_| {
                Err(DeliveryError::Handler {
                    source: Box::new(std::io::Error::other("handler failed")),
                })
            },
        )
        .expect("subscription starts");
    bus.publish(request("unrejectable".into())).expect("publish");
    diagnostic_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("settlement limitation diagnostic");
    bus.wait_for_received_deliveries(&topic(), Some(Duration::from_secs(2)))
        .expect("terminal handling completes");
    assert_eq!(backend.settlement_calls(), 0);
    subscription.cancel().expect("cancel");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn undecodable_message_respects_settlement_capability_and_reports_unavailable_reject() {
    let (bus, backend) = create_bus();
    backend.set_settlement_capability(SettlementCapabilities::AcceptOnly);
    let (diagnostic_tx, diagnostic_rx) = mpsc::channel();
    let _observer = bus.observe_diagnostics(move |diagnostic| {
        if matches!(
            diagnostic,
            Diagnostic::SettlementUnavailable {
                requested: DeliveryDisposition::Reject,
                ..
            }
        ) {
            diagnostic_tx.send(()).expect("test receiver remains alive");
        }
    });
    let (handler_tx, handler_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("decode-failure", topic()).expect("valid ID"),
            move |_| {
                let _ = handler_tx.send(());
            },
        )
        .expect("subscription starts");
    let wrong_topic = Topic::new("sync.events").expect("valid topic");
    bus.publish(PublishRequest::new(wrong_topic, 42_i32).expect("valid request"))
        .expect("malformed payload reaches provider");

    diagnostic_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("unsupported Reject diagnostic");
    assert!(handler_rx.try_recv().is_err());
    assert_eq!(backend.settlement_calls(), 0);
    subscription.cancel().expect("cancel");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn subscribe_preserves_provider_subscription_id_and_typed_delivery_metadata() {
    let (bus, _) = create_bus();
    let (done_tx, done_rx) = mpsc::channel();
    let subscription = bus
        .subscribe(
            SubscribeRequest::new("metadata-case", topic()).expect("valid ID"),
            move |delivery: Delivery<String>| {
                done_tx
                    .send((
                        delivery.payload().clone(),
                        delivery.context().subscription_id(),
                        delivery.context().subscriber_id().as_str().to_owned(),
                    ))
                    .expect("test receiver remains alive");
                Ok(())
            },
        )
        .expect("subscription starts");
    let expected_id = subscription.id();
    bus.publish(request("metadata".into())).expect("publish");
    let (payload, actual_id, subscriber) = done_rx.recv_timeout(Duration::from_secs(2)).expect("handler invoked");
    assert_eq!(payload, "metadata");
    assert_eq!(actual_id, expected_id);
    assert_eq!(subscriber, "metadata-case");
    subscription.cancel().expect("cancel");
    bus.shutdown(ShutdownMode::Immediate).expect("shutdown");
}

#[test]
fn facade_publisher_interceptor_runs_after_typed_interceptors_and_can_drop() {
    use PublishOptions;
    use qubit_event_bus::model::PublishMetadata;

    let order = Arc::new(Mutex::new(Vec::new()));
    let typed_order = order.clone();
    let global_order = order.clone();
    let config = EventBusFacadeConfig::new().publisher_interceptor(move |metadata: &mut PublishMetadata| {
        global_order.lock().unwrap().push("global");
        assert_eq!(metadata.header("origin"), Some("typed"));
        metadata.set_header("trace", "global")?;
        Ok(false)
    });
    let (bus, backend) = create_bus_with_config(config);
    let options = PublishOptions::<String>::builder()
        .interceptor(move |mut envelope| {
            typed_order.lock().unwrap().push("typed");
            envelope.set_header("origin", "typed").expect("valid header");
            Ok(Some(envelope))
        })
        .build();

    let receipt = bus
        .publish(request("dropped".to_owned()).with_options(options))
        .unwrap();

    assert!(matches!(
        receipt.acknowledgement(),
        PublishAcknowledgement::DroppedByInterceptor
    ));
    assert_eq!(*order.lock().unwrap(), ["typed", "global"]);
    assert_eq!(backend.publish_calls(), 0);
    bus.shutdown(ShutdownMode::Immediate).unwrap();
}
