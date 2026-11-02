#![cfg(feature = "conformance")]

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;

use qubit_event_bus::error::SpiError;
use qubit_event_bus::model::EventId;
use qubit_event_bus::model::Headers;
use qubit_event_bus::model::ProviderMessageMetadata;
use qubit_event_bus::model::PublishAcknowledgement;
use qubit_event_bus::spi::AsyncEventBusSpi;
use qubit_event_bus::spi::AsyncEventSubscriptionSpi;
use qubit_event_bus::spi::DelayedDeliveryCapability;
use qubit_event_bus::spi::DurabilityCapability;
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
use qubit_event_bus::spi::SubscriptionModes;
use qubit_event_bus::spi::TopicAddress;
use qubit_event_bus::spi::TransportPayload;
use qubit_event_bus::spi::conformance::ConformanceCase;
use qubit_event_bus::spi::conformance::ConformanceHooks;
use qubit_event_bus::spi::conformance::run_async;
use qubit_event_bus::spi::conformance::run_sync;

#[derive(Clone)]
struct StoredMessage {
    topic: TopicAddress,
    id: EventId,
    timestamp: SystemTime,
    headers: Headers,
    ordering_key: Option<qubit_event_bus::spi::OrderingKey>,
    payload: StoredPayload,
}

#[derive(Clone)]
enum StoredPayload {
    Native(Arc<dyn std::any::Any + Send + Sync>),
    Encoded(qubit_event_bus::spi::EncodedPayload),
}

impl StoredMessage {
    fn from_outbound(message: &OutboundMessage) -> Self {
        let payload = match message.payload() {
            TransportPayload::Native(value) => StoredPayload::Native(Arc::clone(value)),
            TransportPayload::Encoded(value) => StoredPayload::Encoded(value.clone()),
            _ => unreachable!("transport payload is non-exhaustive"),
        };
        Self {
            topic: message.topic().clone(),
            id: message.id().clone(),
            timestamp: message.timestamp(),
            headers: message.headers().clone(),
            ordering_key: message.ordering_key().cloned(),
            payload,
        }
    }

    fn inbound(&self) -> InboundMessage {
        let payload = match &self.payload {
            StoredPayload::Native(value) => TransportPayload::Native(Arc::clone(value)),
            StoredPayload::Encoded(value) => TransportPayload::Encoded(value.clone()),
        };
        InboundMessage::new(
            self.topic.clone(),
            self.id.clone(),
            self.timestamp,
            self.headers.clone(),
            self.ordering_key.clone(),
            payload,
            None,
            ProviderMessageMetadata::default(),
        )
    }
}

#[derive(Default)]
struct DurableStore {
    messages: HashMap<String, Vec<StoredMessage>>,
    cursors: HashMap<(String, String), usize>,
    closed_with_backlog: usize,
}

fn test_capabilities() -> EventBusCapabilities {
    EventBusCapabilities::new(
        PayloadModes::NativeAndEncoded,
        SettlementCapabilities::None,
        OrderingCapability::None,
        DelayedDeliveryCapability::None,
        DurabilityCapability::Durable,
        SubscriptionModes::BOTH,
        false,
        ReplayCapability::None,
        PublishGuarantee::Accepted,
        PublishVisibility::Opaque,
    )
}

#[derive(Clone)]
struct PersistentProvider(Arc<Mutex<DurableStore>>);

impl EventBusSpi for PersistentProvider {
    fn capabilities(&self) -> EventBusCapabilities {
        test_capabilities()
    }

    fn publish(&self, message: OutboundMessage) -> Result<PublishAcknowledgement, SpiError> {
        self.0
            .lock()
            .unwrap()
            .messages
            .entry(message.topic().as_str().to_owned())
            .or_default()
            .push(StoredMessage::from_outbound(&message));
        Ok(PublishAcknowledgement::Accepted {
            provider_message_id: None,
            metadata: Default::default(),
        })
    }

    fn subscribe(&self, request: SpiSubscriptionRequest) -> Result<Box<dyn EventSubscriptionSpi>, SpiError> {
        let topic = request.topic().as_str().to_owned();
        let subscriber = request.subscriber_id().as_str().to_owned();
        let mut store = self.0.lock().unwrap();
        let cursor = *store.cursors.entry((topic.clone(), subscriber.clone())).or_insert(0);
        let durable = request.durability() == qubit_event_bus::model::SubscriptionDurability::Durable;
        Ok(Box::new(PersistentSubscription {
            store: Arc::clone(&self.0),
            topic,
            subscriber,
            cursor,
            durable,
            closed: false,
        }))
    }

    fn shutdown(&self, _: ShutdownMode) -> Result<ShutdownOutcome, SpiError> {
        Ok(ShutdownOutcome::Complete)
    }
}

struct PersistentSubscription {
    store: Arc<Mutex<DurableStore>>,
    topic: String,
    subscriber: String,
    cursor: usize,
    durable: bool,
    closed: bool,
}

impl PersistentSubscription {
    fn receive_message(&mut self) -> Option<InboundMessage> {
        let mut store = self.store.lock().unwrap();
        let messages = store.messages.get(&self.topic)?;
        let message = messages.get(self.cursor)?.inbound();
        self.cursor += 1;
        store
            .cursors
            .insert((self.topic.clone(), self.subscriber.clone()), self.cursor);
        Some(message)
    }
}

impl EventSubscriptionSpi for PersistentSubscription {
    fn receive(&mut self, _: Duration) -> Result<ReceiveOutcome, SpiError> {
        if self.closed {
            return Ok(ReceiveOutcome::Closed);
        }
        Ok(self
            .receive_message()
            .map_or(ReceiveOutcome::TimedOut, ReceiveOutcome::Message))
    }
    fn settle(&mut self, _: &SettlementToken, _: qubit_event_bus::spi::DeliveryDisposition) -> Result<(), SpiError> {
        Ok(())
    }
    fn close(&mut self) -> Result<(), SpiError> {
        if !self.closed && self.durable {
            let mut store = self.store.lock().unwrap();
            if store
                .messages
                .get(&self.topic)
                .is_some_and(|messages| messages.len() > self.cursor)
            {
                store.closed_with_backlog += 1;
            }
        }
        self.closed = true;
        Ok(())
    }
}

impl AsyncEventBusSpi for PersistentProvider {
    fn capabilities(&self) -> EventBusCapabilities {
        test_capabilities()
    }
    fn publish<'a>(&'a self, message: OutboundMessage) -> SpiFuture<'a, Result<PublishAcknowledgement, SpiError>> {
        Box::pin(async move { EventBusSpi::publish(self, message) })
    }
    fn subscribe<'a>(
        &'a self,
        request: SpiSubscriptionRequest,
    ) -> SpiFuture<'a, Result<Box<dyn AsyncEventSubscriptionSpi>, SpiError>> {
        Box::pin(async move {
            let topic = request.topic().as_str().to_owned();
            let subscriber = request.subscriber_id().as_str().to_owned();
            let mut store = self.0.lock().unwrap();
            let cursor = *store.cursors.entry((topic.clone(), subscriber.clone())).or_insert(0);
            let durable = request.durability() == qubit_event_bus::model::SubscriptionDurability::Durable;
            Ok(Box::new(AsyncPersistentSubscription {
                store: Arc::clone(&self.0),
                topic,
                subscriber,
                cursor,
                durable,
                closed: false,
            }) as Box<dyn AsyncEventSubscriptionSpi>)
        })
    }
    fn shutdown<'a>(&'a self, _: ShutdownMode) -> SpiFuture<'a, Result<ShutdownOutcome, SpiError>> {
        Box::pin(async { Ok(ShutdownOutcome::Complete) })
    }
}

struct AsyncPersistentSubscription {
    store: Arc<Mutex<DurableStore>>,
    topic: String,
    subscriber: String,
    cursor: usize,
    durable: bool,
    closed: bool,
}

impl AsyncPersistentSubscription {
    fn receive_message(&mut self) -> Option<InboundMessage> {
        let mut store = self.store.lock().unwrap();
        let messages = store.messages.get(&self.topic)?;
        let message = messages.get(self.cursor)?.inbound();
        self.cursor += 1;
        store
            .cursors
            .insert((self.topic.clone(), self.subscriber.clone()), self.cursor);
        Some(message)
    }
}

impl AsyncEventSubscriptionSpi for AsyncPersistentSubscription {
    fn receive<'a>(&'a mut self, _: Duration) -> SpiFuture<'a, Result<ReceiveOutcome, SpiError>> {
        Box::pin(async move {
            if self.closed {
                Ok(ReceiveOutcome::Closed)
            } else {
                Ok(self
                    .receive_message()
                    .map_or(ReceiveOutcome::TimedOut, ReceiveOutcome::Message))
            }
        })
    }
    fn settle<'a>(
        &'a mut self,
        _: &SettlementToken,
        _: qubit_event_bus::spi::DeliveryDisposition,
    ) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(async { Ok(()) })
    }
    fn close<'a>(&'a mut self) -> SpiFuture<'a, Result<(), SpiError>> {
        Box::pin(async move {
            if !self.closed && self.durable {
                let mut store = self.store.lock().unwrap();
                if store
                    .messages
                    .get(&self.topic)
                    .is_some_and(|messages| messages.len() > self.cursor)
                {
                    store.closed_with_backlog += 1;
                }
            }
            self.closed = true;
            Ok(())
        })
    }
}

fn report_has_no_empty_receive_failures(cases: &[ConformanceCase]) -> bool {
    !cases
        .iter()
        .any(|case| matches!(case, ConformanceCase::Failed { case_id, .. } if case_id == "receive-empty-zero-timeout"))
}

fn assert_all_probe_combinations_ran(cases: &[ConformanceCase]) {
    let count = cases
        .iter()
        .filter(|case| {
            matches!(case,
                ConformanceCase::Passed { case_id } | ConformanceCase::Failed { case_id, .. }
                    if case_id == "receive-empty-zero-timeout"
            )
        })
        .count();
    assert_eq!(count, 4, "native/encoded x ephemeral/durable combinations must run");
}

fn has_unconsumed_backlog(store: &DurableStore) -> bool {
    store.closed_with_backlog > 0
}

#[test]
fn test_sync_conformance_isolates_durable_probes() {
    let backend = Arc::new(Mutex::new(DurableStore::default()));
    let report = run_sync(
        || Arc::new(PersistentProvider(Arc::clone(&backend))) as Arc<dyn EventBusSpi>,
        &ConformanceHooks::default(),
    );
    assert!(report_has_no_empty_receive_failures(report.cases()), "{report:?}");
    assert_all_probe_combinations_ran(report.cases());
    assert!(
        has_unconsumed_backlog(&backend.lock().unwrap()),
        "pending-close must leave durable probe backlog"
    );
}

#[test]
fn test_async_conformance_isolates_durable_probes() {
    let backend = Arc::new(Mutex::new(DurableStore::default()));
    let report = block_on(run_async(
        || async { Arc::new(PersistentProvider(Arc::clone(&backend))) as Arc<dyn AsyncEventBusSpi> },
        &Default::default(),
    ));
    assert!(report_has_no_empty_receive_failures(report.cases()), "{report:?}");
    assert_all_probe_combinations_ran(report.cases());
    assert!(
        has_unconsumed_backlog(&backend.lock().unwrap()),
        "pending-close must leave durable probe backlog"
    );
}

#[test]
fn test_repeated_runs_do_not_reuse_durable_identity() {
    let backend = Arc::new(Mutex::new(DurableStore::default()));
    for _ in 0..2 {
        let report = run_sync(
            || Arc::new(PersistentProvider(Arc::clone(&backend))) as Arc<dyn EventBusSpi>,
            &ConformanceHooks::default(),
        );
        assert!(report_has_no_empty_receive_failures(report.cases()), "{report:?}");
        assert_all_probe_combinations_ran(report.cases());
    }
    assert!(
        has_unconsumed_backlog(&backend.lock().unwrap()),
        "pending-close must leave durable probe backlog"
    );
}

fn block_on<F: Future>(future: F) -> F::Output {
    use std::task::{Context, Poll, Wake, Waker};
    struct NoopWake;
    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }
    }
}
