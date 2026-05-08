use qubit_event_bus::{
    EventBus,
    EventBusError,
    EventBusResult,
    EventEnvelope,
    IntoEventBusResult,
    PublishOptions,
    StagedEvent,
    StagedEventEnvelope,
    SubscribeOptions,
    Subscription,
    Topic,
    TransactionalEventBus,
    TransactionalPublisher,
    UnsupportedTransactionalEventBus,
};
use std::sync::{
    Arc,
    Mutex,
};
use std::time::Duration;

#[derive(Clone, Default)]
struct RecordingTransactionalBus {
    batches: Arc<Mutex<Vec<Vec<String>>>>,
}

#[derive(Default)]
struct RecordingPublisher;

impl TransactionalPublisher for RecordingPublisher {
    fn publish_staged(&mut self, _event: Box<dyn StagedEvent>) -> EventBusResult<()> {
        Ok(())
    }

    fn commit(&mut self) -> EventBusResult<()> {
        Ok(())
    }

    fn rollback(&mut self) -> EventBusResult<()> {
        Ok(())
    }
}

impl EventBus for RecordingTransactionalBus {
    fn start(&self) -> EventBusResult<bool> {
        Ok(true)
    }

    fn shutdown(&self) -> bool {
        true
    }

    fn publish_envelope_with_options<T>(
        &self,
        _envelope: EventEnvelope<T>,
        _options: PublishOptions<T>,
    ) -> EventBusResult<()>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(())
    }

    fn subscribe_with_options<T, S, F, R>(
        &self,
        _subscriber_id: S,
        _topic: &Topic<T>,
        _handler: F,
        _options: SubscribeOptions<T>,
    ) -> EventBusResult<Subscription<T>>
    where
        T: Clone + Send + Sync + 'static,
        S: Into<String>,
        F: Fn(EventEnvelope<T>) -> R + Send + Sync + 'static,
        R: IntoEventBusResult + 'static,
    {
        Err(EventBusError::unsupported_operation("subscribe"))
    }

    fn wait_for_idle<T>(&self, _topic: &Topic<T>) -> EventBusResult<()>
    where
        T: 'static,
    {
        Ok(())
    }

    fn wait_for_idle_timeout<T>(
        &self,
        _topic: &Topic<T>,
        _timeout: Duration,
    ) -> EventBusResult<bool>
    where
        T: 'static,
    {
        Ok(true)
    }
}

impl TransactionalEventBus for RecordingTransactionalBus {
    type Publisher = RecordingPublisher;

    fn create_transactional_publisher(&self) -> EventBusResult<Self::Publisher> {
        Ok(RecordingPublisher)
    }

    fn publish_batch_atomically_staged(
        &self,
        events: Vec<Box<dyn StagedEvent>>,
    ) -> EventBusResult<()> {
        let topics = events
            .iter()
            .map(|event| event.metadata().topic_name().to_string())
            .collect::<Vec<_>>();
        self.batches
            .lock()
            .expect("recorded batches should lock")
            .push(topics);
        Ok(())
    }
}

#[test]
fn test_transactional_event_bus_placeholder_rejects_publisher_creation() {
    let bus = UnsupportedTransactionalEventBus::new();

    assert_eq!(
        TransactionalEventBus::create_transactional_publisher(&bus)
            .expect_err("unsupported bus should reject publisher creation"),
        EventBusError::unsupported_operation("create_transactional_publisher")
    );
}

#[test]
fn test_transactional_event_bus_typed_batch_delegates_to_staged_batch() {
    let bus = RecordingTransactionalBus::default();
    let topic = Topic::<String>::try_new("transactional-typed-batch").expect("topic should build");

    TransactionalEventBus::publish_batch_atomically(
        &bus,
        vec![
            EventEnvelope::create(topic.clone(), "first".to_string()),
            EventEnvelope::create(topic.clone(), "second".to_string()),
        ],
        PublishOptions::empty(),
    )
    .expect("typed atomic batch should stage through erased batch");

    assert_eq!(
        bus.batches
            .lock()
            .expect("recorded batches should lock")
            .as_slice(),
        &[vec![
            "transactional-typed-batch".to_string(),
            "transactional-typed-batch".to_string()
        ]]
    );
}

#[test]
fn test_transactional_event_bus_accepts_heterogeneous_staged_batch() {
    let bus = RecordingTransactionalBus::default();
    let string_topic =
        Topic::<String>::try_new("transactional-heterogeneous-string").expect("topic should build");
    let number_topic =
        Topic::<i64>::try_new("transactional-heterogeneous-number").expect("topic should build");
    let events: Vec<Box<dyn StagedEvent>> = vec![
        Box::new(StagedEventEnvelope::new(
            EventEnvelope::create(string_topic, "payload".to_string()),
            PublishOptions::empty(),
        )),
        Box::new(StagedEventEnvelope::new(
            EventEnvelope::create(number_topic, 7_i64),
            PublishOptions::empty(),
        )),
    ];

    TransactionalEventBus::publish_batch_atomically_staged(&bus, events)
        .expect("heterogeneous staged batch should be accepted");

    assert_eq!(
        bus.batches
            .lock()
            .expect("recorded batches should lock")
            .as_slice(),
        &[vec![
            "transactional-heterogeneous-string".to_string(),
            "transactional-heterogeneous-number".to_string()
        ]]
    );
}
