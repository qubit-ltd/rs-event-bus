use qubit_event_bus::{
    EventBusError,
    EventBusResult,
    EventEnvelope,
    PublishOptions,
    StagedEvent,
    StagedEventEnvelope,
    Topic,
    TransactionalPublisher,
    UnsupportedTransactionalPublisher,
};

#[derive(Default)]
struct RecordingTransactionalPublisher {
    staged: Vec<Box<dyn StagedEvent>>,
}

impl TransactionalPublisher for RecordingTransactionalPublisher {
    fn publish_staged(&mut self, event: Box<dyn StagedEvent>) -> EventBusResult<()> {
        self.staged.push(event);
        Ok(())
    }

    fn commit(&mut self) -> EventBusResult<()> {
        Ok(())
    }

    fn rollback(&mut self) -> EventBusResult<()> {
        self.staged.clear();
        Ok(())
    }
}

#[test]
fn test_transactional_publisher_default_publish_builds_envelope_and_delegates() {
    let topic = Topic::<String>::try_new("transactional-publisher").expect("topic should build");
    let mut publisher = UnsupportedTransactionalPublisher::new();

    let error = TransactionalPublisher::publish(&mut publisher, &topic, "payload".to_string())
        .expect_err("unsupported publisher should reject publish");

    assert_eq!(
        error,
        EventBusError::unsupported_operation("transactional_publish")
    );
}

#[test]
fn test_transactional_publisher_default_publish_stages_type_erased_event() {
    let topic =
        Topic::<String>::try_new("transactional-staged-publisher").expect("topic should build");
    let mut publisher = RecordingTransactionalPublisher::default();
    let options = PublishOptions::<String>::builder()
        .error_handler(|_event, _error| Ok(()))
        .build();

    TransactionalPublisher::publish_envelope_with_options(
        &mut publisher,
        EventEnvelope::create(topic.clone(), "payload".to_string()),
        options,
    )
    .expect("typed publish should stage through erased event");

    assert_eq!(publisher.staged.len(), 1);
    let staged = publisher.staged.pop().expect("staged event should exist");
    assert_eq!(
        staged.metadata().topic_name(),
        "transactional-staged-publisher"
    );
    assert_eq!(
        staged.metadata().payload_type_name(),
        topic.payload_type_name()
    );
    let typed = staged
        .as_any()
        .downcast_ref::<StagedEventEnvelope<String>>()
        .expect("staged event should keep typed envelope");
    assert_eq!(typed.envelope().payload(), "payload");
    assert_eq!(typed.options().error_handler_count(), 1);
}

#[test]
fn test_transactional_publisher_default_publish_all_staged_delegates_each_event() {
    let topic = Topic::<String>::try_new("transactional-staged-batch").expect("topic should build");
    let mut publisher = RecordingTransactionalPublisher::default();
    let events: Vec<Box<dyn StagedEvent>> = vec![
        Box::new(StagedEventEnvelope::new(
            EventEnvelope::create(topic.clone(), "first".to_string()),
            PublishOptions::empty(),
        )),
        Box::new(StagedEventEnvelope::new(
            EventEnvelope::create(topic, "second".to_string()),
            PublishOptions::empty(),
        )),
    ];

    TransactionalPublisher::publish_all_staged(&mut publisher, events)
        .expect("staged batch should delegate every event");

    assert_eq!(publisher.staged.len(), 2);
    let staged_payloads = publisher
        .staged
        .iter()
        .map(|event| {
            event
                .as_any()
                .downcast_ref::<StagedEventEnvelope<String>>()
                .expect("staged event should keep typed envelope")
                .envelope()
                .payload()
                .clone()
        })
        .collect::<Vec<_>>();
    assert_eq!(staged_payloads, ["first", "second"]);
}
